import argparse
import csv
import json
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

import etf_tracker


API_URL = "https://api.finmindtrade.com/api/v4/data"
HOLDING_DATASET = "TaiwanStockActiveETFHolding"
PRICE_DATASET = "TaiwanStockPrice"
LAGGED_DISCLOSURE_ETFS = {"00988A", "00990A"}


def load_finmind_token(env_path: Path) -> str:
    if not env_path.exists():
        raise RuntimeError(f"找不到 {env_path}")
    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        match = re.match(r"\s*FINMIND_API_KEY\s*=\s*(.*?)\s*$", raw_line)
        if match:
            token = match.group(1).strip().strip('"').strip("'")
            if token:
                return token
    raise RuntimeError(".env 內找不到 FINMIND_API_KEY")


def request_finmind(session: requests.Session, token: str, params: dict, retries: int = 3) -> dict:
    request_params = dict(params)
    request_params["token"] = token
    request_params.setdefault("limit", 10000)
    last_error = None
    for attempt in range(retries):
        try:
            response = session.get(API_URL, params=request_params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            if payload.get("msg") != "success":
                raise RuntimeError(payload.get("msg") or f"FinMind status={payload.get('status')}")
            return payload
        except Exception as exc:
            # requests 的連線錯誤可能把含 token 的完整 URL 寫進訊息，只保留異常類型。
            last_error = type(exc).__name__
            if attempt + 1 < retries:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"FinMind request failed: {last_error}")


def fetch_holding_payload(session: requests.Session, token: str, etf: str, target_date: date) -> tuple[dict, str]:
    start_date = target_date - timedelta(days=7)
    end_date = target_date + timedelta(days=1)
    payload = request_finmind(
        session,
        token,
        {
            "dataset": HOLDING_DATASET,
            "data_id": etf,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    rows = payload.get("data") or []
    grouped = {}
    for row in rows:
        source_date = str(row.get("date", ""))
        if source_date <= target_date.isoformat():
            grouped.setdefault(source_date, []).append(row)

    exact_date = target_date.isoformat()
    if exact_date in grouped:
        selected_date = exact_date
    elif etf in LAGGED_DISCLOSURE_ETFS and grouped:
        selected_date = max(grouped)
    else:
        raise RuntimeError(f"{etf} 在 FinMind 沒有 {exact_date} 持股資料")

    selected_payload = dict(payload)
    selected_payload["data"] = grouped[selected_date]
    return selected_payload, selected_date


def normalize_stock_rows(etf: str, payload: dict) -> list[dict]:
    holdings = []
    seen = set()
    for row in payload.get("data") or []:
        if str(row.get("asset_type", "stock")).strip().lower() not in {"", "stock", "股票"}:
            continue
        stock_code = str(row.get("component_stock_id") or "").strip()
        if not stock_code or stock_code == etf or stock_code in seen:
            continue
        try:
            shares = float(row.get("shares", 0) or 0)
            weight = float(row.get("weight", 0) or 0)
            market_value = float(row.get("market_value", 0) or 0)
        except (TypeError, ValueError):
            continue
        if shares <= 0 and weight <= 0:
            continue
        holdings.append(
            {
                "Stock_Code": stock_code,
                "Stock_Name": str(row.get("component_stock_name") or stock_code).strip(),
                "Weight": weight,
                "Shares": shares,
                "Market_Value": market_value,
            }
        )
        seen.add(stock_code)

    holdings.sort(key=lambda item: item["Weight"], reverse=True)
    if not holdings:
        raise RuntimeError(f"{etf} 沒有可用的股票持股")
    total_weight = sum(item["Weight"] for item in holdings)
    if not 50 <= total_weight <= 105:
        raise RuntimeError(f"{etf} 股票權重合計異常: {total_weight:.4f}%")
    return holdings


def fetch_close_price(
    session: requests.Session,
    token: str,
    stock_id: str,
    source_date: str,
    cache: dict,
) -> float:
    key = (stock_id, source_date)
    if key in cache:
        return cache[key]
    source_dt = date.fromisoformat(source_date)
    close = 0.0
    for attempt in range(3):
        payload = request_finmind(
            session,
            token,
            {
                "dataset": PRICE_DATASET,
                "data_id": stock_id,
                "start_date": source_date,
                "end_date": (source_dt + timedelta(days=1)).isoformat(),
                "limit": 100,
            },
        )
        for row in payload.get("data") or []:
            if str(row.get("date", "")) == source_date:
                close = float(row.get("close", 0) or 0)
                break
        if close > 0:
            break
        if attempt < 2:
            time.sleep(0.25 * (attempt + 1))
    cache[key] = close
    return close


def estimate_net_asset(
    session: requests.Session,
    token: str,
    holdings: list[dict],
    source_rows: list[dict],
    source_date: str,
    price_cache: dict,
) -> tuple[float, str, list[dict]]:
    valuation_rows = []
    for holding in holdings:
        stock_code = holding["Stock_Code"]
        weight = float(holding["Weight"])
        if weight <= 0 or not stock_code.isdigit():
            continue
        market_value = float(holding.get("Market_Value", 0) or 0)
        method = "finmind_market_value"
        close = 0.0
        if market_value <= 0:
            close = fetch_close_price(session, token, stock_code, source_date, price_cache)
            if close <= 0:
                continue
            market_value = float(holding["Shares"]) * close
            method = "finmind_close_x_shares"
        valuation_rows.append(
            {
                "stock_id": stock_code,
                "asset_type": "stock",
                "weight": weight,
                "market_value_twd": market_value,
                "close": close,
                "method": method,
            }
        )

    # 期貨名目市值已包含收盤價、契約乘數與口數，不再重複以收盤價直乘口數。
    for row in source_rows:
        if str(row.get("asset_type", "")).strip().lower() not in {"futures", "期貨"}:
            continue
        futures_code = str(row.get("component_stock_id") or "").strip().upper()
        if futures_code not in {"TX", "MTX", "TE", "TF", "XIF"}:
            continue
        try:
            weight = float(row.get("weight", 0) or 0)
            market_value = float(row.get("market_value", 0) or 0)
        except (TypeError, ValueError):
            continue
        if weight <= 0 or market_value <= 0:
            continue
        valuation_rows.append(
            {
                "stock_id": futures_code,
                "asset_type": "futures",
                "weight": weight,
                "market_value_twd": market_value,
                "close": 0.0,
                "method": "finmind_futures_notional_market_value",
            }
        )

    total_weight = sum(row["weight"] for row in valuation_rows)
    total_market_value = sum(row["market_value_twd"] for row in valuation_rows)
    if total_weight <= 0 or total_market_value <= 0:
        raise RuntimeError("無法使用 FinMind 台幣市值與權重估算基金淨資產")

    net_asset = total_market_value * 100.0 / total_weight
    methods = sorted({row["method"] for row in valuation_rows})
    method_label = "+".join(methods) + "_div_weight"
    return net_asset, method_label, valuation_rows


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(text, encoding=encoding, newline="")
    os.replace(temp_path, path)


def write_normalized_csv(
    path: Path,
    target_label: str,
    source_date: str,
    etf: str,
    holdings: list[dict],
    net_asset: float,
    net_asset_method: str,
) -> None:
    from io import StringIO

    stream = StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(["資料日期", source_date.replace("-", "/")])
    writer.writerow(["檔案標籤日", target_label.replace("-", "/")])
    writer.writerow(["持股來源", "finmind"])
    writer.writerow(["FinMind資料集", HOLDING_DATASET])
    writer.writerow(["FinMind查詢ETF", etf])
    writer.writerow(["基金資產淨值", f"{net_asset:.6f}"])
    writer.writerow(["淨資產估算方式", net_asset_method])
    writer.writerow([])
    writer.writerow(["股票代號", "股票名稱", "持股權重", "股數"])
    for holding in holdings:
        shares = holding["Shares"]
        shares_value = int(shares) if float(shares).is_integer() else shares
        writer.writerow(
            [
                holding["Stock_Code"],
                holding["Stock_Name"],
                f"{holding['Weight']:.6f}",
                shares_value,
            ]
        )
    atomic_write_text(path, "\ufeff" + stream.getvalue(), encoding="utf-8")


def atomic_write_json(path: Path, payload: dict) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    atomic_write_text(path, text, encoding="utf-8")


def atomic_write_dataframe(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temp_path, index=False, encoding="utf-8-sig")
    os.replace(temp_path, path)


def rebuild_date(target_label: str) -> tuple[int, int, str]:
    target_compact = target_label.replace("-", "")
    history_frames = []
    meta_rows = []
    for etf in etf_tracker.TARGET_ETFS:
        frame = etf_tracker.fetch_etf_holdings(etf, target_compact)
        meta = etf_tracker.fetch_etf_meta(etf, target_compact)
        if frame.empty:
            raise RuntimeError(f"{target_label} 仍缺少 {etf} 持股")
        source = meta.get("Holding_Source", "official") if meta else "official"
        frame["ETF"] = etf
        frame["Holding_Source"] = source
        history_frames.append(frame)
        if meta:
            meta_rows.append(meta)

    history_frame = pd.concat(history_frames, ignore_index=True)
    history_frame = history_frame.sort_values(["ETF", "Stock_Code", "Stock_Name"], kind="stable")
    meta_frame = pd.DataFrame(meta_rows).sort_values("ETF", kind="stable")
    atomic_write_dataframe(Path("history") / f"history_{target_compact}.csv", history_frame)
    atomic_write_dataframe(Path("history") / f"fund_meta_{target_compact}.csv", meta_frame)

    previous_dates = sorted(
        path.stem.replace("history_", "")
        for path in Path("history").glob("history_*.csv")
        if path.stem.replace("history_", "") < target_compact
    )
    if not previous_dates:
        raise RuntimeError(f"{target_label} 找不到前一個比較日")
    previous_date = previous_dates[-1]
    available_dates = previous_dates + [target_compact]
    etf_tracker.generate_dashboard(target_compact, previous_date, available_dates, is_root=False)
    return len(history_frame), len(meta_frame), previous_date


def main() -> None:
    parser = argparse.ArgumentParser(description="使用 FinMind 回補指定日期的主動式 ETF 持股")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--etfs", nargs="+", required=True)
    parser.add_argument("--env", default=".env")
    parser.add_argument("--force", action="store_true", help="覆寫已存在的當日檔")
    parser.add_argument("--no-rebuild", action="store_true")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    target_compact = target_date.strftime("%Y%m%d")
    unknown = sorted(set(args.etfs) - set(etf_tracker.TARGET_ETFS))
    if unknown:
        raise RuntimeError(f"不在 TARGET_ETFS: {', '.join(unknown)}")

    token = load_finmind_token(Path(args.env))
    session = requests.Session()
    price_cache = {}
    prepared = []
    for etf in args.etfs:
        target_dir = Path("data") / etf
        existing = list(target_dir.glob(f"{target_compact}.*"))
        if existing and not args.force:
            raise RuntimeError(f"{etf} {args.date} 已有檔案: {existing[0]}")

        payload, source_date = fetch_holding_payload(session, token, etf, target_date)
        holdings = normalize_stock_rows(etf, payload)
        net_asset, method, valuation_rows = estimate_net_asset(
            session, token, holdings, payload.get("data") or [], source_date, price_cache
        )
        prepared.append(
            {
                "etf": etf,
                "payload": payload,
                "source_date": source_date,
                "holdings": holdings,
                "net_asset": net_asset,
                "method": method,
                "valuation_rows": valuation_rows,
            }
        )
        print(
            f"[Ready] {etf} source_date={source_date} stocks={len(holdings)} "
            f"weight={sum(x['Weight'] for x in holdings):.4f}% net_asset={net_asset:,.0f}"
        )

    for item in prepared:
        etf = item["etf"]
        target_dir = Path("data") / etf
        normalized_path = target_dir / f"{target_compact}.csv"
        raw_path = target_dir / f"finmind_raw_{target_compact}.json"
        write_normalized_csv(
            normalized_path,
            args.date,
            item["source_date"],
            etf,
            item["holdings"],
            item["net_asset"],
            item["method"],
        )
        raw_record = {
            "request": {
                "dataset": HOLDING_DATASET,
                "data_id": etf,
                "target_label_date": args.date,
                "selected_source_date": item["source_date"],
            },
            "response": item["payload"],
            "net_asset_estimate": item["net_asset"],
            "net_asset_method": item["method"],
            "valuation_rows": item["valuation_rows"],
            "downloaded_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        atomic_write_json(raw_path, raw_record)
        print(f"[Saved] {normalized_path} ({raw_path.name})")

    if not args.no_rebuild:
        rows, meta_count, previous_date = rebuild_date(args.date)
        print(
            f"[Done] history_{target_compact}.csv rows={rows}, funds={meta_count}, "
            f"comparison={previous_date}->{target_compact}"
        )


if __name__ == "__main__":
    main()
