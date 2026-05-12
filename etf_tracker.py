import os
import glob
import json
import pandas as pd
from datetime import datetime, timedelta
import traceback
import shutil
from playwright.sync_api import sync_playwright

# 設定目標 ETF
TARGET_ETFS = ['00981A', '00988A', '00990A', '00992A', '00982A', '00991A']

# 對應的真實網址字典
URL_MAPPING = {
    '00981A': 'https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW',
    '00988A': 'https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=61YTW',
    '00990A': 'https://www.yuantaetfs.com/product/detail/00990A/ratio',
    '00992A': 'https://www.capitalfund.com.tw/etf/product/detail/500/portfolio',
    '00982A': 'https://www.capitalfund.com.tw/etf/product/detail/399/portfolio',
    '00991A': 'https://www.fhtrust.com.tw/ETF/etf_detail/ETF23#stockhold',
}

def download_all_etfs(today_str):
    """
    使用 Playwright 自動化化瀏覽器下載各家投信的檔案，並歸檔至 data/<ETF代碼>/YYYYMMDD.*
    """
    os.makedirs('data', exist_ok=True)
    
    # 檢查是否已全部下載 (暫時註解掉，讓您可以測試並看到覆蓋效果)
    # all_downloaded = True
    # for etf in TARGET_ETFS:
    #     target_dir = os.path.join('data', etf)
    #     os.makedirs(target_dir, exist_ok=True)
    #     # 尋找是否有今日的 xlsx 或 csv
    #     existing_files = glob.glob(os.path.join(target_dir, f"{today_str}.*"))
    #     if not existing_files:
    #         all_downloaded = False
    #         break
            
    # if all_downloaded:
    #     print(f"[Info] 所有 ETF 今日 ({today_str}) 的檔案均已存在，跳過自動化下載階段。")
    #     return

    print("[Info] 啟動瀏覽器自動下載檔案...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            
            for etf in TARGET_ETFS:
                target_dir = os.path.join('data', etf)
                os.makedirs(target_dir, exist_ok=True)
                
                url = URL_MAPPING.get(etf)
                if not url:
                    print(f"[Warning] 找不到 {etf} 的對應網址，跳過。")
                    continue
                    
                print(f"[Download] 準備下載 {etf} ... 網址: {url}")
                
                # 每個 ETF 都開一個新的 page，避免同一網站 (SPA) 的網頁狀態或快取干擾
                page = context.new_page()
                
                try:
                    page.goto(url, wait_until='networkidle', timeout=30000)
                    
                    # 等待一下確保按鈕渲染與資料載入
                    page.wait_for_timeout(3000)
                    
                    # 尋找下載按鈕
                    button = None
                    if 'ezmoney.com.tw' in url:
                        button = page.locator("text=/匯出(XLSX|EXCEL)檔?/i").first
                    elif 'yuantaetfs.com' in url:
                        button = page.locator("text=/匯出(excel)?/i").first
                    elif 'capitalfund.com.tw' in url:
                        button = page.locator("text=/下載資料/i").first
                    elif 'fhtrust.com.tw' in url:
                        button = page.locator("text=/檔案下載/i").first
                        
                    if button and button.count() > 0:
                        # 綁定下載事件，expect_download 會攔截下載
                        with page.expect_download(timeout=15000) as download_info:
                            # 透過 JavaScript 點擊，無視任何浮動元素遮擋
                            button.evaluate("el => el.click()")
                        
                        # 取得下載物件
                        download = download_info.value
                        
                        # 確認副檔名
                        ext = 'xlsx'
                        if download.suggested_filename:
                            file_ext = download.suggested_filename.split('.')[-1].lower()
                            if file_ext in ['xlsx', 'csv', 'xls']:
                                ext = file_ext
                                
                        # Playwright 的 download.save_as() 是同步方法，會「等待下載完全結束」才寫入到指定路徑
                        save_path = os.path.join(target_dir, f"{today_str}.{ext}")
                        download.save_as(save_path)
                        print(f"[Success] {etf} 下載完成且已儲存至: {save_path}")
                    else:
                        print(f"[Error] {etf} 找不到對應的下載按鈕或無規則")
                    
                except Exception as e:
                    print(f"[Error] 下載 {etf} 失敗: {e}")
                finally:
                    # 無論成功失敗，下載完該檔就關閉分頁
                    page.close()
                    
            browser.close()
    except Exception as e:
        print(f"[Error] Playwright 啟動失敗: {e}")

def fetch_etf_holdings(etf_code, today_str):
    """
    從本地端資料夾讀取 ETF 持股檔案
    """
    target_dir = os.path.join('data', etf_code)
    if not os.path.exists(target_dir):
        print(f"[Error] 目錄 {target_dir} 不存在，請先將今日的 Excel 檔案放入 data/{etf_code}/ 目錄中")
        return pd.DataFrame()
        
    files = glob.glob(os.path.join(target_dir, f"{today_str}.*"))
    if not files:
        print(f"[Error] 缺少 {etf_code} 檔案，請先將今日的 Excel 檔案放入 data/{etf_code}/ 目錄中 (檔名: {today_str}.xlsx 或 csv)")
        return pd.DataFrame()
        
    file_path = files[0]
    print(f"[Info] 讀取本地檔案: {file_path}")
    
    df = pd.DataFrame()
    try:
        if file_path.endswith('.csv'):
            try:
                df_raw = pd.read_csv(file_path, encoding='utf-8-sig', names=range(20), engine='python')
            except Exception:
                df_raw = pd.read_csv(file_path, encoding='cp950', names=range(20), engine='python')
        else:
            # 判斷應該使用的引擎
            engine_type = 'openpyxl' if file_path.endswith('.xlsx') else 'xlrd'
            excel = pd.ExcelFile(file_path, engine=engine_type)
            target_sheet = excel.sheet_names[0]
            for s in excel.sheet_names:
                if '股' in s or '明細' in s or '成' in s:
                    target_sheet = s
                    break
            df_raw = pd.read_excel(file_path, sheet_name=target_sheet, header=None, engine=engine_type)
            
        # 尋找真正的表格頭
        start_row = 0
        col_mapping = {}
        found_header = False
        
        for i in range(min(40, len(df_raw))):
            row_values = [str(x) for x in df_raw.iloc[i].values]
            joined = "".join(row_values)
            if ('代' in joined or '名' in joined) and ('權' in joined or '股' in joined):
                start_row = i + 1
                df_raw.columns = df_raw.iloc[i]
                found_header = True
                break
                
        if found_header:
            df_raw = df_raw.iloc[start_row:].reset_index(drop=True)
            
        df_raw.columns = [str(c).replace(' ', '').replace('\n', '') for c in df_raw.columns]
        
        for col in df_raw.columns:
            if '代' in col and ('碼' in col or '號' in col): col_mapping[col] = 'Stock_Code'
            elif '名' in col and '稱' in col: col_mapping[col] = 'Stock_Name'
            elif '權' in col and '重' in col: col_mapping[col] = 'Weight'
            elif '比' in col and '重' in col: col_mapping[col] = 'Weight'
            elif '比' in col and '例' in col: col_mapping[col] = 'Weight'
            elif ('股' in col and '數' in col) or '數量' in col or '股數' in col: col_mapping[col] = 'Shares'
            
        df = df_raw.rename(columns=col_mapping)
        
        for required_col in ['Stock_Code', 'Stock_Name', 'Weight', 'Shares']:
            if required_col not in df.columns:
                df[required_col] = 0 if required_col in ['Weight', 'Shares'] else ''
                
        df = df[['Stock_Code', 'Stock_Name', 'Weight', 'Shares']].copy()
        
        # 1. 尋找邊界 (Boundary) 與 2. 截斷資料 (Truncate)
        truncate_keywords = ['投資', '區域', '現金', '小計', '合計']
        for index, row in df.iterrows():
            code_str = str(row['Stock_Code'])
            name_str = str(row['Stock_Name'])
            if any(kw in code_str or kw in name_str for kw in truncate_keywords):
                df = df.iloc[:index]
                break
                
        # 3. 基礎清理：移除黑名單與強制轉數值的複雜代碼，只保留基本的 dropna
        df['Stock_Code'] = df['Stock_Code'].replace(['', 'nan', 'None', 'NaT'], pd.NA)
        df = df.dropna(subset=['Stock_Code'])
        
        # 清理資料：確保代碼與名稱皆為字串，並清除前後隱藏空白字元
        df['Stock_Code'] = df['Stock_Code'].astype(str).str.strip()
        df['Stock_Name'] = df['Stock_Name'].astype(str).str.strip()
        
        # 嘗試將數字轉為數值，若無法轉換則補 0 (簡單處理)
        df['Weight'] = pd.to_numeric(df['Weight'].astype(str).str.replace('%', '').str.replace(',', ''), errors='coerce').fillna(0)
        df['Shares'] = pd.to_numeric(df['Shares'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
    except Exception as e:
        print(f"[Error] 讀取 {file_path} 失敗: {e}")
        traceback.print_exc()

    return df

def calculate_changes(df_yesterday, df_today, is_first_run=False):
    """
    計算昨日與今日持股權重與股數差異
    """
    if df_today.empty and df_yesterday.empty:
        return pd.DataFrame()
        
    merged = pd.merge(
        df_today, 
        df_yesterday, 
        on=['Stock_Code', 'Stock_Name'], 
        how='outer', 
        suffixes=('_Today', '_Yesterday')
    )
    
    # 填補空值
    for col in ['Weight_Today', 'Weight_Yesterday', 'Shares_Today', 'Shares_Yesterday']:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)
        else:
            merged[col] = 0
            
    if is_first_run:
        merged['Weight_Change'] = 0.0
        merged['Shares_Change'] = 0.0
        merged['Shares_Change_Pct'] = 0.0
        merged['Weight_Change_Display'] = "基準建立中"
        merged['Shares_Change_Display'] = "基準建立中"
        merged['Yesterday_Shares_Display'] = "-"
    else:
        merged['Weight_Change'] = merged['Weight_Today'] - merged['Weight_Yesterday']
        merged['Shares_Change'] = merged['Shares_Today'] - merged['Shares_Yesterday']
        
        def calc_pct(row):
            sy = row['Shares_Yesterday']
            st = row['Shares_Today']
            if sy == 0 and st > 0:
                return float('inf')
            elif sy > 0 and st == 0:
                return -100.0
            elif sy == 0 and st == 0:
                return 0.0
            else:
                return (st - sy) / sy * 100.0
                
        merged['Shares_Change_Pct'] = merged.apply(calc_pct, axis=1)
        
        merged['Weight_Change_Display'] = merged['Weight_Change'].apply(lambda x: f"{x:+.2f}%" if x != 0 else "0.00%")
        merged['Shares_Change_Display'] = merged['Shares_Change'].apply(lambda x: f"{x:+,.0f}" if x != 0 else "0")
        merged['Yesterday_Shares_Display'] = merged['Shares_Yesterday'].apply(lambda x: f"{x:,.0f}")
        
    return merged

def get_top_changes(df_changes, top_n=3, sort_by='Shares_Change'):
    """
    取得增減 TOP N 的持股
    """
    if df_changes.empty:
        return pd.DataFrame(), pd.DataFrame()
        
    # 將資料嚴格拆分為「大於 0 的增持」與「小於 0 的減持」
    df_increases = df_changes[df_changes[sort_by] > 0]
    df_decreases = df_changes[df_changes[sort_by] < 0]
    
    top_increases = df_increases.nlargest(top_n, sort_by)
    top_decreases = df_decreases.nsmallest(top_n, sort_by)
    
    return top_increases, top_decreases

def format_number(val, is_float=False):
    try:
        if is_float:
            return f"{float(val):.2f}"
        return f"{int(val):,.0f}"
    except:
        return val

def legacy_generate_dashboard(target_date_str, prev_date_str, available_dates, is_root=False):
    target_file = f"history/history_{target_date_str}.csv"
    if not os.path.exists(target_file):
        return
        
    year = target_date_str[:4]
    month = target_date_str[4:6]

    df_today_all = pd.read_csv(target_file, dtype={'Stock_Code': str})
    
    is_first_run = False
    if prev_date_str:
        prev_file = f"history/history_{prev_date_str}.csv"
        df_yesterday_all = pd.read_csv(prev_file, dtype={'Stock_Code': str})
    else:
        is_first_run = True
        df_yesterday_all = pd.DataFrame(columns=['Stock_Code', 'Stock_Name', 'Weight', 'Shares', 'ETF'])
        
    if not df_today_all.empty:
        df_today_all['Stock_Code'] = df_today_all['Stock_Code'].astype(str)
        
    if not df_yesterday_all.empty:
        df_yesterday_all['Stock_Code'] = df_yesterday_all['Stock_Code'].astype(str)
        
    all_changes = []
    etf_results = {}
    
    for etf in TARGET_ETFS:
        df_yest = df_yesterday_all[df_yesterday_all['ETF'] == etf].copy() if not df_yesterday_all.empty else pd.DataFrame(columns=['Stock_Code', 'Stock_Name', 'Weight', 'Shares', 'ETF'])
        df_tod = df_today_all[df_today_all['ETF'] == etf].copy() if not df_today_all.empty else pd.DataFrame(columns=['Stock_Code', 'Stock_Name', 'Weight', 'Shares', 'ETF'])
        
        if df_tod.empty and df_yest.empty:
            continue
            
        # 如果該檔 ETF 昨天完全沒有資料 (例如新加入追蹤，或昨天下載失敗)，則將其視為首日建立基準，不計算變動差異
        etf_is_first_run = is_first_run or df_yest.empty
            
        changes = calculate_changes(df_yest, df_tod, etf_is_first_run)
        if not changes.empty:
            changes['ETF'] = etf
            all_changes.append(changes)
            inc, dec = get_top_changes(changes, top_n=5, sort_by='Shares_Change')
            inc_pct, dec_pct = get_top_changes(changes, top_n=5, sort_by='Shares_Change_Pct')
            etf_results[etf] = {
                'changes': changes,
                'inc': inc,
                'dec': dec,
                'inc_pct': inc_pct,
                'dec_pct': dec_pct
            }

    def format_date(d_str):
        if len(d_str) == 8:
            return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        return d_str

    if prev_date_str:
        date_display_str = f"{format_date(prev_date_str)} vs {format_date(target_date_str)}"
    else:
        date_display_str = f"無前日資料 vs {format_date(target_date_str)}"

    # 建立下拉選單 HTML (反向排序讓最新日期在最上方)
    options_html = ""
    for d in reversed(available_dates):
        selected = 'selected' if d == target_date_str else ''
        d_year = d[:4]
        d_month = d[4:6]
        
        if is_root:
            link = f"./dashboards/{d_year}/{d_month}/index_{d}.html"
        else:
            link = f"../../../dashboards/{d_year}/{d_month}/index_{d}.html"
            
        options_html += f"            <option value='{link}' {selected}>{format_date(d)}</option>\n"
        
    select_html = f"""
    <div style="text-align: center; margin-bottom: 20px;">
        <label for="dateSelect" style="font-size: 1.1em; font-weight: bold; color: #2c3e50;">切換歷史日期：</label>
        <select id="dateSelect" style="padding: 5px 10px; font-size: 1em; border-radius: 4px; border: 1px solid #bdc3c7;" onchange="window.location.href=this.value">
{options_html}        </select>
    </div>
    """

    # 產生 HTML
    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <title>主動式 ETF 持股變化追蹤</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 30px; color: #333; background-color: #f9f9f9; line-height: 1.6; }}
            h1 {{ color: #2c3e50; text-align: center; border-bottom: 3px solid #3498db; padding-bottom: 15px; margin-bottom: 10px; }}
            h2 {{ color: #ffffff; background-color: #2980b9; padding: 10px 20px; border-radius: 5px; margin-top: 50px; font-size: 1.5em; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            h3 {{ color: #2c3e50; border-left: 5px solid #16a085; padding-left: 15px; margin-top: 35px; background-color: #eef7f5; padding: 10px 15px; border-radius: 0 5px 5px 0; }}
            h4 {{ color: #e67e22; margin-top: 20px; font-size: 1.1em; padding-left: 10px; }}
            .date-subtitle {{ text-align: center; color: #7f8c8d; font-size: 1.1em; margin-bottom: 20px; font-weight: bold; }}
            .warning-msg {{ background-color: #f39c12; color: white; padding: 15px; text-align: center; border-radius: 5px; font-size: 1.1em; margin-bottom: 30px; font-weight: bold; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            .styled-table {{ 
                border-collapse: collapse; 
                margin: 15px 0 30px 0; 
                font-size: 0.95em; 
                width: 100%; 
                box-shadow: 0 0 20px rgba(0, 0, 0, 0.05); 
                background-color: white;
                border-radius: 5px;
                overflow: hidden;
            }}
            .styled-table thead tr {{ background-color: #34495e; color: #ffffff; text-align: right; }}
            .styled-table thead th {{ padding: 15px; border: 1px solid #ecf0f1; text-align: center; font-weight: 600; letter-spacing: 0.5px; }}
            .styled-table th, .styled-table td {{ padding: 12px 15px; border: 1px solid #ecf0f1; text-align: right; }}
            .styled-table td:nth-child(1), .styled-table td:nth-child(2) {{ text-align: left; font-weight: bold; color: #2c3e50; }}
            .styled-table tbody tr {{ border-bottom: 1px solid #ecf0f1; transition: all 0.2s ease; }}
            .styled-table tbody tr:nth-of-type(even) {{ background-color: #fcfcfc; }}
            .styled-table tbody tr:hover {{ background-color: #e8f4f8; transform: scale(1.001); }}
            .empty-msg {{ color: #7f8c8d; font-style: italic; margin-left: 20px; background-color: #f1f2f6; padding: 10px; border-radius: 4px; display: inline-block; }}
            .tab-btn {{ background-color: #ecf0f1; border: none; padding: 8px 16px; margin-right: 5px; cursor: pointer; border-radius: 4px 4px 0 0; font-weight: bold; color: #7f8c8d; }}
            .tab-btn.active {{ background-color: #3498db; color: white; }}
            .tab-content {{ display: none; }}
            .tab-content.active {{ display: block; }}
            .tab-container {{ margin-bottom: 20px; }}
        </style>
        <script>
        function switchTab(containerId, tabType, btn) {{
            var container = document.getElementById(containerId);
            var contents = container.getElementsByClassName('tab-content');
            for (var i = 0; i < contents.length; i++) {{
                contents[i].classList.remove('active');
            }}
            var btns = btn.parentElement.getElementsByClassName('tab-btn');
            for (var i = 0; i < btns.length; i++) {{
                btns[i].classList.remove('active');
            }}
            container.querySelector('.' + tabType).classList.add('active');
            btn.classList.add('active');
        }}
        </script>
    </head>
    <body>
        <h1>主動式 ETF 持股變化追蹤</h1>
        {select_html}
        <div class="date-subtitle">比較日期: {date_display_str}</div>
    """
    
    if is_first_run:
        html_content += """
        <div class="warning-msg">
            [注意] 缺乏前日資料，此日期僅建立基準檔，變動欄位顯示為「基準建立中」。
        </div>
        """

    def format_pct(x):
        if x == float('inf'): return "新建倉"
        elif x == -100.0: return "清倉"
        else: return f"{x:+.2f}%"

    # 區塊一：總體市場 TOP 5 增減持股
    html_content += "<h2>區塊一：總體市場 TOP 5 增減持股</h2>\n"
    if all_changes:
        combined_df = pd.concat(all_changes, ignore_index=True)
        overall = combined_df.groupby(['Stock_Code', 'Stock_Name']).agg({
            'Shares_Change': 'sum',
            'Weight_Change': 'sum',
            'Shares_Today': 'sum',
            'Shares_Yesterday': 'sum'
        }).reset_index()
        
        def calc_overall_pct(row):
            sy = row['Shares_Yesterday']
            st = row['Shares_Today']
            if sy == 0 and st > 0: return float('inf')
            elif sy > 0 and st == 0: return -100.0
            elif sy == 0 and st == 0: return 0.0
            else: return (st - sy) / sy * 100.0
            
        overall['Shares_Change_Pct'] = overall.apply(calc_overall_pct, axis=1)
        
        overall_inc, overall_dec = get_top_changes(overall, top_n=5, sort_by='Shares_Change')
        overall_inc_pct, overall_dec_pct = get_top_changes(overall, top_n=5, sort_by='Shares_Change_Pct')
        
        html_content += f"""
        <div class="tab-container" id="overall-tabs">
            <div>
                <button class="tab-btn active" onclick="switchTab('overall-tabs', 'tab-vol', this)">依股數增減 (量體)</button>
                <button class="tab-btn" onclick="switchTab('overall-tabs', 'tab-pct', this)">依變動幅度 (意圖)</button>
            </div>
            <div class="tab-content tab-vol active">
        """
        
        html_content += "<h3>[增] 總體資金增持 TOP 5 (量體)</h3>\n"
        if not overall_inc.empty:
            df_disp = overall_inc[['Stock_Code', 'Stock_Name', 'Shares_Change', 'Weight_Change', 'Shares_Today']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '總股數變動', '總權重變動(%)', '總體最新股數']
            df_disp['總股數變動'] = df_disp['總股數變動'].apply(lambda x: format_number(x))
            df_disp['總權重變動(%)'] = df_disp['總權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            df_disp['總體最新股數'] = df_disp['總體最新股數'].apply(lambda x: format_number(x))
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無增持紀錄</p>\n"
            
        html_content += "<h3>[減] 總體資金減持 TOP 5 (量體)</h3>\n"
        if not overall_dec.empty:
            df_disp = overall_dec[['Stock_Code', 'Stock_Name', 'Shares_Change', 'Weight_Change', 'Shares_Today']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '總股數變動', '總權重變動(%)', '總體最新股數']
            df_disp['總股數變動'] = df_disp['總股數變動'].apply(lambda x: format_number(x))
            df_disp['總權重變動(%)'] = df_disp['總權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            df_disp['總體最新股數'] = df_disp['總體最新股數'].apply(lambda x: format_number(x))
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無減持紀錄</p>\n"
            
        html_content += "</div>\n<div class=\"tab-content tab-pct\">\n"
        
        html_content += "<h3>[增] 總體資金增持幅度 TOP 5 (意圖)</h3>\n"
        if not overall_inc_pct.empty:
            df_disp = overall_inc_pct[['Stock_Code', 'Stock_Name', 'Shares_Change_Pct', 'Shares_Change', 'Weight_Change']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '變動幅度', '總股數變動', '總權重變動(%)']
            df_disp['變動幅度'] = df_disp['變動幅度'].apply(format_pct)
            df_disp['總股數變動'] = df_disp['總股數變動'].apply(lambda x: format_number(x))
            df_disp['總權重變動(%)'] = df_disp['總權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無增持紀錄</p>\n"
            
        html_content += "<h3>[減] 總體資金減持幅度 TOP 5 (意圖)</h3>\n"
        if not overall_dec_pct.empty:
            df_disp = overall_dec_pct[['Stock_Code', 'Stock_Name', 'Shares_Change_Pct', 'Shares_Change', 'Weight_Change']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '變動幅度', '總股數變動', '總權重變動(%)']
            df_disp['變動幅度'] = df_disp['變動幅度'].apply(format_pct)
            df_disp['總股數變動'] = df_disp['總股數變動'].apply(lambda x: format_number(x))
            df_disp['總權重變動(%)'] = df_disp['總權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無減持紀錄</p>\n"
            
        html_content += "</div>\n</div>\n"
    else:
        html_content += "<p class='empty-msg'>目前無任何變動資料</p>\n"

    # 區塊二：各檔 ETF 獨立的 TOP 5 增減持股
    html_content += "<h2>區塊二：各檔 ETF 獨立的 TOP 5 增減持股</h2>\n"
    for etf in TARGET_ETFS:
        html_content += f"<h3>[{etf}] 持股變動排行榜</h3>\n"
        if etf not in etf_results:
            html_content += f"<p class='empty-msg'>尚無 {etf} 資料</p>\n"
            continue
            
        inc = etf_results[etf]['inc']
        dec = etf_results[etf]['dec']
        inc_pct = etf_results[etf]['inc_pct']
        dec_pct = etf_results[etf]['dec_pct']
        
        html_content += f"""
        <div class="tab-container" id="etf-tabs-{etf}">
            <div>
                <button class="tab-btn active" onclick="switchTab('etf-tabs-{etf}', 'tab-vol', this)">依股數增減 (量體)</button>
                <button class="tab-btn" onclick="switchTab('etf-tabs-{etf}', 'tab-pct', this)">依變動幅度 (意圖)</button>
            </div>
            <div class="tab-content tab-vol active">
        """
        
        html_content += "<h4>[增] 增持最多 TOP 5 (量體)</h4>\n"
        if not inc.empty:
            df_disp = inc[['Stock_Code', 'Stock_Name', 'Shares_Change', 'Weight_Change', 'Shares_Today', 'Weight_Today']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '股數變動', '權重變動(%)', '最新股數', '最新權重(%)']
            df_disp['股數變動'] = df_disp['股數變動'].apply(lambda x: format_number(x))
            df_disp['權重變動(%)'] = df_disp['權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            df_disp['最新股數'] = df_disp['最新股數'].apply(lambda x: format_number(x))
            df_disp['最新權重(%)'] = df_disp['最新權重(%)'].apply(lambda x: f"{x:.2f}%")
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無增持紀錄</p>\n"
            
        html_content += "<h4>[減] 減持最多 TOP 5 (量體)</h4>\n"
        if not dec.empty:
            df_disp = dec[['Stock_Code', 'Stock_Name', 'Shares_Change', 'Weight_Change', 'Shares_Today', 'Weight_Today']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '股數變動', '權重變動(%)', '最新股數', '最新權重(%)']
            df_disp['股數變動'] = df_disp['股數變動'].apply(lambda x: format_number(x))
            df_disp['權重變動(%)'] = df_disp['權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            df_disp['最新股數'] = df_disp['最新股數'].apply(lambda x: format_number(x))
            df_disp['最新權重(%)'] = df_disp['最新權重(%)'].apply(lambda x: f"{x:.2f}%")
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無減持紀錄</p>\n"
            
        html_content += "</div>\n<div class=\"tab-content tab-pct\">\n"
        
        html_content += "<h4>[增] 增持幅度 TOP 5 (意圖)</h4>\n"
        if not inc_pct.empty:
            df_disp = inc_pct[['Stock_Code', 'Stock_Name', 'Shares_Change_Pct', 'Shares_Change', 'Weight_Change']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '變動幅度', '股數變動', '權重變動(%)']
            df_disp['變動幅度'] = df_disp['變動幅度'].apply(format_pct)
            df_disp['股數變動'] = df_disp['股數變動'].apply(lambda x: format_number(x))
            df_disp['權重變動(%)'] = df_disp['權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無增持紀錄</p>\n"
            
        html_content += "<h4>[減] 減持幅度 TOP 5 (意圖)</h4>\n"
        if not dec_pct.empty:
            df_disp = dec_pct[['Stock_Code', 'Stock_Name', 'Shares_Change_Pct', 'Shares_Change', 'Weight_Change']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '變動幅度', '股數變動', '權重變動(%)']
            df_disp['變動幅度'] = df_disp['變動幅度'].apply(format_pct)
            df_disp['股數變動'] = df_disp['股數變動'].apply(lambda x: format_number(x))
            df_disp['權重變動(%)'] = df_disp['權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無減持紀錄</p>\n"
            
        html_content += "</div>\n</div>\n"

    # 區塊三：各檔 ETF 詳細持股明細
    html_content += "<h2>區塊三：各檔 ETF 詳細持股明細</h2>\n"
    for etf in TARGET_ETFS:
        html_content += f"<details style='margin-bottom: 20px; background-color: #ffffff; padding: 10px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.05);'>\n"
        html_content += f"  <summary style='cursor: pointer; font-size: 1.2em; font-weight: bold; color: #2c3e50; padding: 10px; border-left: 5px solid #16a085; background-color: #eef7f5; border-radius: 0 5px 5px 0; list-style-position: inside;'>[{etf}] 完整持股清單 (點擊展開/收合)</summary>\n"
        html_content += "  <div style='margin-top: 15px;'>\n"
        if etf not in etf_results:
            html_content += f"  <p class='empty-msg'>尚無 {etf} 資料</p>\n  </div>\n</details>\n"
            continue
            
        changes = etf_results[etf]['changes']
        if not changes.empty:
            # 依照權重大小由高至低排序
            changes = changes.sort_values(by='Weight_Today', ascending=False)
            
            df_disp = changes[['Stock_Code', 'Stock_Name', 'Shares_Today', 'Weight_Today', 'Yesterday_Shares_Display', 'Shares_Change_Display', 'Weight_Change_Display']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '今日股數', '今日權重(%)', '昨日股數', '股數變化', '權重變化(%)']
            
            # 數值格式化
            df_disp['今日股數'] = df_disp['今日股數'].apply(lambda x: format_number(x))
            df_disp['今日權重(%)'] = df_disp['今日權重(%)'].apply(lambda x: f"{x:.2f}%")
            
            html_content += df_disp.to_html(index=False, classes="styled-table", escape=False)
        else:
            html_content += "<p class='empty-msg'>無明細資料</p>\n"
            
        html_content += "  </div>\n</details>\n"

    html_content += """
    </body>
    </html>
    """
    
    if is_root:
        out_file = "index.html"
    else:
        out_dir = f"dashboards/{year}/{month}"
        os.makedirs(out_dir, exist_ok=True)
        out_file = f"{out_dir}/index_{target_date_str}.html"
        
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[Done] 已產生網頁: {out_file}")

def format_number(val, is_float=False):
    try:
        if pd.isna(val):
            return "-"
        if is_float:
            return f"{float(val):.2f}"
        return f"{int(round(float(val))):,.0f}"
    except Exception:
        return val

def format_date(d_str):
    if isinstance(d_str, str) and len(d_str) == 8:
        return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
    return d_str

def format_pct(x):
    if x == float('inf'):
        return "新建倉"
    if x == -100.0:
        return "清倉"
    try:
        return f"{float(x):+.2f}%"
    except Exception:
        return x

def empty_history_df():
    return pd.DataFrame(columns=['Stock_Code', 'Stock_Name', 'Weight', 'Shares', 'ETF'])

def read_history(date_str):
    path = f"history/history_{date_str}.csv"
    if not date_str or not os.path.exists(path):
        return empty_history_df()
    df = pd.read_csv(path, dtype={'Stock_Code': str})
    if not df.empty:
        df['Stock_Code'] = df['Stock_Code'].astype(str)
        df['Stock_Name'] = df['Stock_Name'].astype(str)
        df['Shares'] = pd.to_numeric(df['Shares'], errors='coerce').fillna(0)
        df['Weight'] = pd.to_numeric(df['Weight'], errors='coerce').fillna(0)
    return df

def calc_change_pct(start_shares, end_shares):
    if start_shares == 0 and end_shares > 0:
        return float('inf')
    if start_shares > 0 and end_shares == 0:
        return -100.0
    if start_shares == 0 and end_shares == 0:
        return 0.0
    return (end_shares - start_shares) / start_shares * 100.0

def build_total_share_changes(df_start, df_end):
    key_cols = ['Stock_Code', 'Stock_Name']
    if df_start.empty:
        start = pd.DataFrame(columns=key_cols + ['Start_Shares', 'Start_ETF_Count'])
    else:
        start = df_start.groupby(key_cols, dropna=False).agg(
            Start_Shares=('Shares', 'sum'),
            Start_ETF_Count=('ETF', 'nunique')
        ).reset_index()
    if df_end.empty:
        end = pd.DataFrame(columns=key_cols + ['End_Shares', 'End_ETF_Count'])
    else:
        end = df_end.groupby(key_cols, dropna=False).agg(
            End_Shares=('Shares', 'sum'),
            End_ETF_Count=('ETF', 'nunique')
        ).reset_index()
    overall = pd.merge(end, start, on=key_cols, how='outer')
    for col in ['Start_Shares', 'End_Shares', 'Start_ETF_Count', 'End_ETF_Count']:
        overall[col] = pd.to_numeric(overall[col], errors='coerce').fillna(0)
    overall['Total_Shares_Change'] = overall['End_Shares'] - overall['Start_Shares']
    overall['Total_Shares_Change_Pct'] = overall.apply(
        lambda row: calc_change_pct(row['Start_Shares'], row['End_Shares']), axis=1
    )
    overall['ETF_Count'] = overall[['Start_ETF_Count', 'End_ETF_Count']].max(axis=1).astype(int)
    return overall

def build_etf_results(df_start, df_end, is_first_run=False):
    results = {}
    for etf in TARGET_ETFS:
        df_yest = df_start[df_start['ETF'] == etf].copy() if not df_start.empty else empty_history_df()
        df_tod = df_end[df_end['ETF'] == etf].copy() if not df_end.empty else empty_history_df()
        if df_tod.empty and df_yest.empty:
            continue
        etf_is_first_run = is_first_run or df_yest.empty
        changes = calculate_changes(df_yest, df_tod, etf_is_first_run)
        if changes.empty:
            continue
        changes['ETF'] = etf
        inc, dec = get_top_changes(changes, top_n=5, sort_by='Shares_Change')
        inc_pct, dec_pct = get_top_changes(changes, top_n=5, sort_by='Shares_Change_Pct')
        results[etf] = {'changes': changes, 'inc': inc, 'dec': dec, 'inc_pct': inc_pct, 'dec_pct': dec_pct}
    return results

def relative_prefix(depth):
    return "../" * depth

def nav_html(prefix="", active="daily"):
    links = [
        ("daily", "每日總覽", f"{prefix}index.html"),
        ("weekly", "每週歷史", f"{prefix}weekly/index.html"),
        ("range", "自訂區間", f"{prefix}range/index.html"),
    ]
    return "<nav class='site-nav'>" + "".join(
        f"<a class='{'active' if key == active else ''}' href='{href}'>{label}</a>"
        for key, label, href in links
    ) + "</nav>"

def page_head(title):
    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 30px; color: #333; background-color: #f9f9f9; line-height: 1.6; }}
        h1 {{ color: #2c3e50; text-align: center; border-bottom: 3px solid #3498db; padding-bottom: 15px; margin-bottom: 10px; }}
        h2 {{ color: #ffffff; background-color: #2980b9; padding: 10px 20px; border-radius: 5px; margin-top: 50px; font-size: 1.5em; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        h3 {{ color: #2c3e50; border-left: 5px solid #16a085; padding-left: 15px; margin-top: 35px; background-color: #eef7f5; padding: 10px 15px; border-radius: 0 5px 5px 0; }}
        h4 {{ color: #e67e22; margin-top: 20px; font-size: 1.1em; padding-left: 10px; }}
        .date-subtitle {{ text-align: center; color: #7f8c8d; font-size: 1.1em; margin-bottom: 20px; font-weight: bold; }}
        .warning-msg {{ background-color: #f39c12; color: white; padding: 15px; text-align: center; border-radius: 5px; font-size: 1.1em; margin-bottom: 30px; font-weight: bold; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        .styled-table {{ border-collapse: collapse; margin: 15px 0 30px 0; font-size: 0.95em; width: 100%; box-shadow: 0 0 20px rgba(0,0,0,0.05); background-color: white; border-radius: 5px; overflow: hidden; }}
        .styled-table thead tr {{ background-color: #34495e; color: #ffffff; text-align: right; }}
        .styled-table thead th {{ padding: 15px; border: 1px solid #ecf0f1; text-align: center; font-weight: 600; letter-spacing: 0.5px; }}
        .styled-table th, .styled-table td {{ padding: 12px 15px; border: 1px solid #ecf0f1; text-align: right; }}
        .styled-table td:nth-child(1), .styled-table td:nth-child(2) {{ text-align: left; font-weight: bold; color: #2c3e50; }}
        .styled-table tbody tr {{ border-bottom: 1px solid #ecf0f1; transition: all 0.2s ease; }}
        .styled-table tbody tr:nth-of-type(even) {{ background-color: #fcfcfc; }}
        .styled-table tbody tr:hover {{ background-color: #e8f4f8; transform: scale(1.001); }}
        .empty-msg {{ color: #7f8c8d; font-style: italic; margin-left: 20px; background-color: #f1f2f6; padding: 10px; border-radius: 4px; display: inline-block; }}
        .tab-btn {{ background-color: #ecf0f1; border: none; padding: 8px 16px; margin-right: 5px; cursor: pointer; border-radius: 4px 4px 0 0; font-weight: bold; color: #7f8c8d; }}
        .tab-btn.active {{ background-color: #3498db; color: white; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
        .tab-container {{ margin-bottom: 20px; }}
        .site-nav {{ display: flex; gap: 8px; justify-content: center; margin: 14px 0 24px; flex-wrap: wrap; }}
        .site-nav a {{ color: #2c3e50; text-decoration: none; background: #ecf0f1; padding: 8px 14px; border-radius: 4px; font-weight: 700; }}
        .site-nav a.active {{ color: white; background: #2980b9; }}
        .control-panel {{ background: white; border: 1px solid #ecf0f1; border-radius: 5px; padding: 14px; margin: 20px 0; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }}
        .control-panel label {{ font-weight: 700; color: #2c3e50; }}
        .control-panel select, .control-panel button {{ padding: 6px 10px; border-radius: 4px; border: 1px solid #bdc3c7; font-size: 1em; }}
        .control-panel button {{ color: white; background: #2980b9; border-color: #2980b9; font-weight: 700; cursor: pointer; }}
    </style>
    <script>
    function switchTab(containerId, tabType, btn) {{
        var container = document.getElementById(containerId);
        var contents = container.getElementsByClassName('tab-content');
        for (var i = 0; i < contents.length; i++) contents[i].classList.remove('active');
        var btns = btn.parentElement.getElementsByClassName('tab-btn');
        for (var j = 0; j < btns.length; j++) btns[j].classList.remove('active');
        container.querySelector('.' + tabType).classList.add('active');
        btn.classList.add('active');
    }}
    </script>
</head>
<body>
"""

def page_tail():
    return "</body>\n</html>\n"

def render_date_select(available_dates, target_date_str, depth):
    prefix = relative_prefix(depth)
    options = ""
    for d in reversed(available_dates):
        selected = 'selected' if d == target_date_str else ''
        link = f"{prefix}dashboards/{d[:4]}/{d[4:6]}/index_{d}.html"
        options += f"            <option value='{link}' {selected}>{format_date(d)}</option>\n"
    return f"""
    <div style="text-align: center; margin-bottom: 20px;">
        <label for="dateSelect" style="font-size: 1.1em; font-weight: bold; color: #2c3e50;">切換歷史日期：</label>
        <select id="dateSelect" style="padding: 5px 10px; font-size: 1em; border-radius: 4px; border: 1px solid #bdc3c7;" onchange="window.location.href=this.value">
{options}        </select>
    </div>
    """

def render_week_select(week_rows, current_key, depth):
    prefix = relative_prefix(depth)
    options = ""
    for row in reversed(week_rows):
        selected = "selected" if row['key'] == current_key else ""
        options += f"            <option value='{prefix}{row['href']}' {selected}>{row['key']}（{format_date(row['start'])} ~ {format_date(row['end'])}）</option>\n"
    return f"""
    <div style="text-align: center; margin-bottom: 20px;">
        <label for="weekSelect" style="font-size: 1.1em; font-weight: bold; color: #2c3e50;">切換歷史週別：</label>
        <select id="weekSelect" style="padding: 5px 10px; font-size: 1em; border-radius: 4px; border: 1px solid #bdc3c7;" onchange="window.location.href=this.value">
{options}        </select>
    </div>
    """

def render_total_table(df):
    if df.empty:
        return "<p class='empty-msg'>無資料</p>\n"
    df_disp = df[['Stock_Code', 'Stock_Name', 'Total_Shares_Change', 'Total_Shares_Change_Pct', 'Start_Shares', 'End_Shares', 'ETF_Count']].copy()
    df_disp.columns = ['股票代碼', '股票名稱', '總股數變動', '總持股變動幅度(%)', '起始總股數', '最新總股數', '出現ETF數']
    df_disp['總股數變動'] = df_disp['總股數變動'].apply(format_number)
    df_disp['總持股變動幅度(%)'] = df_disp['總持股變動幅度(%)'].apply(format_pct)
    df_disp['起始總股數'] = df_disp['起始總股數'].apply(format_number)
    df_disp['最新總股數'] = df_disp['最新總股數'].apply(format_number)
    df_disp['出現ETF數'] = df_disp['出現ETF數'].apply(format_number)
    return df_disp.to_html(index=False, classes="styled-table", escape=False)

def render_overall_block(overall, container_id="overall-tabs"):
    html = "<h2>區塊一：總體市場 TOP 5 增減持股</h2>\n"
    if overall.empty:
        return html + "<p class='empty-msg'>目前無任何變動資料</p>\n"
    overall_inc, overall_dec = get_top_changes(overall, top_n=5, sort_by='Total_Shares_Change')
    overall_inc_pct, overall_dec_pct = get_top_changes(overall, top_n=5, sort_by='Total_Shares_Change_Pct')
    html += f"""
    <div class="tab-container" id="{container_id}">
        <div>
            <button class="tab-btn active" onclick="switchTab('{container_id}', 'tab-vol', this)">依股數增減 (量體)</button>
            <button class="tab-btn" onclick="switchTab('{container_id}', 'tab-pct', this)">依變動幅度 (意圖)</button>
        </div>
        <div class="tab-content tab-vol active">
    """
    html += "<h3>[增] 總體資金增持 TOP 5 (量體)</h3>\n"
    html += render_total_table(overall_inc) if not overall_inc.empty else "<p class='empty-msg'>無增持紀錄</p>\n"
    html += "<h3>[減] 總體資金減持 TOP 5 (量體)</h3>\n"
    html += render_total_table(overall_dec) if not overall_dec.empty else "<p class='empty-msg'>無減持紀錄</p>\n"
    html += "</div>\n<div class=\"tab-content tab-pct\">\n"
    html += "<h3>[增] 總體資金增持幅度 TOP 5 (意圖)</h3>\n"
    html += render_total_table(overall_inc_pct) if not overall_inc_pct.empty else "<p class='empty-msg'>無增持紀錄</p>\n"
    html += "<h3>[減] 總體資金減持幅度 TOP 5 (意圖)</h3>\n"
    html += render_total_table(overall_dec_pct) if not overall_dec_pct.empty else "<p class='empty-msg'>無減持紀錄</p>\n"
    html += "</div>\n</div>\n"
    return html

def render_etf_top_table(title, df):
    html = f"<h4>{title}</h4>\n"
    if df.empty:
        return html + "<p class='empty-msg'>無紀錄</p>\n"
    df_disp = df[['Stock_Code', 'Stock_Name', 'Shares_Change', 'Shares_Change_Pct', 'Weight_Change', 'Shares_Today', 'Weight_Today']].copy()
    df_disp.columns = ['股票代碼', '股票名稱', '股數變動', '股數變動幅度', '權重變動(%)', '最新股數', '最新權重(%)']
    df_disp['股數變動'] = df_disp['股數變動'].apply(format_number)
    df_disp['股數變動幅度'] = df_disp['股數變動幅度'].apply(format_pct)
    df_disp['權重變動(%)'] = df_disp['權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
    df_disp['最新股數'] = df_disp['最新股數'].apply(format_number)
    df_disp['最新權重(%)'] = df_disp['最新權重(%)'].apply(lambda x: f"{x:.2f}%")
    return html + df_disp.to_html(index=False, classes="styled-table", escape=False)

def render_etf_pct_table(title, df):
    html = f"<h4>{title}</h4>\n"
    if df.empty:
        return html + "<p class='empty-msg'>無紀錄</p>\n"
    df_disp = df[['Stock_Code', 'Stock_Name', 'Shares_Change', 'Shares_Change_Pct', 'Weight_Change', 'Shares_Today', 'Weight_Today']].copy()
    df_disp.columns = ['股票代碼', '股票名稱', '股數變動', '股數變動幅度', '權重變動(%)', '最新股數', '最新權重(%)']
    df_disp['股數變動'] = df_disp['股數變動'].apply(format_number)
    df_disp['股數變動幅度'] = df_disp['股數變動幅度'].apply(format_pct)
    df_disp['權重變動(%)'] = df_disp['權重變動(%)'].apply(lambda x: f"{x:+.2f}%")
    df_disp['最新股數'] = df_disp['最新股數'].apply(format_number)
    df_disp['最新權重(%)'] = df_disp['最新權重(%)'].apply(lambda x: f"{x:.2f}%")
    return html + df_disp.to_html(index=False, classes="styled-table", escape=False)

def render_etf_blocks(etf_results):
    html = "<h2>區塊二：各檔 ETF 獨立的 TOP 5 增減持股</h2>\n"
    for etf in TARGET_ETFS:
        html += f"<h3>[{etf}] 持股變動排行榜</h3>\n"
        if etf not in etf_results:
            html += f"<p class='empty-msg'>尚無 {etf} 資料</p>\n"
            continue
        html += f"""
        <div class="tab-container" id="etf-tabs-{etf}">
            <div>
                <button class="tab-btn active" onclick="switchTab('etf-tabs-{etf}', 'tab-vol', this)">依股數增減 (量體)</button>
                <button class="tab-btn" onclick="switchTab('etf-tabs-{etf}', 'tab-pct', this)">依變動幅度 (意圖)</button>
            </div>
            <div class="tab-content tab-vol active">
        """
        html += render_etf_top_table("[增] 增持最多 TOP 5 (量體)", etf_results[etf]['inc'])
        html += render_etf_top_table("[減] 減持最多 TOP 5 (量體)", etf_results[etf]['dec'])
        html += "</div>\n<div class=\"tab-content tab-pct\">\n"
        html += render_etf_pct_table("[增] 增持幅度 TOP 5 (意圖)", etf_results[etf]['inc_pct'])
        html += render_etf_pct_table("[減] 減持幅度 TOP 5 (意圖)", etf_results[etf]['dec_pct'])
        html += "</div>\n</div>\n"
    return html

def render_detail_blocks(etf_results):
    html = "<h2>區塊三：各檔 ETF 詳細持股明細</h2>\n"
    for etf in TARGET_ETFS:
        html += "<details style='margin-bottom: 20px; background-color: #ffffff; padding: 10px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.05);'>\n"
        html += f"  <summary style='cursor: pointer; font-size: 1.2em; font-weight: bold; color: #2c3e50; padding: 10px; border-left: 5px solid #16a085; background-color: #eef7f5; border-radius: 0 5px 5px 0; list-style-position: inside;'>[{etf}] 完整持股清單 (點擊展開/收合)</summary>\n"
        html += "  <div style='margin-top: 15px;'>\n"
        if etf not in etf_results:
            html += f"  <p class='empty-msg'>尚無 {etf} 資料</p>\n  </div>\n</details>\n"
            continue
        changes = etf_results[etf]['changes']
        if changes.empty:
            html += "<p class='empty-msg'>無明細資料</p>\n"
        else:
            changes = changes.sort_values(by='Weight_Today', ascending=False)
            df_disp = changes[['Stock_Code', 'Stock_Name', 'Shares_Today', 'Weight_Today', 'Yesterday_Shares_Display', 'Shares_Change_Display', 'Weight_Change_Display']].copy()
            df_disp.columns = ['股票代碼', '股票名稱', '今日股數', '今日權重(%)', '起始股數', '股數變化', '權重變化(%)']
            df_disp['今日股數'] = df_disp['今日股數'].apply(format_number)
            df_disp['今日權重(%)'] = df_disp['今日權重(%)'].apply(lambda x: f"{x:.2f}%")
            html += df_disp.to_html(index=False, classes="styled-table", escape=False)
        html += "  </div>\n</details>\n"
    return html

def render_change_report(title, subtitle, start_date, end_date, depth, active, include_details=False, available_dates=None, extra_controls=""):
    prefix = relative_prefix(depth)
    df_start = read_history(start_date) if start_date else empty_history_df()
    df_end = read_history(end_date)
    is_first_run = not start_date
    overall = build_total_share_changes(df_start, df_end)
    etf_results = build_etf_results(df_start, df_end, is_first_run=is_first_run)
    html = page_head(title)
    html += f"<h1>{title}</h1>\n"
    html += nav_html(prefix, active=active)
    html += extra_controls
    if available_dates is not None:
        html += render_date_select(available_dates, end_date, depth)
    html += f"<div class='date-subtitle'>{subtitle}</div>\n"
    if is_first_run:
        html += "<div class='warning-msg'>此日期沒有前一筆歷史資料，僅建立基準，不計算變動差異。</div>\n"
    html += render_overall_block(overall)
    html += render_etf_blocks(etf_results)
    if include_details:
        html += render_detail_blocks(etf_results)
    html += page_tail()
    return html

def generate_dashboard(target_date_str, prev_date_str, available_dates, is_root=False):
    if not os.path.exists(f"history/history_{target_date_str}.csv"):
        return
    depth = 0 if is_root else 3
    if prev_date_str:
        subtitle = f"比較期間：{format_date(prev_date_str)} vs {format_date(target_date_str)}"
    else:
        subtitle = f"比較期間：無前日資料 vs {format_date(target_date_str)}"
    html_content = render_change_report(
        "主動式 ETF 持股變化追蹤",
        subtitle,
        prev_date_str,
        target_date_str,
        depth=depth,
        active="daily",
        include_details=True,
        available_dates=available_dates
    )
    if is_root:
        out_file = "index.html"
    else:
        out_dir = f"dashboards/{target_date_str[:4]}/{target_date_str[4:6]}"
        os.makedirs(out_dir, exist_ok=True)
        out_file = f"{out_dir}/index_{target_date_str}.html"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[Done] 已產生網頁: {out_file}")

def generate_weekly_pages(available_dates):
    if len(available_dates) < 2:
        return
    weeks = {}
    for d in available_dates:
        iso_year, iso_week, _ = datetime.strptime(d, "%Y%m%d").isocalendar()
        key = f"{iso_year}W{iso_week:02d}"
        weeks.setdefault(key, []).append(d)
    os.makedirs("weekly", exist_ok=True)
    rows = []
    for key in sorted(weeks):
        dates = sorted(weeks[key])
        start_date, end_date = dates[0], dates[-1]
        if start_date == end_date:
            continue
        year = key[:4]
        out_dir = f"weekly/{year}"
        os.makedirs(out_dir, exist_ok=True)
        out_file = f"{out_dir}/index_{key}.html"
        title = f"每週歷史資料 {key}"
        subtitle = f"週區間：{format_date(start_date)} vs {format_date(end_date)}"
        html = render_change_report(title, subtitle, start_date, end_date, depth=2, active="weekly", include_details=False)
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(html)
        rows.append([key, format_date(start_date), format_date(end_date), f"{year}/index_{key}.html"])
    index_html = page_head("每週歷史資料")
    index_html += "<h1>每週歷史資料</h1>\n"
    index_html += nav_html("../", active="weekly")
    index_html += "<div class='date-subtitle'>以每週第一個與最後一個可用交易日計算持股變化</div>\n"
    if rows:
        df = pd.DataFrame(rows, columns=['週別', '起始日期', '結束日期', '連結'])
        df['連結'] = df['連結'].apply(lambda x: f"<a href='{x}'>查看</a>")
        index_html += df.to_html(index=False, classes="styled-table", escape=False)
    else:
        index_html += "<p class='empty-msg'>尚無足夠資料產生週報</p>\n"
    index_html += page_tail()
    with open("weekly/index.html", "w", encoding="utf-8") as f:
        f.write(index_html)
    print("[Done] 已產生每週歷史頁")

def generate_range_page(available_dates):
    os.makedirs("range", exist_ok=True)
    records = []
    for d in available_dates:
        df = read_history(d)
        for row in df[['Stock_Code', 'Stock_Name', 'Shares', 'ETF']].itertuples(index=False):
            records.append({
                'date': d,
                'code': str(row.Stock_Code),
                'name': str(row.Stock_Name),
                'shares': float(row.Shares),
                'etf': str(row.ETF)
            })
    data_json = json.dumps(records, ensure_ascii=False)
    dates_json = json.dumps(available_dates, ensure_ascii=False)
    default_start = available_dates[-2] if len(available_dates) > 1 else available_dates[-1]
    default_end = available_dates[-1]
    html = page_head("自訂區間")
    html += "<h1>自訂區間</h1>\n"
    html += nav_html("../", active="range")
    html += """
<div class="control-panel">
    <label for="startDate">起始日期</label>
    <select id="startDate"></select>
    <label for="endDate">結束日期</label>
    <select id="endDate"></select>
    <button onclick="renderRange()">更新</button>
</div>
<div class="date-subtitle" id="rangeSubtitle"></div>
<div id="rangeOutput"></div>
"""
    html += f"""
<script>
const historyRecords = {data_json};
const availableDates = {dates_json};
const defaultStart = "{default_start}";
const defaultEnd = "{default_end}";

function fmtDate(d) {{
    return d.slice(0, 4) + "-" + d.slice(4, 6) + "-" + d.slice(6, 8);
}}
function fmtNumber(v) {{
    return Math.round(v).toLocaleString("en-US");
}}
function fmtPct(v) {{
    if (v === Infinity) return "新建倉";
    if (v === -100) return "清倉";
    return (v >= 0 ? "+" : "") + v.toFixed(2) + "%";
}}
function aggregate(date) {{
    const map = new Map();
    historyRecords.filter(r => r.date === date).forEach(r => {{
        const key = r.code + "||" + r.name;
        if (!map.has(key)) map.set(key, {{ code: r.code, name: r.name, shares: 0, etfs: new Set() }});
        const item = map.get(key);
        item.shares += Number(r.shares || 0);
        item.etfs.add(r.etf);
    }});
    return map;
}}
function calcPct(startShares, endShares) {{
    if (startShares === 0 && endShares > 0) return Infinity;
    if (startShares > 0 && endShares === 0) return -100;
    if (startShares === 0 && endShares === 0) return 0;
    return (endShares - startShares) / startShares * 100;
}}
function buildRows(startDate, endDate) {{
    const start = aggregate(startDate);
    const end = aggregate(endDate);
    const keys = new Set([...start.keys(), ...end.keys()]);
    return Array.from(keys).map(key => {{
        const s = start.get(key) || {{ code: key.split("||")[0], name: key.split("||")[1], shares: 0, etfs: new Set() }};
        const e = end.get(key) || {{ code: key.split("||")[0], name: key.split("||")[1], shares: 0, etfs: new Set() }};
        const etfs = new Set([...s.etfs, ...e.etfs]);
        const change = e.shares - s.shares;
        return {{ code: e.code || s.code, name: e.name || s.name, change, pct: calcPct(s.shares, e.shares), startShares: s.shares, endShares: e.shares, etfCount: etfs.size }};
    }}).filter(r => r.change !== 0);
}}
function tableHtml(title, rows) {{
    if (!rows.length) return `<h3>${{title}}</h3><p class='empty-msg'>無資料</p>`;
    const body = rows.map(r => `<tr><td>${{r.code}}</td><td>${{r.name}}</td><td>${{fmtNumber(r.change)}}</td><td>${{fmtPct(r.pct)}}</td><td>${{fmtNumber(r.startShares)}}</td><td>${{fmtNumber(r.endShares)}}</td><td>${{r.etfCount}}</td></tr>`).join("");
    return `<h3>${{title}}</h3><table class="styled-table"><thead><tr><th>股票代碼</th><th>股票名稱</th><th>總股數變動</th><th>總持股變動幅度(%)</th><th>起始總股數</th><th>最新總股數</th><th>出現ETF數</th></tr></thead><tbody>${{body}}</tbody></table>`;
}}
function renderRange() {{
    const startDate = document.getElementById("startDate").value;
    const endDate = document.getElementById("endDate").value;
    const rows = buildRows(startDate, endDate);
    const inc = rows.filter(r => r.change > 0).sort((a, b) => b.change - a.change).slice(0, 20);
    const dec = rows.filter(r => r.change < 0).sort((a, b) => a.change - b.change).slice(0, 20);
    document.getElementById("rangeSubtitle").textContent = "比較期間：" + fmtDate(startDate) + " vs " + fmtDate(endDate);
    document.getElementById("rangeOutput").innerHTML = tableHtml("[增] 區間增持 TOP 20", inc) + tableHtml("[減] 區間減持 TOP 20", dec);
}}
function initSelectors() {{
    ["startDate", "endDate"].forEach(id => {{
        const select = document.getElementById(id);
        availableDates.forEach(d => {{
            const opt = document.createElement("option");
            opt.value = d;
            opt.textContent = fmtDate(d);
            select.appendChild(opt);
        }});
    }});
    document.getElementById("startDate").value = defaultStart;
    document.getElementById("endDate").value = defaultEnd;
    renderRange();
}}
initSelectors();
</script>
"""
    html += page_tail()
    with open("range/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[Done] 已產生自訂區間頁")

def generate_weekly_pages(available_dates):
    if len(available_dates) < 2:
        return
    weeks = {}
    for d in available_dates:
        iso_year, iso_week, _ = datetime.strptime(d, "%Y%m%d").isocalendar()
        key = f"{iso_year}W{iso_week:02d}"
        weeks.setdefault(key, []).append(d)
    week_rows = []
    for key in sorted(weeks):
        dates = sorted(weeks[key])
        start_date, end_date = dates[0], dates[-1]
        year = key[:4]
        week_rows.append({
            'key': key,
            'start': start_date,
            'end': end_date,
            'href': f"weekly/{year}/index_{key}.html"
        })
    if not week_rows:
        return
    os.makedirs("weekly", exist_ok=True)
    for row in week_rows:
        key = row['key']
        start_date = row['start']
        end_date = row['end']
        out_dir = f"weekly/{key[:4]}"
        os.makedirs(out_dir, exist_ok=True)
        html = render_change_report(
            "每週歷史資料",
            f"週別：{key}｜區間：{format_date(start_date)} vs {format_date(end_date)}",
            start_date,
            end_date,
            depth=2,
            active="weekly",
            include_details=False,
            extra_controls=render_week_select(week_rows, key, depth=2)
        )
        with open(f"{out_dir}/index_{key}.html", "w", encoding="utf-8") as f:
            f.write(html)
    latest = week_rows[-1]
    latest_html = render_change_report(
        "每週歷史資料",
        f"週別：{latest['key']}｜區間：{format_date(latest['start'])} vs {format_date(latest['end'])}",
        latest['start'],
        latest['end'],
        depth=1,
        active="weekly",
        include_details=False,
        extra_controls=render_week_select(week_rows, latest['key'], depth=1)
    )
    with open("weekly/index.html", "w", encoding="utf-8") as f:
        f.write(latest_html)
    print("[Done] 已產生每週歷史頁")

def generate_range_page(available_dates):
    os.makedirs("range", exist_ok=True)
    records = []
    for d in available_dates:
        df = read_history(d)
        for row in df[['Stock_Code', 'Stock_Name', 'Weight', 'Shares', 'ETF']].itertuples(index=False):
            records.append({
                'date': d,
                'code': str(row.Stock_Code),
                'name': str(row.Stock_Name),
                'weight': float(row.Weight),
                'shares': float(row.Shares),
                'etf': str(row.ETF)
            })
    default_start = available_dates[-2] if len(available_dates) > 1 else available_dates[-1]
    default_end = available_dates[-1]
    html = page_head("自訂區間")
    html += "<h1>自訂區間</h1>\n"
    html += nav_html("../", active="range")
    html += """
<div class="control-panel">
    <label for="startDate">起始日期</label>
    <select id="startDate"></select>
    <label for="endDate">結束日期</label>
    <select id="endDate"></select>
    <button onclick="renderRange()">更新</button>
</div>
<div class="date-subtitle" id="rangeSubtitle"></div>
<div id="rangeOutput"></div>
"""
    script = r"""
<script>
const historyRecords = __DATA__;
const availableDates = __DATES__;
const targetEtfs = __ETFS__;
const defaultStart = "__START__";
const defaultEnd = "__END__";

function fmtDate(d) {
    return d.slice(0, 4) + "-" + d.slice(4, 6) + "-" + d.slice(6, 8);
}
function fmtNumber(v) {
    return Math.round(Number(v || 0)).toLocaleString("en-US");
}
function fmtSignedNumber(v) {
    const n = Math.round(Number(v || 0));
    if (n === 0) return "0";
    return (n > 0 ? "+" : "") + n.toLocaleString("en-US");
}
function fmtPct(v) {
    if (v === Infinity) return "新建倉";
    if (v === -100) return "清倉";
    return (v >= 0 ? "+" : "") + Number(v || 0).toFixed(2) + "%";
}
function pct(startShares, endShares) {
    if (startShares === 0 && endShares > 0) return Infinity;
    if (startShares > 0 && endShares === 0) return -100;
    if (startShares === 0 && endShares === 0) return 0;
    return (endShares - startShares) / startShares * 100;
}
function recordsByDate(date, etf) {
    return historyRecords.filter(r => r.date === date && (!etf || r.etf === etf));
}
function aggregate(date, etf) {
    const map = new Map();
    recordsByDate(date, etf).forEach(r => {
        const key = r.code + "||" + r.name;
        if (!map.has(key)) map.set(key, { code: r.code, name: r.name, shares: 0, weight: 0, etfs: new Set() });
        const item = map.get(key);
        item.shares += Number(r.shares || 0);
        item.weight += Number(r.weight || 0);
        item.etfs.add(r.etf);
    });
    return map;
}
function buildOverallRows(startDate, endDate) {
    const start = aggregate(startDate);
    const end = aggregate(endDate);
    const keys = new Set([...start.keys(), ...end.keys()]);
    return Array.from(keys).map(key => {
        const parts = key.split("||");
        const s = start.get(key) || { code: parts[0], name: parts[1], shares: 0, etfs: new Set() };
        const e = end.get(key) || { code: parts[0], name: parts[1], shares: 0, etfs: new Set() };
        const etfs = new Set([...s.etfs, ...e.etfs]);
        const change = e.shares - s.shares;
        return { code: e.code || s.code, name: e.name || s.name, change, pct: pct(s.shares, e.shares), startShares: s.shares, endShares: e.shares, etfCount: etfs.size };
    }).filter(r => r.change !== 0);
}
function buildEtfRows(startDate, endDate, etf) {
    const start = aggregate(startDate, etf);
    const end = aggregate(endDate, etf);
    const keys = new Set([...start.keys(), ...end.keys()]);
    return Array.from(keys).map(key => {
        const parts = key.split("||");
        const s = start.get(key) || { code: parts[0], name: parts[1], shares: 0, weight: 0 };
        const e = end.get(key) || { code: parts[0], name: parts[1], shares: 0, weight: 0 };
        const change = e.shares - s.shares;
        const weightChange = e.weight - s.weight;
        return { code: e.code || s.code, name: e.name || s.name, change, pct: pct(s.shares, e.shares), startShares: s.shares, endShares: e.shares, weightChange, endWeight: e.weight };
    }).filter(r => r.change !== 0);
}
function overallTable(title, rows) {
    if (!rows.length) return `<h3>${title}</h3><p class='empty-msg'>無資料</p>`;
    const body = rows.map(r => `<tr><td>${r.code}</td><td>${r.name}</td><td>${fmtNumber(r.change)}</td><td>${fmtPct(r.pct)}</td><td>${fmtNumber(r.startShares)}</td><td>${fmtNumber(r.endShares)}</td><td>${r.etfCount}</td></tr>`).join("");
    return `<h3>${title}</h3><table class="styled-table"><thead><tr><th>股票代碼</th><th>股票名稱</th><th>總股數變動</th><th>總持股變動幅度(%)</th><th>起始總股數</th><th>最新總股數</th><th>出現ETF數</th></tr></thead><tbody>${body}</tbody></table>`;
}
function etfVolumeTable(title, rows) {
    if (!rows.length) return `<h4>${title}</h4><p class='empty-msg'>無紀錄</p>`;
    const body = rows.map(r => `<tr><td>${r.code}</td><td>${r.name}</td><td>${fmtSignedNumber(r.change)}</td><td>${fmtPct(r.pct)}</td><td>${fmtPct(r.weightChange)}</td><td>${fmtNumber(r.endShares)}</td><td>${Number(r.endWeight || 0).toFixed(2)}%</td></tr>`).join("");
    return `<h4>${title}</h4><table class="styled-table"><thead><tr><th>股票代碼</th><th>股票名稱</th><th>股數變動</th><th>股數變動幅度</th><th>權重變動(%)</th><th>最新股數</th><th>最新權重(%)</th></tr></thead><tbody>${body}</tbody></table>`;
}
function etfPctTable(title, rows) {
    if (!rows.length) return `<h4>${title}</h4><p class='empty-msg'>無紀錄</p>`;
    const body = rows.map(r => `<tr><td>${r.code}</td><td>${r.name}</td><td>${fmtSignedNumber(r.change)}</td><td>${fmtPct(r.pct)}</td><td>${fmtPct(r.weightChange)}</td><td>${fmtNumber(r.endShares)}</td><td>${Number(r.endWeight || 0).toFixed(2)}%</td></tr>`).join("");
    return `<h4>${title}</h4><table class="styled-table"><thead><tr><th>股票代碼</th><th>股票名稱</th><th>股數變動</th><th>股數變動幅度</th><th>權重變動(%)</th><th>最新股數</th><th>最新權重(%)</th></tr></thead><tbody>${body}</tbody></table>`;
}
function renderOverall(rows) {
    const inc = rows.filter(r => r.change > 0).sort((a, b) => b.change - a.change).slice(0, 5);
    const dec = rows.filter(r => r.change < 0).sort((a, b) => a.change - b.change).slice(0, 5);
    const incPct = rows.filter(r => r.pct > 0).sort((a, b) => b.pct - a.pct).slice(0, 5);
    const decPct = rows.filter(r => r.pct < 0).sort((a, b) => a.pct - b.pct).slice(0, 5);
    return `<h2>區塊一：總體市場 TOP 5 增減持股</h2>
    <div class="tab-container" id="overall-tabs">
      <div><button class="tab-btn active" onclick="switchTab('overall-tabs', 'tab-vol', this)">依股數增減 (量體)</button><button class="tab-btn" onclick="switchTab('overall-tabs', 'tab-pct', this)">依變動幅度 (意圖)</button></div>
      <div class="tab-content tab-vol active">${overallTable("[增] 總體資金增持 TOP 5 (量體)", inc)}${overallTable("[減] 總體資金減持 TOP 5 (量體)", dec)}</div>
      <div class="tab-content tab-pct">${overallTable("[增] 總體資金增持幅度 TOP 5 (意圖)", incPct)}${overallTable("[減] 總體資金減持幅度 TOP 5 (意圖)", decPct)}</div>
    </div>`;
}
function renderEtfBlocks(startDate, endDate) {
    let html = "<h2>區塊二：各檔 ETF 獨立的 TOP 5 增減持股</h2>";
    targetEtfs.forEach(etf => {
        const rows = buildEtfRows(startDate, endDate, etf);
        const inc = rows.filter(r => r.change > 0).sort((a, b) => b.change - a.change).slice(0, 5);
        const dec = rows.filter(r => r.change < 0).sort((a, b) => a.change - b.change).slice(0, 5);
        const incPct = rows.filter(r => r.pct > 0).sort((a, b) => b.pct - a.pct).slice(0, 5);
        const decPct = rows.filter(r => r.pct < 0).sort((a, b) => a.pct - b.pct).slice(0, 5);
        html += `<h3>[${etf}] 持股變動排行榜</h3><div class="tab-container" id="range-etf-${etf}">
          <div><button class="tab-btn active" onclick="switchTab('range-etf-${etf}', 'tab-vol', this)">依股數增減 (量體)</button><button class="tab-btn" onclick="switchTab('range-etf-${etf}', 'tab-pct', this)">依變動幅度 (意圖)</button></div>
          <div class="tab-content tab-vol active">${etfVolumeTable("[增] 增持最多 TOP 5 (量體)", inc)}${etfVolumeTable("[減] 減持最多 TOP 5 (量體)", dec)}</div>
          <div class="tab-content tab-pct">${etfPctTable("[增] 增持幅度 TOP 5 (意圖)", incPct)}${etfPctTable("[減] 減持幅度 TOP 5 (意圖)", decPct)}</div>
        </div>`;
    });
    return html;
}
function renderRange() {
    const startDate = document.getElementById("startDate").value;
    const endDate = document.getElementById("endDate").value;
    document.getElementById("rangeSubtitle").textContent = "比較期間：" + fmtDate(startDate) + " vs " + fmtDate(endDate);
    const overall = buildOverallRows(startDate, endDate);
    document.getElementById("rangeOutput").innerHTML = renderOverall(overall) + renderEtfBlocks(startDate, endDate);
}
function initSelectors() {
    ["startDate", "endDate"].forEach(id => {
        const select = document.getElementById(id);
        availableDates.forEach(d => {
            const opt = document.createElement("option");
            opt.value = d;
            opt.textContent = fmtDate(d);
            select.appendChild(opt);
        });
    });
    document.getElementById("startDate").value = defaultStart;
    document.getElementById("endDate").value = defaultEnd;
    renderRange();
}
initSelectors();
</script>
"""
    script = script.replace("__DATA__", json.dumps(records, ensure_ascii=False))
    script = script.replace("__DATES__", json.dumps(available_dates, ensure_ascii=False))
    script = script.replace("__ETFS__", json.dumps(TARGET_ETFS, ensure_ascii=False))
    script = script.replace("__START__", default_start)
    script = script.replace("__END__", default_end)
    html += script
    html += page_tail()
    with open("range/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[Done] 已產生自訂區間頁")

def parse_money_number(value):
    if pd.isna(value):
        return 0.0
    text = str(value).replace(",", "").replace("%", "").strip()
    match = __import__("re").search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group(0)) if match else 0.0

def fetch_etf_meta(etf_code, today_str):
    target_dir = os.path.join("data", etf_code)
    files = glob.glob(os.path.join(target_dir, f"{today_str}.*"))
    if not files:
        return None
    file_path = files[0]
    rows = []
    try:
        if file_path.endswith(".csv"):
            try:
                df_raw = pd.read_csv(file_path, encoding="utf-8-sig", header=None, names=range(8), engine="python")
            except Exception:
                df_raw = pd.read_csv(file_path, encoding="cp950", header=None, names=range(8), engine="python")
            rows = df_raw.head(40).values.tolist()
        else:
            engine_type = "openpyxl" if file_path.endswith(".xlsx") else "xlrd"
            excel = pd.ExcelFile(file_path, engine=engine_type)
            for sheet_name in excel.sheet_names[:2]:
                df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine=engine_type)
                rows.extend(df_raw.head(40).values.tolist())
    except Exception as e:
        print(f"[Warning] 無法解析 {etf_code} 基金規模資訊: {e}")
        return None

    net_asset = 0.0
    nav = 0.0
    units = 0.0
    data_date = ""
    for i, row in enumerate(rows):
        cells = ["" if pd.isna(x) else str(x).strip() for x in row]
        joined = " ".join(cells)
        if not data_date and ("資料日期" in joined or "日期:" in joined or "/" in joined):
            match = __import__("re").search(r"(\d{4}/\d{2}/\d{2}|\d{3}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2})", joined)
            if match:
                data_date = match.group(1)
        label = cells[0]
        value = next((c for c in cells[1:] if c), "")
        if "基金資產淨值" in label and not value and i + 1 < len(rows):
            value = str(rows[i + 1][0])
        if "基金在外流通單位數" in label and not value and i + 1 < len(rows):
            value = str(rows[i + 1][0])
        if "基金每單位淨值" in label and not value and i + 1 < len(rows):
            value = str(rows[i + 1][0])
        if ("淨資產" in label or "資產總淨值" in label or "基金淨資產價值" in label) and "每" not in label:
            net_asset = parse_money_number(value)
        elif "每單位淨值" in label or "每受益權單位淨資產價值" in label:
            nav = parse_money_number(value)
        elif "流通在外單位數" in label or "在外流通單位數" in label or "已發行受益權單位總數" in label:
            units = parse_money_number(value)

    if not net_asset and nav and units:
        net_asset = nav * units
    if not (net_asset or nav or units):
        return None
    return {
        "ETF": etf_code,
        "File_Date": today_str,
        "Data_Date": data_date,
        "Net_Asset": net_asset,
        "NAV": nav,
        "Units": units,
    }

def empty_fund_meta_df():
    return pd.DataFrame(columns=["ETF", "File_Date", "Data_Date", "Net_Asset", "NAV", "Units"])

def read_fund_meta(date_str):
    path = f"history/fund_meta_{date_str}.csv"
    if not date_str or not os.path.exists(path):
        return empty_fund_meta_df()
    df = pd.read_csv(path, dtype={"ETF": str, "File_Date": str, "Data_Date": str})
    for col in ["Net_Asset", "NAV", "Units"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def rebuild_fund_meta_history(available_dates):
    os.makedirs("history", exist_ok=True)
    for d in available_dates:
        meta_rows = []
        for etf in TARGET_ETFS:
            meta = fetch_etf_meta(etf, d)
            if meta:
                meta_rows.append(meta)
        if meta_rows:
            pd.DataFrame(meta_rows).to_csv(f"history/fund_meta_{d}.csv", index=False, encoding="utf-8-sig")

def add_position_value(df, meta):
    df = df.copy()
    if df.empty:
        df["Position_Value"] = []
        return df
    if meta.empty:
        df["Net_Asset"] = 0.0
    else:
        df = df.merge(meta[["ETF", "Net_Asset"]], on="ETF", how="left")
        df["Net_Asset"] = pd.to_numeric(df["Net_Asset"], errors="coerce").fillna(0)
    df["Position_Value"] = df["Net_Asset"] * pd.to_numeric(df["Weight"], errors="coerce").fillna(0) / 100.0
    return df

def build_total_share_changes(df_start, df_end, start_date=None, end_date=None):
    start_meta = read_fund_meta(start_date) if start_date else empty_fund_meta_df()
    end_meta = read_fund_meta(end_date) if end_date else empty_fund_meta_df()
    start_total_asset = pd.to_numeric(start_meta["Net_Asset"], errors="coerce").fillna(0).sum() if not start_meta.empty else 0.0
    end_total_asset = pd.to_numeric(end_meta["Net_Asset"], errors="coerce").fillna(0).sum() if not end_meta.empty else 0.0
    df_start = add_position_value(df_start, start_meta)
    df_end = add_position_value(df_end, end_meta)
    key_cols = ["Stock_Code", "Stock_Name"]
    if df_start.empty:
        start = pd.DataFrame(columns=key_cols + ["Start_Shares", "Start_Position_Value", "Start_ETF_Count"])
    else:
        start = df_start.groupby(key_cols, dropna=False).agg(
            Start_Shares=("Shares", "sum"),
            Start_Position_Value=("Position_Value", "sum"),
            Start_ETF_Count=("ETF", "nunique")
        ).reset_index()
    if df_end.empty:
        end = pd.DataFrame(columns=key_cols + ["End_Shares", "End_Position_Value", "End_ETF_Count"])
    else:
        end = df_end.groupby(key_cols, dropna=False).agg(
            End_Shares=("Shares", "sum"),
            End_Position_Value=("Position_Value", "sum"),
            End_ETF_Count=("ETF", "nunique")
        ).reset_index()
    overall = pd.merge(end, start, on=key_cols, how="outer")
    for col in ["Start_Shares", "End_Shares", "Start_Position_Value", "End_Position_Value", "Start_ETF_Count", "End_ETF_Count"]:
        overall[col] = pd.to_numeric(overall[col], errors="coerce").fillna(0)
    overall["Total_Shares_Change"] = overall["End_Shares"] - overall["Start_Shares"]
    overall["Total_Position_Change"] = overall["End_Position_Value"] - overall["Start_Position_Value"]
    overall["Start_Position_Weight"] = overall["Start_Position_Value"] / start_total_asset * 100.0 if start_total_asset else 0.0
    overall["End_Position_Weight"] = overall["End_Position_Value"] / end_total_asset * 100.0 if end_total_asset else 0.0
    overall["Total_Position_Weight_Change"] = overall["End_Position_Weight"] - overall["Start_Position_Weight"]
    overall["ETF_Count"] = overall[["Start_ETF_Count", "End_ETF_Count"]].max(axis=1).astype(int)
    return overall

def render_total_table(df):
    if df.empty:
        return "<p class='empty-msg'>無持股紀錄</p>\n"
    df_disp = df[["Stock_Code", "Stock_Name", "Total_Shares_Change", "Total_Position_Change", "Total_Position_Weight_Change", "End_Position_Weight", "Start_Position_Value", "End_Position_Value", "ETF_Count"]].copy()
    df_disp.columns = ["股票代碼", "股票名稱", "總股數變動", "總資金部位變動", "總資金占比變動(百分點)", "最新總資金占比(%)", "起始總資金部位", "最新總資金部位", "出現ETF數"]
    for col in ["總股數變動", "總資金部位變動", "起始總資金部位", "最新總資金部位", "出現ETF數"]:
        df_disp[col] = df_disp[col].apply(format_number)
    df_disp["總資金占比變動(百分點)"] = df_disp["總資金占比變動(百分點)"].apply(format_pct)
    df_disp["最新總資金占比(%)"] = df_disp["最新總資金占比(%)"].apply(lambda x: f"{float(x):.2f}%")
    return df_disp.to_html(index=False, classes="styled-table", escape=False)

def render_overall_block(overall, container_id="overall-tabs"):
    html = "<h2>區塊一：總體市場 TOP 5 增減持股</h2>\n"
    if overall.empty:
        return html + "<p class='empty-msg'>沒有可比較的資料</p>\n"
    overall_inc, overall_dec = get_top_changes(overall, top_n=5, sort_by="Total_Shares_Change")
    overall_inc_pct, overall_dec_pct = get_top_changes(overall, top_n=5, sort_by="Total_Position_Weight_Change")
    html += f"""
    <div class="tab-container" id="{container_id}">
        <div>
            <button class="tab-btn active" onclick="switchTab('{container_id}', 'tab-vol', this)">依股數增減 (量體)</button>
            <button class="tab-btn" onclick="switchTab('{container_id}', 'tab-pct', this)">依總資金占比變動 (意圖)</button>
        </div>
        <div class="tab-content tab-vol active">
    """
    html += "<h3>[增] 總體股數增持 TOP 5 (量體)</h3>\n"
    html += render_total_table(overall_inc) if not overall_inc.empty else "<p class='empty-msg'>無增持紀錄</p>\n"
    html += "<h3>[減] 總體股數減持 TOP 5 (量體)</h3>\n"
    html += render_total_table(overall_dec) if not overall_dec.empty else "<p class='empty-msg'>無減持紀錄</p>\n"
    html += "</div>\n<div class=\"tab-content tab-pct\">\n"
    html += "<h3>[增] 總資金占比增加 TOP 5 (意圖)</h3>\n"
    html += render_total_table(overall_inc_pct) if not overall_inc_pct.empty else "<p class='empty-msg'>無增持紀錄</p>\n"
    html += "<h3>[減] 總資金占比減少 TOP 5 (意圖)</h3>\n"
    html += render_total_table(overall_dec_pct) if not overall_dec_pct.empty else "<p class='empty-msg'>無減持紀錄</p>\n"
    html += "</div>\n</div>\n"
    return html

def render_change_report(title, subtitle, start_date, end_date, depth, active, include_details=False, available_dates=None, extra_controls=""):
    prefix = relative_prefix(depth)
    df_start = read_history(start_date) if start_date else empty_history_df()
    df_end = read_history(end_date)
    is_first_run = not start_date
    overall = build_total_share_changes(df_start, df_end, start_date=start_date, end_date=end_date)
    etf_results = build_etf_results(df_start, df_end, is_first_run=is_first_run)
    html = page_head(title)
    html += f"<h1>{title}</h1>\n"
    html += nav_html(prefix, active=active)
    html += extra_controls
    if available_dates is not None:
        html += render_date_select(available_dates, end_date, depth)
    html += f"<div class='date-subtitle'>{subtitle}</div>\n"
    if is_first_run:
        html += "<div class='warning-msg'>第一筆資料只能建立基準，尚無前期可比較。</div>\n"
    html += render_overall_block(overall)
    html += render_etf_blocks(etf_results)
    if include_details:
        html += render_detail_blocks(etf_results)
    html += page_tail()
    return html

def generate_range_page(available_dates):
    os.makedirs("range", exist_ok=True)
    records = []
    meta_by_date = {}
    fund_totals = {}
    for d in available_dates:
        meta_by_date[d] = {
            row.ETF: float(row.Net_Asset)
            for row in read_fund_meta(d)[["ETF", "Net_Asset"]].itertuples(index=False)
        }
        fund_totals[d] = sum(meta_by_date[d].values())
        df = read_history(d)
        for row in df[["Stock_Code", "Stock_Name", "Weight", "Shares", "ETF"]].itertuples(index=False):
            net_asset = meta_by_date[d].get(str(row.ETF), 0.0)
            records.append({
                "date": d,
                "code": str(row.Stock_Code),
                "name": str(row.Stock_Name),
                "weight": float(row.Weight),
                "shares": float(row.Shares),
                "position": net_asset * float(row.Weight) / 100.0,
                "etf": str(row.ETF)
            })
    default_start = available_dates[-2] if len(available_dates) > 1 else available_dates[-1]
    default_end = available_dates[-1]
    html = page_head("自訂區間")
    html += "<h1>自訂區間</h1>\n"
    html += nav_html("../", active="range")
    html += """
<div class="control-panel">
    <label for="startDate">起始日期</label>
    <select id="startDate"></select>
    <label for="endDate">結束日期</label>
    <select id="endDate"></select>
    <button onclick="renderRange()">產生</button>
</div>
<div class="date-subtitle" id="rangeSubtitle"></div>
<div id="rangeOutput"></div>
"""
    script = r"""
<script>
const historyRecords = __DATA__;
const availableDates = __DATES__;
const targetEtfs = __ETFS__;
const fundTotals = __FUND_TOTALS__;
const defaultStart = "__START__";
const defaultEnd = "__END__";

function fmtDate(d) { return d.slice(0, 4) + "-" + d.slice(4, 6) + "-" + d.slice(6, 8); }
function fmtNumber(v) { return Math.round(Number(v || 0)).toLocaleString("en-US"); }
function fmtSignedNumber(v) {
    const n = Math.round(Number(v || 0));
    if (n === 0) return "0";
    return (n > 0 ? "+" : "") + n.toLocaleString("en-US");
}
function fmtPct(v) {
    if (v === Infinity) return "新建倉";
    if (v === -100) return "清倉";
    return (v >= 0 ? "+" : "") + Number(v || 0).toFixed(2) + "%";
}
function pct(startValue, endValue) {
    if (startValue === 0 && endValue > 0) return Infinity;
    if (startValue > 0 && endValue === 0) return -100;
    if (startValue === 0 && endValue === 0) return 0;
    return (endValue - startValue) / startValue * 100;
}
function recordsByDate(date, etf) {
    return historyRecords.filter(r => r.date === date && (!etf || r.etf === etf));
}
function aggregate(date, etf) {
    const map = new Map();
    recordsByDate(date, etf).forEach(r => {
        const key = r.code + "||" + r.name;
        if (!map.has(key)) map.set(key, { code: r.code, name: r.name, shares: 0, weight: 0, position: 0, etfs: new Set() });
        const item = map.get(key);
        item.shares += Number(r.shares || 0);
        item.weight += Number(r.weight || 0);
        item.position += Number(r.position || 0);
        item.etfs.add(r.etf);
    });
    return map;
}
function buildOverallRows(startDate, endDate) {
    const start = aggregate(startDate);
    const end = aggregate(endDate);
    const startTotal = Number(fundTotals[startDate] || 0);
    const endTotal = Number(fundTotals[endDate] || 0);
    const keys = new Set([...start.keys(), ...end.keys()]);
    return Array.from(keys).map(key => {
        const parts = key.split("||");
        const s = start.get(key) || { code: parts[0], name: parts[1], shares: 0, position: 0, etfs: new Set() };
        const e = end.get(key) || { code: parts[0], name: parts[1], shares: 0, position: 0, etfs: new Set() };
        const etfs = new Set([...s.etfs, ...e.etfs]);
        const startWeight = startTotal ? s.position / startTotal * 100 : 0;
        const endWeight = endTotal ? e.position / endTotal * 100 : 0;
        return {
            code: e.code || s.code,
            name: e.name || s.name,
            change: e.shares - s.shares,
            positionChange: e.position - s.position,
            pct: endWeight - startWeight,
            endWeight,
            startPosition: s.position,
            endPosition: e.position,
            etfCount: etfs.size
        };
    }).filter(r => r.change !== 0 || r.positionChange !== 0);
}
function buildEtfRows(startDate, endDate, etf) {
    const start = aggregate(startDate, etf);
    const end = aggregate(endDate, etf);
    const keys = new Set([...start.keys(), ...end.keys()]);
    return Array.from(keys).map(key => {
        const parts = key.split("||");
        const s = start.get(key) || { code: parts[0], name: parts[1], shares: 0, weight: 0 };
        const e = end.get(key) || { code: parts[0], name: parts[1], shares: 0, weight: 0 };
        const change = e.shares - s.shares;
        const weightChange = e.weight - s.weight;
        return { code: e.code || s.code, name: e.name || s.name, change, pct: pct(s.shares, e.shares), startShares: s.shares, endShares: e.shares, weightChange, endWeight: e.weight };
    }).filter(r => r.change !== 0);
}
function overallTable(title, rows) {
    if (!rows.length) return `<h3>${title}</h3><p class='empty-msg'>無持股紀錄</p>`;
    const body = rows.map(r => `<tr><td>${r.code}</td><td>${r.name}</td><td>${fmtSignedNumber(r.change)}</td><td>${fmtSignedNumber(r.positionChange)}</td><td>${fmtPct(r.pct)}</td><td>${Number(r.endWeight || 0).toFixed(2)}%</td><td>${fmtNumber(r.startPosition)}</td><td>${fmtNumber(r.endPosition)}</td><td>${r.etfCount}</td></tr>`).join("");
    return `<h3>${title}</h3><table class="styled-table"><thead><tr><th>股票代碼</th><th>股票名稱</th><th>總股數變動</th><th>總資金部位變動</th><th>總資金占比變動(百分點)</th><th>最新總資金占比(%)</th><th>起始總資金部位</th><th>最新總資金部位</th><th>出現ETF數</th></tr></thead><tbody>${body}</tbody></table>`;
}
function etfVolumeTable(title, rows) {
    if (!rows.length) return `<h4>${title}</h4><p class='empty-msg'>無持股紀錄</p>`;
    const body = rows.map(r => `<tr><td>${r.code}</td><td>${r.name}</td><td>${fmtSignedNumber(r.change)}</td><td>${fmtPct(r.pct)}</td><td>${fmtPct(r.weightChange)}</td><td>${fmtNumber(r.endShares)}</td><td>${Number(r.endWeight || 0).toFixed(2)}%</td></tr>`).join("");
    return `<h4>${title}</h4><table class="styled-table"><thead><tr><th>股票代碼</th><th>股票名稱</th><th>股數變動</th><th>股數變動幅度</th><th>權重變動(%)</th><th>最新股數</th><th>最新權重(%)</th></tr></thead><tbody>${body}</tbody></table>`;
}
function etfPctTable(title, rows) {
    if (!rows.length) return `<h4>${title}</h4><p class='empty-msg'>無持股紀錄</p>`;
    const body = rows.map(r => `<tr><td>${r.code}</td><td>${r.name}</td><td>${fmtSignedNumber(r.change)}</td><td>${fmtPct(r.pct)}</td><td>${fmtPct(r.weightChange)}</td><td>${fmtNumber(r.endShares)}</td><td>${Number(r.endWeight || 0).toFixed(2)}%</td></tr>`).join("");
    return `<h4>${title}</h4><table class="styled-table"><thead><tr><th>股票代碼</th><th>股票名稱</th><th>股數變動</th><th>股數變動幅度</th><th>權重變動(%)</th><th>最新股數</th><th>最新權重(%)</th></tr></thead><tbody>${body}</tbody></table>`;
}
function renderOverall(rows) {
    const inc = rows.filter(r => r.change > 0).sort((a, b) => b.change - a.change).slice(0, 5);
    const dec = rows.filter(r => r.change < 0).sort((a, b) => a.change - b.change).slice(0, 5);
    const incPct = rows.filter(r => r.pct > 0).sort((a, b) => b.pct - a.pct).slice(0, 5);
    const decPct = rows.filter(r => r.pct < 0).sort((a, b) => a.pct - b.pct).slice(0, 5);
    return `<h2>區塊一：總體市場 TOP 5 增減持股</h2>
    <div class="tab-container" id="overall-tabs">
      <div><button class="tab-btn active" onclick="switchTab('overall-tabs', 'tab-vol', this)">依股數增減 (量體)</button><button class="tab-btn" onclick="switchTab('overall-tabs', 'tab-pct', this)">依總資金占比變動 (意圖)</button></div>
      <div class="tab-content tab-vol active">${overallTable("[增] 總體股數增持 TOP 5 (量體)", inc)}${overallTable("[減] 總體股數減持 TOP 5 (量體)", dec)}</div>
      <div class="tab-content tab-pct">${overallTable("[增] 總資金占比增加 TOP 5 (意圖)", incPct)}${overallTable("[減] 總資金占比減少 TOP 5 (意圖)", decPct)}</div>
    </div>`;
}
function renderEtfBlocks(startDate, endDate) {
    let html = "<h2>區塊二：各檔 ETF 獨立的 TOP 5 增減持股</h2>";
    targetEtfs.forEach(etf => {
        const rows = buildEtfRows(startDate, endDate, etf);
        const inc = rows.filter(r => r.change > 0).sort((a, b) => b.change - a.change).slice(0, 5);
        const dec = rows.filter(r => r.change < 0).sort((a, b) => a.change - b.change).slice(0, 5);
        const incPct = rows.filter(r => r.pct > 0).sort((a, b) => b.pct - a.pct).slice(0, 5);
        const decPct = rows.filter(r => r.pct < 0).sort((a, b) => a.pct - b.pct).slice(0, 5);
        html += `<h3>[${etf}] 持股變動排行榜</h3><div class="tab-container" id="range-etf-${etf}">
          <div><button class="tab-btn active" onclick="switchTab('range-etf-${etf}', 'tab-vol', this)">依股數增減 (量體)</button><button class="tab-btn" onclick="switchTab('range-etf-${etf}', 'tab-pct', this)">依變動幅度 (意圖)</button></div>
          <div class="tab-content tab-vol active">${etfVolumeTable("[增] 增持最多 TOP 5 (量體)", inc)}${etfVolumeTable("[減] 減持最多 TOP 5 (量體)", dec)}</div>
          <div class="tab-content tab-pct">${etfPctTable("[增] 增持幅度 TOP 5 (意圖)", incPct)}${etfPctTable("[減] 減持幅度 TOP 5 (意圖)", decPct)}</div>
        </div>`;
    });
    return html;
}
function renderRange() {
    const startDate = document.getElementById("startDate").value;
    const endDate = document.getElementById("endDate").value;
    document.getElementById("rangeSubtitle").textContent = "區間：" + fmtDate(startDate) + " vs " + fmtDate(endDate);
    const overall = buildOverallRows(startDate, endDate);
    document.getElementById("rangeOutput").innerHTML = renderOverall(overall) + renderEtfBlocks(startDate, endDate);
}
function initSelectors() {
    ["startDate", "endDate"].forEach(id => {
        const select = document.getElementById(id);
        availableDates.forEach(d => {
            const opt = document.createElement("option");
            opt.value = d;
            opt.textContent = fmtDate(d);
            select.appendChild(opt);
        });
    });
    document.getElementById("startDate").value = defaultStart;
    document.getElementById("endDate").value = defaultEnd;
    renderRange();
}
initSelectors();
</script>
"""
    script = script.replace("__DATA__", json.dumps(records, ensure_ascii=False))
    script = script.replace("__DATES__", json.dumps(available_dates, ensure_ascii=False))
    script = script.replace("__ETFS__", json.dumps(TARGET_ETFS, ensure_ascii=False))
    script = script.replace("__FUND_TOTALS__", json.dumps(fund_totals, ensure_ascii=False))
    script = script.replace("__START__", default_start)
    script = script.replace("__END__", default_end)
    html += script
    html += page_tail()
    with open("range/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[Done] 已產生自訂區間頁")

def main():
    today = datetime.now()
    
    # 這裡我們保留這段邏輯以取得 today_str 作為「今日下載的標籤」
    # (注意：這裡的 weekday 邏輯主要影響我們去抓哪個日期的檔案，我們繼續沿用)
    if today.weekday() == 0: # 週一
        pass # 原本的寫法是 yesterday -3，這邊我們統一抓今日即可
    elif today.weekday() == 6: # 週日
        today = today - timedelta(days=2)
    elif today.weekday() == 5: # 週六
        today = today - timedelta(days=1)
        
    today_str = today.strftime('%Y%m%d')
    
    print(f"=============== 主動式 ETF 持股變化追蹤 ===============")
    print(f"今日下載標籤: {today_str}\n")
    
    # 確保 history 資料夾存在
    os.makedirs('history', exist_ok=True)
    history_today_file = f"history/history_{today_str}.csv"
    
    # 1. 執行自動下載
    download_all_etfs(today_str)
    
    # 2. 讀取並彙整今日檔案
    print(f"\n[Info] 正在彙整今日 ({today.strftime('%Y-%m-%d')}) 最新持股資料...")
    today_data_list = []
    today_meta_list = []
    for etf in TARGET_ETFS:
        try:
            meta = fetch_etf_meta(etf, today_str)
            if meta:
                today_meta_list.append(meta)
            df = fetch_etf_holdings(etf, today_str)
            if not df.empty:
                df['ETF'] = etf
                today_data_list.append(df)
        except Exception as e:
            print(f"[Error] 處理 {etf} 時發生錯誤: {e}")
            
    if today_data_list:
        df_today_all = pd.concat(today_data_list, ignore_index=True)
    else:
        df_today_all = pd.DataFrame(columns=['Stock_Code', 'Stock_Name', 'Weight', 'Shares', 'ETF'])
    df_today_meta = pd.DataFrame(today_meta_list) if today_meta_list else empty_fund_meta_df()
        
    # 檢查是否與前一個交易日完全相同 (過濾假日或資料未更新的情形)
    is_data_changed = True
    if not df_today_all.empty:
        history_files_for_check = glob.glob('history/history_*.csv')
        if history_files_for_check:
            available_dates_check = sorted([os.path.basename(f).replace('history_', '').replace('.csv', '') for f in history_files_for_check])
            # 排除今天的日期，找前一個交易日
            prev_dates = [d for d in available_dates_check if d < today_str]
            if prev_dates:
                latest_prev_date = prev_dates[-1]
                latest_prev_file = f"history/history_{latest_prev_date}.csv"
                df_prev_all = pd.read_csv(latest_prev_file, dtype={'Stock_Code': str})
                
                # 將兩個 DataFrame 整理並比對 (以 Shares 為主)
                df_today_compare = df_today_all[['ETF', 'Stock_Code', 'Shares']].sort_values(by=['ETF', 'Stock_Code']).reset_index(drop=True)
                df_prev_compare = df_prev_all[['ETF', 'Stock_Code', 'Shares']].sort_values(by=['ETF', 'Stock_Code']).reset_index(drop=True)
                
                # 確保型態一致再比對
                df_today_compare['Shares'] = pd.to_numeric(df_today_compare['Shares'], errors='coerce')
                df_prev_compare['Shares'] = pd.to_numeric(df_prev_compare['Shares'], errors='coerce')
                
                if df_today_compare.equals(df_prev_compare):
                    is_data_changed = False
                    print(f"[Info] 經比對今日資料與 {latest_prev_date} 完全相同，判定為假日或無更動，跳過今日存檔並清理下載檔案。")
                    
                    # 刪除今日下載的檔案
                    for etf in TARGET_ETFS:
                        for ext_file in glob.glob(os.path.join('data', etf, f"{today_str}.*")):
                            try:
                                os.remove(ext_file)
                            except OSError:
                                pass
                    
                    # 若過去已不小心產生了今日的 history 或 dashboard 檔案，一併刪除
                    if os.path.exists(history_today_file):
                        try:
                            os.remove(history_today_file)
                        except OSError:
                            pass
                    dash_file = f"dashboards/{today_str[:4]}/{today_str[4:6]}/index_{today_str}.html"
                    if os.path.exists(dash_file):
                        try:
                            os.remove(dash_file)
                        except OSError:
                            pass

    # 將今日資料存檔
    if not df_today_all.empty and is_data_changed:
        df_today_all.to_csv(history_today_file, index=False, encoding='utf-8-sig')
        if not df_today_meta.empty:
            df_today_meta.to_csv(f"history/fund_meta_{today_str}.csv", index=False, encoding='utf-8-sig')
        print(f"[Save] 今日持股資料已儲存至 {history_today_file}\n")
    elif not is_data_changed:
        pass # 上方已印出提示
    else:
        print(f"[Warning] 今日無讀取到任何資料，未產生存檔。\n")
    
    # 3. 重新掃描並產生所有歷史網頁
    print(f"[Info] 掃描所有歷史資料，準備更新網頁...")
    history_files = glob.glob('history/history_*.csv')
    
    # 從檔名解析出所有的日期，並排序 (例如: 'history_20260421.csv' -> '20260421')
    available_dates = sorted([os.path.basename(f).replace('history_', '').replace('.csv', '') for f in history_files])
    
    if not available_dates:
        print("[Warning] 目前無任何歷史資料可供產出網頁。")
        return
        
    print(f"[Info] 找到 {len(available_dates)} 天的歷史資料，開始產生對應的 dashboard...")
    for i, target_date_str in enumerate(available_dates):
        prev_date_str = available_dates[i-1] if i > 0 else None
        generate_dashboard(target_date_str, prev_date_str, available_dates, is_root=False)
        
    # 將最新的那一天產生一份為預設的 index.html
    latest_date = available_dates[-1]
    latest_prev_date = available_dates[-2] if len(available_dates) > 1 else None
    generate_dashboard(latest_date, latest_prev_date, available_dates, is_root=True)
    generate_weekly_pages(available_dates)
    generate_range_page(available_dates)
    print(f"\n[Done] 所有歷史網頁均已更新，預設入口為 index.html (目前最新為 {latest_date})\n")

if __name__ == "__main__":
    main()
