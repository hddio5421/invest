import os
import glob
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
            excel = pd.ExcelFile(file_path)
            target_sheet = excel.sheet_names[0]
            for s in excel.sheet_names:
                if '股' in s or '明細' in s or '成' in s:
                    target_sheet = s
                    break
            df_raw = pd.read_excel(file_path, sheet_name=target_sheet, header=None)
            
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

def generate_dashboard(target_date_str, prev_date_str, available_dates, is_root=False):
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
    for etf in TARGET_ETFS:
        try:
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
        
    # 將今日資料存檔
    if not df_today_all.empty:
        df_today_all.to_csv(history_today_file, index=False, encoding='utf-8-sig')
        print(f"[Save] 今日持股資料已儲存至 {history_today_file}\n")
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
    print(f"\n[Done] 所有歷史網頁均已更新，預設入口為 index.html (目前最新為 {latest_date})\n")

if __name__ == "__main__":
    main()