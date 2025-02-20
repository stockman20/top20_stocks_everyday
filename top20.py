import yfinance as yf
import pandas as pd
import finnhub
import concurrent.futures
import time
import json
import os
import fcntl
import queue
import random
import threading
from datetime import datetime
import logging
import subprocess
import pytz
import sys

# 获取当前日期时间
now = datetime.now()
log_dir = os.path.join('logs', now.strftime('%Y%m%d_%H%M%S'))

# 读取 API Keys（从环境变量 JSON 解析）
api_keys_json = os.getenv("API_KEYS_JSON", "[]")
api_keys = json.loads(api_keys_json)

if not api_keys:
    raise ValueError("No API keys found! Make sure you've set up GitHub Secrets correctly.")

class APIKeyRotator:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.key_index = 0
        self.lock = threading.Lock()

    def get_next_key(self):
        with self.lock:
            current_key = self.api_keys[self.key_index]
            self.key_index = (self.key_index + 1) % len(self.api_keys)
            return current_key

key_rotator = APIKeyRotator(api_keys)

def is_market_open():
    """判断是否是美股交易日"""
    eastern_tz = pytz.timezone('America/New_York')
    today = datetime.now(eastern_tz).date()

    if today.weekday() >= 5:  # 周六日
        return False

    us_holidays = [
        f"{today.year}-01-01",  # 元旦
        f"{today.year}-07-04",  # 独立日
        f"{today.year}-12-25",  # 圣诞节
    ]

    holiday_dates = [datetime.strptime(h, "%Y-%m-%d").date() for h in us_holidays]
    return today not in holiday_dates

def setup_logging():
    """配置日志系统"""
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, 'execution.log')
    
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )

    logging.info(f'日志目录: {log_dir}')
    logging.info(f'日志文件: {log_filename}')
    return log_dir, log_filename

def get_finnhub_client():
    """获取 Finnhub 客户端实例"""
    current_key = key_rotator.get_next_key()
    return finnhub.Client(api_key=current_key)

def load_or_fetch_symbols():
    """从 Finnhub 获取美股代码"""
    logging.info("开始获取股票列表...")
    try:
        symbols = get_finnhub_client().stock_symbols('US')
        logging.info(f"获取到 {len(symbols)} 只股票")
        return symbols
    except Exception as e:
        logging.error(f"获取股票列表失败: {e}")
        return []

def get_stock_details(symbol):
    """获取股票详细信息"""
    try:
        stock = yf.Ticker(symbol)
        info = stock.info
        market_cap = info.get('marketCap', 0) / 1e8 if info.get('marketCap') else 0
        return {
            'name': info.get('longName', 'N/A'),
            'market_cap': round(market_cap, 2),
            'sector': info.get('sector', 'N/A')
        }
    except Exception as e:
        logging.error(f"获取 {symbol} 详细信息失败: {e}")
        return None

def process_stock(stock):
    """处理单只股票数据"""
    symbol = stock.get('symbol', '')
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2d")

        if len(hist) < 2:
            return None

        previous_close, current_price = hist['Close'].iloc[-2], hist['Close'].iloc[-1]
        price_change = ((current_price - previous_close) / previous_close) * 100
        details = get_stock_details(symbol)

        if details is None or details['market_cap'] <= 0:
            return None

        return {
            '股票代码': symbol,
            '公司名称': details['name'],
            '市值(亿)': details['market_cap'],
            '板块': details['sector'],
            '昨天收盘': round(previous_close, 2),
            '今天收盘': round(current_price, 2),
            '涨跌幅(%)': round(price_change, 2)
        }
    except Exception as e:
        logging.error(f"处理 {symbol} 失败: {e}")
        return None

def get_gainers_multithreaded():
    """多线程获取股票数据"""
    stock_symbols = load_or_fetch_symbols()
    total_symbols = len(stock_symbols)
    stock_data = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
        results = list(executor.map(process_stock, stock_symbols))

    stock_data = [result for result in results if result is not None]
    df = pd.DataFrame(stock_data)

    if not df.empty:
        df.sort_values(by='涨跌幅(%)', ascending=False, inplace=True)
        display_results(df)
        process_final_stock_data(df)

    return df

def display_results(df):
    """打印并存储前 20 涨幅股票"""
    log_dir, _ = setup_logging()
    result_log_filename = os.path.join(log_dir, 'top20_result.log')

    result_logger = logging.getLogger('result_logger')
    result_logger.setLevel(logging.INFO)
    
    file_handler = logging.FileHandler(result_log_filename, encoding='utf-8')
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    result_logger.addHandler(file_handler)

    top_20 = df.head(20)
    result_logger.info("\n=== 涨幅前 20 名股票 ===")
    result_logger.info(top_20.to_string(index=False))

    file_handler.close()
    result_logger.removeHandler(file_handler)

def process_final_stock_data(df):
    """存储最终股票数据"""
    log_dir, _ = setup_logging()
    output_filename = os.path.join(log_dir, 'stocks_data.json')
    sector_analysis_filename = os.path.join(log_dir, 'sector_analysis.json')

    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(df.to_dict('records'), f, ensure_ascii=False, indent=2)

    sector_performance = df.groupby('板块')['涨跌幅(%)'].agg(['mean', 'count']).reset_index()
    sector_performance = sector_performance.rename(columns={'mean': '平均涨跌幅', 'count': '股票数量'})

    with open(sector_analysis_filename, 'w', encoding='utf-8') as f:
        json.dump(sector_performance.to_dict('records'), f, ensure_ascii=False, indent=2)

def run_git_update():
    """运行 Git 提交更新"""
    try:
        subprocess.run(['chmod', '+x', 'update_git.sh'], check=True)
        result = subprocess.run(['./update_git.sh'], shell=True, capture_output=True, text=True, check=True)
        logging.info("Git 更新成功")
        logging.debug(f"脚本输出: {result.stdout}")
    except Exception as e:
        logging.error(f"Git 更新失败: {e}")

if __name__ == "__main__":
    """主执行流程"""
    if not is_market_open():
        logging.warning("今日非交易日，程序终止")
        sys.exit("非交易日")

    try:
        gainers_df = get_gainers_multithreaded()
        if not gainers_df.empty:
            run_git_update()
    except Exception as e:
        logging.error(f"程序执行错误: {e}")
