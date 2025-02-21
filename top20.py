import yfinance as yf
import pandas as pd
import finnhub
import concurrent.futures
import time
import json
import os
import fcntl
import random
import threading
import logging
import subprocess
import pytz
import sys
from datetime import datetime
from threading import Lock

# =============================================================================
# 0. 全局速率限制器（针对 yfinance 请求）
# =============================================================================

class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls    # 最大调用次数
        self.period = period          # 周期（秒）
        self.calls = []               # 保存调用时间戳
        self.lock = Lock()
        
    def acquire(self):
        with self.lock:
            now_time = time.time()
            # 移除周期外的调用记录
            while self.calls and self.calls[0] <= now_time - self.period:
                self.calls.pop(0)
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now_time - self.calls[0])
                logging.info(f"[yfinance] 请求速率达到上限，等待 {sleep_time:.2f} 秒")
                time.sleep(sleep_time)
            self.calls.append(time.time())

# 针对 yfinance 的速率限制：每分钟最多 60 次请求
yfinance_rate_limiter = RateLimiter(60, 60)

# =============================================================================
# 1. 初始化日志和环境
# =============================================================================

now = datetime.now()
log_dir = os.path.join('logs', now.strftime('%Y%m%d_%H%M%S'))
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, 'execution.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.info(f'日志目录: {log_dir}')
logging.info(f'日志文件: {log_filename}')

# =============================================================================
# 2. API Key 及轮换逻辑（用于 Finnhub）
# =============================================================================

api_keys_json = os.getenv("API_KEYS_JSON", "[]")
try:
    api_keys = json.loads(api_keys_json)
except json.JSONDecodeError:
    raise ValueError("API_KEYS_JSON 格式错误，请确保它是一个有效的 JSON 数组。")
if not api_keys or not isinstance(api_keys, list):
    raise ValueError("未找到有效的 API keys！请检查 GitHub Secrets 配置。")

class APIKeyRotator:
    def __init__(self, keys):
        self.keys = keys
        self.index = 0
        self.lock = Lock()
    def get_next_key(self):
        with self.lock:
            key = self.keys[self.index]
            self.index = (self.index + 1) % len(self.keys)
            return key

key_rotator = APIKeyRotator(api_keys)
def get_finnhub_client():
    return finnhub.Client(api_key=key_rotator.get_next_key())

# =============================================================================
# 3. 市场判断函数
# =============================================================================

def is_market_open():
    eastern_tz = pytz.timezone('America/New_York')
    today = datetime.now(eastern_tz).date()
    if today.weekday() >= 5:
        return False
    us_holidays = [f"{today.year}-01-01", f"{today.year}-07-04", f"{today.year}-12-25"]
    holiday_dates = [datetime.strptime(h, "%Y-%m-%d").date() for h in us_holidays]
    return today not in holiday_dates

# =============================================================================
# 4. 股票数据获取与处理（包含缓存/过滤逻辑及限流重试）
# =============================================================================

# 定义全局变量保存过滤文件内容，以便在程序结束后更新缓存
global_filters = {}

def load_or_fetch_symbols():
    logging.info("开始获取和过滤股票列表...")
    
    def load_filter_files():
        filters = {
            'non_us_stocks': {},
            'excluded_stocks': set(),
            'valid_stocks': set()
        }
        try:
            with open('non_us_stocks.json', 'r', encoding='utf-8') as f:
                filters['non_us_stocks'] = json.load(f)
                logging.info(f"已加载 {len(filters['non_us_stocks'])} 个非美股记录")
        except (FileNotFoundError, json.JSONDecodeError):
            logging.info("未找到非美股记录文件或文件格式错误")
        try:
            with open('excluded_stocks.json', 'r', encoding='utf-8') as f:
                filters['excluded_stocks'] = set(json.load(f))
                logging.info(f"已加载 {len(filters['excluded_stocks'])} 个需排除的股票")
        except (FileNotFoundError, json.JSONDecodeError):
            logging.info("未找到需排除的股票列表文件")
        try:
            with open('valid_stocks.json', 'r', encoding='utf-8') as f:
                filters['valid_stocks'] = set(json.load(f))
                logging.info(f"已加载 {len(filters['valid_stocks'])} 个已验证的有效股票")
        except (FileNotFoundError, json.JSONDecodeError):
            logging.info("未找到已验证的有效股票列表文件")
        return filters

    def should_include_symbol(symbol_data, filters):
        symbol = symbol_data.get('symbol', '')
        if not symbol.isalpha() or len(symbol) > 5:
            return False, "非标准股票代码"
        if any(x in symbol for x in ['.WS', 'WS', 'W', '-W', '.W']):
            return False, "权证"
        if symbol in filters['non_us_stocks']:
            return False, "非美股"
        if symbol in filters['excluded_stocks']:
            return False, "在排除列表中"
        if symbol_data.get('type') != 'Common Stock':
            return False, "非普通股"
        if filters['valid_stocks'] and symbol not in filters['valid_stocks']:
            return False, "不在已验证的有效股票列表中"
        return True, "通过"

    try:
        filters = load_filter_files()
        global global_filters
        global_filters = filters  # 保存到全局变量，便于后续更新
        symbols = get_finnhub_client().stock_symbols('US')
        total = len(symbols)
        logging.info(f"从 Finnhub 获取到 {total} 个股票")
        filtered = []
        rejection = {}
        for data in symbols:
            ok, reason = should_include_symbol(data, filters)
            if ok:
                filtered.append(data)
            else:
                sym = data.get('symbol', '')
                rejection.setdefault(reason, []).append(sym)
        logging.info(f"原始股票数量: {total}, 过滤后数量: {len(filtered)}")
        for reason, syms in rejection.items():
            logging.info(f"- {reason}: {len(syms)} 只")
        return filtered
    except Exception as e:
        logging.error(f"获取和过滤股票时出错: {e}")
        return []

def get_stock_details(symbol):
    retries = 5
    while retries > 0:
        try:
            client = get_finnhub_client()
            logging.info(f"[Finnhub] 请求 {symbol}")
            company_info = client.company_profile2(symbol=symbol)
            if not company_info:
                return None
            return {
                'name': company_info.get('name', 'N/A'),
                'market_cap': (company_info.get('marketCapitalization', 0) / 100)
                              if company_info.get('marketCapitalization') else 0,
                'sector': company_info.get('finnhubIndustry', 'N/A'),
                'exchange': company_info.get('exchange', 'N/A')
            }
        except Exception as e:
            msg = str(e)
            if "429" in msg or "Too Many Requests" in msg or "Rate limited" in msg:
                logging.warning(f"[Finnhub] 限流 {symbol}：{e}。等待 10 秒后重试...（剩余 {retries} 次）")
                time.sleep(10)
                retries -= 1
            else:
                logging.error(f"[Finnhub] 获取 {symbol} 详细信息失败: {e}")
                return None
    logging.error(f"[Finnhub] 获取 {symbol} 详细信息失败：超过最大重试次数")
    return None

def process_stock(stock):
    symbol = stock['symbol']
    time.sleep(random.uniform(0.1, 0.3))
    try:
        ticker = yf.Ticker(symbol)
        # yfinance 部分：获取历史数据
        yf_retries = 5
        hist = None
        while yf_retries > 0:
            try:
                logging.info(f"[yfinance] 请求历史数据 {symbol}")
                yfinance_rate_limiter.acquire()
                hist = ticker.history(period="2d")
                break
            except Exception as e:
                msg = str(e)
                if "429" in msg or "Too Many Requests" in msg or "Rate limited" in msg:
                    logging.warning(f"[yfinance] 限流 {symbol}：{e}。等待 10 秒后重试...（剩余 {yf_retries} 次）")
                    time.sleep(10)
                    yf_retries -= 1
                else:
                    logging.error(f"[yfinance] 获取 {symbol} 历史数据失败: {e}")
                    return None
        if hist is None or len(hist) < 2:
            logging.info(f"{symbol} 历史数据不足")
            return None
        current_price = hist['Close'].iloc[-1]
        previous_close = hist['Close'].iloc[-2]
        if current_price <= 0 or previous_close <= 0:
            logging.info(f"{symbol} 价格数据无效")
            return None
        price_change = ((current_price - previous_close) / previous_close) * 100

        # Finnhub 部分：获取详细信息
        details = get_stock_details(symbol)
        if not details or details['market_cap'] <= 0:
            logging.info(f"{symbol} 无法获取有效详细信息")
            return None

        return {
            '股票代码': symbol,
            '公司名称': details['name'],
            '市值(亿)': round(details['market_cap'], 2),
            '板块': details['sector'],
            "昨天收盘": f"{hist.index[-2].strftime('%Y-%m-%d')} {round(previous_close, 2)}",
            "今天收盘": f"{hist.index[-1].strftime('%Y-%m-%d')} {round(current_price, 2)}",
            '涨跌幅(%)': round(price_change, 2)
        }
    except Exception as e:
        logging.info(f"处理 {symbol} 时出错: {e}")
        return None

class ThreadSafeList:
    def __init__(self):
        self._list = []
        self._lock = threading.Lock()
    def append(self, item):
        with self._lock:
            self._list.append(item)
    def get_list(self):
        with self._lock:
            return self._list.copy()

def get_gainers_multithreaded(symbols, max_workers=None):
    total = len(symbols)
    logging.info(f"开始处理 {total} 只股票...")
    results = ThreadSafeList()
    if max_workers is None:
        max_workers = len(api_keys)
    processed = 0
    valid = 0
    progress_lock = threading.Lock()
    def worker(stock):
        nonlocal processed, valid
        res = process_stock({'symbol': stock.get('symbol', '')})
        with progress_lock:
            processed += 1
            if res:
                results.append(res)
                valid += 1
            if processed % 10 == 0:
                logging.info(f"进度: {processed}/{total} ({round(processed/total*100,2)}%)，有效: {valid}")
        return res
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(worker, symbols))
    logging.info(f"处理完成，总计: {processed}，有效: {valid}，无效: {processed - valid}")
    data = results.get_list()
    if not data:
        logging.info("没有获取到有效股票数据！")
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if '涨跌幅(%)' not in df.columns:
        logging.info("数据中缺少涨跌幅信息！")
        return df
    return df.sort_values(by='涨跌幅(%)', ascending=False)

# =============================================================================
# 5. 结果展示与数据保存（包含各 top20 分组和聚合结果）
# =============================================================================

def setup_logging_again():
    os.makedirs(log_dir, exist_ok=True)
    return log_dir, log_filename

def display_results(df):
    if df.empty:
        logging.info("没有数据可供显示!")
        return
    current_dir, _ = setup_logging_again()
    aggregated_results = ""  # 用于聚合所有 top20 分组结果

    def log_dataframe(title, dataframe):
        nonlocal aggregated_results
        section = f"\n{title}\n"
        if not dataframe.empty:
            formatted = dataframe.apply(lambda col: col.apply(lambda x: f'{x:.2f}' if pd.api.types.is_numeric_dtype(col) and pd.notnull(x) else str(x)))
            table_str = formatted.to_string(index=False, justify='center')
            section += table_str + "\n"
        else:
            section += "没有数据！\n"
        aggregated_results += section
        logging.info(section)

    top20 = df.head(20)
    log_dataframe("=== 涨幅榜前20名股票 ===", top20[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']])
    
    billion_20 = df[df['市值(亿)'] > 20].head(20)
    log_dataframe("=== 市值超过20亿的涨幅榜前20名股票 ===", billion_20[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']])
    
    billion_50 = df[df['市值(亿)'] > 50].head(20)
    log_dataframe("=== 市值超过50亿的涨幅榜前20名股票 ===", billion_50[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']])
    
    billion_100 = df[df['市值(亿)'] > 100].head(20)
    log_dataframe("=== 市值超过100亿的涨幅榜前20名股票 ===", billion_100[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']])
    
    billion_200 = df[df['市值(亿)'] > 200].head(20)
    log_dataframe("=== 市值超过200亿的涨幅榜前20名股票 ===", billion_200[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']])
    
    billion_1000 = df[df['市值(亿)'] > 1000].head(20)
    log_dataframe("=== 市值超过1000亿的涨跌榜前20名股票 ===", billion_1000[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']])
    
    # 保存聚合结果到一个文件
    aggregated_file = os.path.join(current_dir, 'top20_result.log')
    with open(aggregated_file, 'w', encoding='utf-8') as f:
        f.write(aggregated_results)
    logging.info(f"聚合结果已保存到: {aggregated_file}")

def process_final_stock_data(df):
    current_dir, _ = setup_logging_again()
    output_file = os.path.join(current_dir, 'stocks_data.json')
    sector_file = os.path.join(current_dir, 'sector_analysis.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(df.to_dict('records'), f, ensure_ascii=False, indent=2)
    logging.info(f"股票数据已保存到 {output_file}")
    sector_perf = df.groupby('板块')['涨跌幅(%)'].agg(['mean', 'count']).reset_index()
    sector_perf = sector_perf.rename(columns={'mean': '平均涨跌幅', 'count': '股票数量'})
    sector_perf = sector_perf.sort_values('平均涨跌幅', ascending=False)
    logging.info("\n=== 板块涨跌分析 ===")
    logging.info("\n" + str(sector_perf))
    with open(sector_file, 'w', encoding='utf-8') as f:
        json.dump(sector_perf.to_dict('records'), f, ensure_ascii=False, indent=2)
    logging.info(f"板块分析已保存到 {sector_file}")
    return sector_perf

# =============================================================================
# 6. 调用外部 Shell 脚本进行 Git 更新
# =============================================================================

def run_git_update():
    try:
        subprocess.run(['chmod', '+x', 'update_git.sh'], check=True)
        result = subprocess.run(
            ['./update_git.sh'],
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True
        )
        logging.info("Git更新成功完成")
        logging.debug(f"脚本输出: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git更新脚本执行失败: {e}")
        logging.error(f"错误输出: {e.stderr}")
    except Exception as e:
        logging.error(f"执行git更新时发生未知错误: {e}")

# =============================================================================
# 7. 更新缓存文件：non_us_stocks.json, excluded_stocks.json, valid_stocks.json
# =============================================================================

def update_filter_files():
    global global_filters
    if global_filters:
        # 写入 non_us_stocks.json
        with open('non_us_stocks.json', 'w', encoding='utf-8') as f:
            json.dump(global_filters.get('non_us_stocks', {}), f, ensure_ascii=False, indent=2)
        # 写入 excluded_stocks.json（将 set 转换为 list）
        with open('excluded_stocks.json', 'w', encoding='utf-8') as f:
            json.dump(list(global_filters.get('excluded_stocks', set())), f, ensure_ascii=False, indent=2)
        # 写入 valid_stocks.json（将 set 转换为 list）
        with open('valid_stocks.json', 'w', encoding='utf-8') as f:
            json.dump(list(global_filters.get('valid_stocks', set())), f, ensure_ascii=False, indent=2)
        logging.info("过滤文件已更新")

# =============================================================================
# 8. 主程序入口
# =============================================================================

if __name__ == "__main__":
    if not is_market_open():
        logging.warning("今日非交易日，程序终止")
        sys.exit("非交易日")
    try:
        symbols = load_or_fetch_symbols()
        if not symbols:
            logging.warning("没有获取到任何股票代码！")
            sys.exit("无有效股票")
        gainers_df = get_gainers_multithreaded(symbols)
        if not gainers_df.empty:
            run_git_update()
            display_results(gainers_df)
            process_final_stock_data(gainers_df)
        else:
            logging.warning("没有获取到有效的股票数据")
        # 更新缓存文件，减少下次请求量
        update_filter_files()
    except Exception as e:
        logging.error(f"程序执行过程中发生致命错误: {e}")
        logging.exception("详细错误堆栈:")
