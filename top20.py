import yfinance as yf
import pandas as pd
import finnhub
import concurrent.futures
import time
import json
import os
import random
import threading
import logging
import subprocess
import pytz
import sys
import holidays
from datetime import datetime, timedelta
from threading import Lock

# 美国全年假期
us_holidays = holidays.US(years=datetime.now().year)
# 全局锁，用于保护 global_filters 的修改
global_filters_lock = Lock()

# 在文件顶部添加全局变量
global_log_dir = None

# =============================================================================
# 0. 全局速率限制器（针对 yfinance 请求）
# =============================================================================
class ThreadSafeList:
    def __init__(self):
        self._list = []
        self._lock = threading.Lock()

    def append(self, item):
        with self._lock:
            self._list.append(item)

    def extend(self, items):
        with self._lock:
            self._list.extend(items)

    def get_list(self):
        with self._lock:
            return self._list.copy()

    def clear(self):
        with self._lock:
            self._list.clear()


class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            now_time = time.time()
            while self.calls and self.calls[0] <= now_time - self.period:
                self.calls.pop(0)
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now_time - self.calls[0])
                logging.info(f"[yfinance] 请求速率达到上限，等待 {sleep_time:.2f} 秒")
                time.sleep(sleep_time)
            self.calls.append(time.time())


yfinance_rate_limiter = RateLimiter(60, 60)


# =============================================================================
# 0.1 指数退避策略
# =============================================================================
def exponential_backoff(attempt, base_delay=5, max_delay=120):
    """计算指数退避延迟时间"""
    delay = min(base_delay * (2 ** attempt), max_delay)
    # 添加 10% 的随机抖动，避免雷群效应
    jitter = delay * random.uniform(0, 0.1)
    return delay + jitter


# =============================================================================
# 1. 初始化日志（仅输出到屏幕）
# =============================================================================
# 只配置屏幕输出，不写入文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


# 定义自定义日志缓冲处理器，用于后续聚合写入其他日志文件（但不输出到屏幕）
class BufferHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.buffer = []

    def emit(self, record):
        self.buffer.append(self.format(record))


buffer_handler = BufferHandler()
buffer_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
logging.getLogger().addHandler(buffer_handler)

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
    return today not in us_holidays


# =============================================================================
# 4. 股票数据获取与处理
# =============================================================================

global_filters = {}

# 临时跳过的股票：API 错误、超时、数据不足等临时性问题（不写入 excluded_stocks.json）
# 注意：我们不再自动永久排除股票，除非经过严格验证
temporarily_skipped_stocks = ThreadSafeList()

# 缓存文件路径
SYMBOLS_CACHE_FILE = 'symbols_cache.json'
SYMBOLS_CACHE_EXPIRY_HOURS = 12  # 缓存有效期（小时）


def load_filter_files():
    """加载过滤缓存文件"""
    filters = {
        'non_us_stocks': {},
        'excluded_stocks': set()
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
    return filters


def should_include_symbol(symbol_data, filters):
    """判断股票是否应该被包含"""
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
    return True, "通过"


def load_symbols_cache():
    """加载股票代码缓存"""
    try:
        with open(SYMBOLS_CACHE_FILE, 'r', encoding='utf-8') as f:
            cache = json.load(f)
            timestamp_str = cache.get('timestamp', '2000-01-01T00:00:00')
            # 兼容 Python 3.6（没有 fromisoformat）
            cached_time = datetime.strptime(timestamp_str[:19], '%Y-%m-%dT%H:%M:%S')
            if datetime.now() - cached_time < timedelta(hours=SYMBOLS_CACHE_EXPIRY_HOURS):
                logging.info(f"使用缓存的股票列表 (缓存时间: {cached_time})")
                return cache.get('symbols', [])
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        pass
    return None


def save_symbols_cache(symbols):
    """保存股票代码缓存"""
    cache = {
        'timestamp': datetime.now().isoformat(),
        'symbols': symbols
    }
    with open(SYMBOLS_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    logging.info(f"已缓存 {len(symbols)} 个股票代码")


def load_or_fetch_symbols():
    """加载或获取股票列表"""
    logging.info("开始获取和过滤股票列表...")

    try:
        filters = load_filter_files()
        global global_filters
        global_filters = filters

        # 尝试使用缓存
        cached_symbols = load_symbols_cache()
        if cached_symbols:
            symbols = cached_symbols
            logging.info(f"从缓存加载到 {len(symbols)} 个股票")
        else:
            # 从 Finnhub 获取
            symbols = get_finnhub_client().stock_symbols('US')
            logging.info(f"从 Finnhub 获取到 {len(symbols)} 个股票")
            # 保存缓存
            save_symbols_cache(symbols)

        total = len(symbols)
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
    """获取股票详细信息（带指数退避重试）
    
    返回:
        dict: 股票详细信息
        None: 临时性错误（API 限流等），不应永久排除
        'NOT_FOUND': 股票不存在，应永久排除
    """
    max_retries = 5
    valid_exchanges = [
        'NASDAQ NMS', 'NASDAQ GM', 'NASDAQ GS', 'NASDAQ GLOBAL', 'NASDAQ GLOBAL SELECT',
        'NASDAQ CAPITAL MARKET', 'NEW YORK STOCK EXCHANGE', 'NYSE', 'NYSE ARCA',
        'NYSE AMERICAN', 'NYSE NATIONAL'
    ]

    for attempt in range(max_retries):
        try:
            client = get_finnhub_client()
            logging.info(f"[Finnhub] 请求 {symbol}")
            company_info = client.company_profile2(symbol=symbol)
            if not company_info:
                # 空响应可能是临时问题，不永久排除
                return None

            exchange = company_info.get('exchange', '').upper()
            is_valid_exchange = any(
                valid_ex in exchange.replace('-', ' ').replace(',', ' ')
                for valid_ex in valid_exchanges
            )

            if not is_valid_exchange:
                with global_filters_lock:
                    global global_filters
                    global_filters.setdefault('non_us_stocks', {})[symbol] = {
                        'exchange': exchange,
                        'name': company_info.get('name', 'N/A'),
                        'country': company_info.get('country', 'N/A'),
                        'added_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                logging.info(f"跳过非美国主要交易所上市的股票 {symbol} (交易所: {exchange})")
                return None

            return {
                'name': company_info.get('name', 'N/A'),
                'market_cap': (company_info.get('marketCapitalization', 0) / 100)
                              if company_info.get('marketCapitalization') else 0,
                'sector': company_info.get('finnhubIndustry', 'N/A'),
                'exchange': exchange
            }
        except Exception as e:
            msg = str(e)
            if "429" in msg or "Too Many Requests" in msg or "Rate limited" in msg:
                delay = exponential_backoff(attempt)
                logging.warning(f"[Finnhub] 限流 {symbol}：{e}。等待 {delay:.2f} 秒后重试...（第 {attempt + 1}/{max_retries} 次）")
                time.sleep(delay)
            else:
                logging.error(f"[Finnhub] 获取 {symbol} 详细信息失败: {e}")
                return None

    logging.error(f"[Finnhub] 获取 {symbol} 详细信息失败：超过最大重试次数")
    return None


def fetch_batch_history(symbols, batch_size=50):
    """批量获取股票历史数据"""
    all_data = {}
    symbol_list = [s['symbol'] if isinstance(s, dict) else s for s in symbols]

    for i in range(0, len(symbol_list), batch_size):
        batch = symbol_list[i:i + batch_size]
        batch_str = " ".join(batch)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                logging.info(f"[yfinance] 批量请求 {len(batch)} 只股票 ({i + 1}-{i + len(batch)}/{len(symbol_list)})")
                yfinance_rate_limiter.acquire()

                tickers = yf.Tickers(batch_str)
                for symbol in batch:
                    try:
                        ticker = tickers.tickers.get(symbol)
                        if ticker:
                            hist = ticker.history(period="2d")
                            if hist is not None and len(hist) >= 2:
                                all_data[symbol] = hist
                    except Exception as e:
                        logging.debug(f"获取 {symbol} 数据失败: {e}")
                break
            except Exception as e:
                msg = str(e)
                if "429" in msg or "Too Many Requests" in msg or "Rate limited" in msg:
                    delay = exponential_backoff(attempt, base_delay=10)
                    logging.warning(f"[yfinance] 批量请求限流，等待 {delay:.2f} 秒后重试...（第 {attempt + 1}/{max_retries} 次）")
                    time.sleep(delay)
                else:
                    logging.error(f"[yfinance] 批量请求失败: {e}")
                    break

    return all_data


def process_stock_with_history(symbol, hist):
    """处理单只股票（已有历史数据）
    
    注意：此函数不会将股票加入永久排除列表，所有失败都视为临时问题
    """
    try:
        if hist is None or len(hist) < 2:
            # 数据不足可能是临时问题（新股、停牌等），不排除
            logging.debug(f"{symbol} 历史数据不足，跳过")
            temporarily_skipped_stocks.append(symbol)
            return None

        current_price = float(hist['Close'].iloc[-1])
        previous_close = float(hist['Close'].iloc[-2])

        if current_price <= 0 or previous_close <= 0:
            logging.debug(f"{symbol} 价格数据无效，跳过")
            temporarily_skipped_stocks.append(symbol)
            return None

        price_change = ((current_price - previous_close) / previous_close) * 100

        details = get_stock_details(symbol)
        if not details:
            logging.debug(f"{symbol} 无法获取详细信息，跳过")
            temporarily_skipped_stocks.append(symbol)
            return None
        
        if details['market_cap'] <= 0:
            logging.debug(f"{symbol} 市值数据无效，跳过")
            temporarily_skipped_stocks.append(symbol)
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
        logging.debug(f"处理 {symbol} 时出错: {e}，跳过")
        temporarily_skipped_stocks.append(symbol)
        return None


def process_stock(stock):
    """处理单只股票（兼容旧接口）
    
    注意：所有失败都视为临时问题，不永久排除任何股票
    """
    symbol = stock['symbol'] if isinstance(stock, dict) else stock
    time.sleep(random.uniform(0.1, 0.3))

    max_retries = 5
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol)
            logging.info(f"[yfinance] 请求历史数据 {symbol}")
            yfinance_rate_limiter.acquire()
            hist = ticker.history(period="2d")
            return process_stock_with_history(symbol, hist)
        except Exception as e:
            msg = str(e)
            if "429" in msg or "Too Many Requests" in msg or "Rate limited" in msg:
                delay = exponential_backoff(attempt)
                logging.warning(f"[yfinance] 限流 {symbol}：{e}。等待 {delay:.2f} 秒后重试...（第 {attempt + 1}/{max_retries} 次）")
                time.sleep(delay)
            elif "timeout" in msg.lower():
                logging.warning(f"{symbol} 请求超时，跳过")
                temporarily_skipped_stocks.append(symbol)
                return None
            else:
                # 所有其他错误都视为临时问题
                logging.debug(f"[yfinance] 获取 {symbol} 历史数据失败: {e}，跳过")
                temporarily_skipped_stocks.append(symbol)
                return None

    # 重试超限也是临时问题
    logging.warning(f"[yfinance] 获取 {symbol} 历史数据失败：超过最大重试次数，跳过")
    temporarily_skipped_stocks.append(symbol)
    return None


def get_gainers_multithreaded(symbols, max_workers=None, use_batch=True):
    """多线程获取股票涨幅数据"""
    total = len(symbols)
    logging.info(f"开始处理 {total} 只股票...")
    results = ThreadSafeList()

    if max_workers is None:
        max_workers = min(len(api_keys) * 2, 10)

    if use_batch and total > 50:
        # 使用批量获取模式
        logging.info("使用批量获取模式...")
        history_data = fetch_batch_history(symbols)
        logging.info(f"批量获取到 {len(history_data)} 只股票的历史数据")

        # 处理批量获取的数据
        processed = 0
        valid = 0
        progress_lock = threading.Lock()

        def worker(item):
            nonlocal processed, valid
            symbol, hist = item
            res = process_stock_with_history(symbol, hist)
            with progress_lock:
                processed += 1
                if res:
                    results.append(res)
                    valid += 1
                if processed % 50 == 0:
                    logging.info(f"进度: {processed}/{len(history_data)} ({round(processed / len(history_data) * 100, 2)}%)，有效: {valid}")
            return res

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(worker, history_data.items()))

        # 处理批量获取失败的股票（使用单独请求）
        failed_symbols = [s['symbol'] if isinstance(s, dict) else s for s in symbols if (s['symbol'] if isinstance(s, dict) else s) not in history_data]
        if failed_symbols:
            logging.info(f"处理 {len(failed_symbols)} 只批量获取失败的股票...")
            for symbol in failed_symbols:
                res = process_stock({'symbol': symbol})
                if res:
                    results.append(res)
                    valid += 1
                processed += 1

        logging.info(f"处理完成，总计: {processed}，有效: {valid}，无效: {processed - valid}")
    else:
        # 使用逐个获取模式（兼容小量数据和测试）
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
                    logging.info(f"进度: {processed}/{total} ({round(processed / total * 100, 2)}%)，有效: {valid}")
            return res

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(worker, symbols))

        logging.info(f"处理完成，总计: {processed}，有效: {valid}，无效: {processed - valid}")

    # 输出统计信息
    temp_skipped = temporarily_skipped_stocks.get_list()
    logging.info(f"临时跳过股票数: {len(temp_skipped)}（不会永久排除）")

    data = results.get_list()
    if not data:
        logging.info("没有获取到有效股票数据！")
        return pd.DataFrame(), []

    df = pd.DataFrame(data)
    if '涨跌幅(%)' not in df.columns:
        logging.info("数据中缺少涨跌幅信息！")
        return df, []

    return df.sort_values(by='涨跌幅(%)', ascending=False), []


# =============================================================================
# 5. 结果展示与数据保存（延迟创建目录）
# =============================================================================

def setup_logging_with_file():
    """设置日志目录"""
    global global_log_dir
    if global_log_dir is not None:
        return global_log_dir, None

    now = datetime.now()
    global_log_dir = os.path.join('logs', now.strftime('%Y%m%d_%H%M%S'))
    os.makedirs(global_log_dir, exist_ok=True)
    logging.info(f'日志目录: {global_log_dir}')
    return global_log_dir, None


def display_results(df):
    """展示并保存结果"""
    if df.empty:
        logging.info("没有数据可供显示!")
        return False

    aggregated_results = ""

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

    # 只有在有数据时才创建目录并保存文件
    if aggregated_results.strip():
        log_dir, _ = setup_logging_with_file()
        aggregated_file = os.path.join(log_dir, 'top20_result.log')
        with open(aggregated_file, 'w', encoding='utf-8') as f:
            f.write(aggregated_results)
        logging.info(f"聚合结果已保存到: {aggregated_file}")
        return True
    return False


def process_final_stock_data(df, log_dir):
    """处理并保存最终股票数据"""
    output_file = os.path.join(log_dir, 'stocks_data.json')
    sector_file = os.path.join(log_dir, 'sector_analysis.json')

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
    """执行 Git 更新脚本"""
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
# 7. 更新缓存文件（统一更新，避免重复写入）
# =============================================================================

def update_filter_files():
    """统一更新过滤缓存文件
    
    注意：只更新 non_us_stocks.json（非美股交易所的股票）
    excluded_stocks.json 不再自动更新，需要手动维护
    """
    global global_filters

    # 只更新 non_us_stocks.json（这是确定性的：股票在非美国交易所上市）
    if global_filters.get('non_us_stocks'):
        try:
            with open('non_us_stocks.json', 'r', encoding='utf-8') as f:
                existing_non_us = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_non_us = {}
        
        new_non_us = global_filters.get('non_us_stocks', {})
        existing_non_us.update(new_non_us)
        
        with open('non_us_stocks.json', 'w', encoding='utf-8') as f:
            json.dump(existing_non_us, f, ensure_ascii=False, indent=2)
        
        if new_non_us:
            logging.info(f"已更新 non_us_stocks.json，新增 {len(new_non_us)} 条非美股记录，共 {len(existing_non_us)} 条")
    
    # excluded_stocks.json 不再自动更新
    # 如果需要排除某些股票，请手动编辑该文件
    logging.info("excluded_stocks.json 不自动更新，如需排除股票请手动编辑")


# =============================================================================
# 8. 主程序入口
# =============================================================================

if __name__ == "__main__":
    if not is_market_open():
        logging.warning("今日非交易日，程序终止")
        logging.info("非交易日")
        sys.exit(0)

    try:
        # 在程序开始时初始化日志目录
        log_dir, _ = setup_logging_with_file()

        symbols = load_or_fetch_symbols()
        if not symbols:
            logging.warning("没有获取到任何股票代码！")
            sys.exit("无有效股票")

        gainers_df, _ = get_gainers_multithreaded(symbols)

        if not gainers_df.empty:
            generated_top20 = display_results(gainers_df)
            process_final_stock_data(gainers_df, log_dir)

            # 更新过滤文件（只更新 non_us_stocks.json）
            update_filter_files()

            # 在所有文件生成和更新后调用 Git 提交
            if generated_top20:
                run_git_update()
            else:
                logging.warning("没有生成 top20_result.log 文件")
        else:
            logging.warning("没有获取到有效的股票数据")

    except Exception as e:
        logging.error(f"程序执行过程中发生致命错误: {e}")
        logging.exception("详细错误堆栈:")
