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
from threading import Thread
from datetime import datetime, timedelta
import logging
import pickle
import subprocess


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


# API Key 列表
api_keys = [
    "cu8b0u1r01qhqu5ciok0cu8b0u1r01qhqu5ciokg",
    "cu9kb2hr01qnf5nn8c30cu9kb2hr01qnf5nn8c3g",
    "cu9kbbpr01qnf5nn8cp0cu9kbbpr01qnf5nn8cpg",
    "cu9kbipr01qnf5nn8da0cu9kbipr01qnf5nn8dag",
    "cu9kbthr01qnf5nn8e20cu9kbthr01qnf5nn8e2g",
    "cua5621r01qkpes47sagcua5621r01qkpes47sb0",
    "cua5739r01qkpes47uhgcua5739r01qkpes47ui0",
    "cua57p9r01qkpes486r0cua57p9r01qkpes486rg",
    "cua58lpr01qkpes48ccgcua58lpr01qkpes48cd0"
]

key_rotator = APIKeyRotator(api_keys)

# 设置 Pandas 显示选项
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# 获取当前日期时间
now = datetime.now()
log_dir = os.path.join('logs', now.strftime('%Y%m%d_%H%M%S'))


def setup_logging():
    # 创建按照年月日时分秒格式的目录路径

    # 确保目录存在
    os.makedirs(log_dir, exist_ok=True)

    # 生成日志文件名
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(log_dir, f'execution.log')

    # 配置日志
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )

    logging.info(f'日志目录: {log_dir}')
    logging.info(f'日志文件: {log_filename}')

    return log_dir, log_filename  # 返回目录和文件名


# 替换原有的日志设置
logging.basicConfig(
    filename=setup_logging(),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

# 创建全局更新队列
update_queue = queue.Queue()


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


def get_finnhub_client():
    current_key = key_rotator.get_next_key()
    return finnhub.Client(api_key=current_key)


def load_or_fetch_symbols():
    logging.info("从 Finnhub 获取股票列表...")
    try:
        # 加载非美股列表
        try:
            with open('non_us_stocks.json', 'r', encoding='utf-8') as f:
                non_us_stocks = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            non_us_stocks = {}

        symbols = get_finnhub_client().stock_symbols('US')
        total_symbols = len(symbols)

        # 转换为所需的格式，并过滤掉已知的非美股
        symbols_data = [
            symbol for symbol in symbols
            if symbol.get('type') == 'Common Stock' and 
            symbol.get('symbol') not in non_us_stocks
        ]

        logging.info(f"获取到 {len(symbols_data)} 个股票 (已排除 {len(non_us_stocks)} 个已知非美股)")
        logging.info(f"实际需要处理的股票数量: {len(symbols_data)} / {total_symbols} "
                    f"({round(len(symbols_data)/total_symbols*100, 2)}%)")
        return symbols_data

    except Exception as e:
        logging.info(f"从 Finnhub 获取数据时出错: {e}")
        return []


def get_stock_details(symbol):
    # 创建或加载非美股列表文件
    non_us_stocks_file = 'non_us_stocks.json'
    try:
        with open(non_us_stocks_file, 'r', encoding='utf-8') as f:
            non_us_stocks = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        non_us_stocks = {}

    # 如果股票已知是非美股，直接返回None
    if symbol in non_us_stocks:
        logging.info(f"跳过已知的非美股: {symbol} (交易所: {non_us_stocks[symbol]['exchange']})")
        return None

    try:
        stock = yf.Ticker(symbol)
        info = stock.info
        max_retries = 5
        retries = 0

        while retries < max_retries:
            try:
                company_info = get_finnhub_client().company_profile2(symbol=symbol)
                exchange = company_info.get('exchange', '').upper()

                valid_exchanges = [
                    'NASDAQ',
                    'NASDAQ NMS',
                    'NASDAQ GM',
                    'NASDAQ GS',
                    'NASDAQ GLOBAL',
                    'NASDAQ GLOBAL SELECT',
                    'NASDAQ CAPITAL MARKET',
                    'NEW YORK STOCK EXCHANGE',
                    'NYSE',
                    'NYSE ARCA',
                    'NYSE AMERICAN'
                ]

                is_valid_exchange = any(
                    valid_ex in exchange.replace('-', ' ').replace(',', ' ')
                    for valid_ex in valid_exchanges
                )

                if not is_valid_exchange:
                    # 记录非美股信息
                    non_us_stocks[symbol] = {
                        'exchange': exchange,
                        'name': company_info.get('name', 'N/A'),
                        'country': company_info.get('country', 'N/A'),
                        'added_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    # 使用文件锁来安全地写入文件
                    with open(non_us_stocks_file, 'w', encoding='utf-8') as f:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                        json.dump(non_us_stocks, f, ensure_ascii=False, indent=2)
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)

                    logging.info(f"跳过非美国主要交易所上市的股票 {symbol} (交易所: {exchange})")
                    return None

                market_cap = company_info.get('marketCapitalization', 0) / 100 if company_info.get(
                    'marketCapitalization') else 0

                return {
                    'name': company_info.get('name', 'N/A'),
                    'market_cap': market_cap,
                    'sector': company_info.get('finnhubIndustry', 'N/A'),
                    'exchange': exchange
                }

            except Exception as e:
                if "429" in str(e):
                    logging.warning(f"从 Finnhub 获取 {symbol} 市值失败: {e}. 稍等 10 秒后重试...")
                    time.sleep(10)
                    retries += 1
                else:
                    raise e

        logging.error(f"从 Finnhub 获取 {symbol} 市值失败: 超过最大重试次数 ({max_retries}).")
        market_cap = info.get('marketCap', 0) / 100_000_000 if info.get('marketCap') else 0

        return {
            'name': info.get('longName', 'N/A'),
            'market_cap': market_cap,
            'sector': info.get('sector', 'N/A')
        }

    except Exception as e:
        logging.error(f"获取 {symbol} 详细信息时出错: {e}")
        return None

def process_stock(stock):
    symbol = stock['symbol']

    time.sleep(random.uniform(0.1, 0.3))

    if any(x in symbol for x in ['.WS', 'WS', 'W', '-W', '.W']):
        logging.info(f"跳过权证: {symbol}")
        return None

    if not symbol.isalpha() or len(symbol) > 5:
        logging.info(f"跳过非标准股票代码: {symbol}")
        return None

    try:
        ticker = yf.Ticker(symbol)

        try:
            hist = ticker.history(period="2d")
        except Exception as e:
            if "No data found for this date range" in str(e):
                logging.info(f"股票 {symbol} 可能已退市")
            else:
                logging.info(f"获取 {symbol} 历史数据时出错: {str(e)}")
            return None

        if len(hist) < 2:
            logging.info(f"股票 {symbol} 数据不足")
            return None

        current_price = hist['Close'][-1]
        previous_close = hist['Close'][-2]

        yesterday_date = hist.index[-2].strftime('%Y-%m-%d')
        today_date = hist.index[-1].strftime('%Y-%m-%d')

        if current_price <= 0 or previous_close <= 0:
            logging.info(f"股票 {symbol} 价格数据无效")
            return None

        price_change = ((current_price - previous_close) / previous_close) * 100

        details = get_stock_details(symbol)
        if details is None:
            logging.info(f"股票 {symbol} 无法获取详细信息")
            return None

        if details['market_cap'] <= 0:
            logging.info(f"股票 {symbol} 市值数据无效")
            return None

        return {
            '股票代码': symbol,
            '公司名称': details['name'],
            '市值(亿)': round(details['market_cap'], 2),
            '板块': details['sector'],
            "昨天收盘": f"{yesterday_date} {previous_close}",
            "今天收盘": f"{today_date} {current_price}",
            '涨跌幅(%)': round(price_change, 2)
        }

    except Exception as e:
        logging.info(f"处理股票 {symbol} 时出错: {str(e)}")
        return None


def get_gainers_multithreaded(max_workers=None):
    stock_symbols = load_or_fetch_symbols()
    total_symbols = len(stock_symbols)
    logging.info(f"开始处理 {total_symbols} 只股票...")

    stock_data = ThreadSafeList()

    if max_workers is None:
        max_workers = len(api_keys)

    processed_count = 0
    valid_count = 0
    progress_lock = threading.Lock()

    def process_stock_wrapper(stock):
        nonlocal processed_count, valid_count

        symbol = stock.get('symbol', '')
        result = process_stock({'symbol': symbol})

        with progress_lock:
            processed_count += 1
            if result:
                stock_data.append(result)
                valid_count += 1

            if processed_count % 10 == 0:
                logging.info(f"进度: {processed_count}/{total_symbols} "
                             f"({round(processed_count / total_symbols * 100, 2)}%) "
                             f"有效数据: {valid_count}")

        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(process_stock_wrapper, stock_symbols))

    logging.info(f"\n处理完成!")
    logging.info(f"总计处理: {processed_count} 只股票")
    logging.info(f"有效数据: {valid_count} 只股票")
    logging.info(f"无效数据: {processed_count - valid_count} 只股票")

    stock_result = stock_data.get_list()
    if not stock_result:
        logging.info("没有获取到任何有效的股票数据!")
        return pd.DataFrame()

    df = pd.DataFrame(stock_result)

    if '涨跌幅(%)' not in df.columns:
        logging.info("数据中没有涨跌幅信息!")
        return df

    df = df.sort_values(by='涨跌幅(%)', ascending=False)

    # 只在有数据时显示结果
    if not df.empty:
        display_results(df)
    else:
        logging.info("没有可显示的数据!")

    if not df.empty:
        process_final_stock_data(df)
    else:
        logging.info("没有可显示的数据!")

    return df


def display_results(df):
    if df.empty:
        logging.info("没有数据可供显示!")
        return

    # 获取日志目录和文件名
    log_dir, log_filename = setup_logging()

    # 生成结果日志文件名（使用相同的目录）
    result_log_filename = os.path.join(log_dir, f'top20_result.log')

    # 创建专门用于结果的日志记录器
    result_logger = logging.getLogger('result_logger')
    result_logger.setLevel(logging.INFO)

    # 防止日志重复
    if result_logger.handlers:
        result_logger.handlers.clear()

    # 创建文件处理器
    file_handler = logging.FileHandler(result_log_filename, encoding='utf-8')
    formatter = logging.Formatter('%(message)s')  # 简化的格式，只显示消息
    file_handler.setFormatter(formatter)
    result_logger.addHandler(file_handler)

    # 记录结果到专门的日志文件
    def log_results(message):
        result_logger.info(message)
        logging.info(message)  # 同时也记录到主日志文件

    # 1. 展示前20名涨幅股票
    top_20 = df.head(20)
    log_results("\n=== 涨幅榜前20名股票 ===")
    log_results("\n" + str(top_20[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']]))

    # 2. 展示市值超过20亿的前20名涨幅股票
    billion_20 = df[df['市值(亿)'] > 20].head(20)
    if not billion_20.empty:
        log_results("\n=== 市值超过20亿的涨幅榜前20名股票 ===")
        log_results(
            "\n" + str(billion_20[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']]))
    else:
        log_results("\n没有市值超过20亿的股票!")

    # 3. 展示市值超过50亿的前20名涨幅股票
    billion_50 = df[df['市值(亿)'] > 50].head(20)
    if not billion_50.empty:
        log_results("\n=== 市值超过50亿的涨幅榜前20名股票 ===")
        log_results(
            "\n" + str(billion_50[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']]))
    else:
        log_results("\n没有市值超过50亿的股票!")

    # 4. 展示市值超过100亿的前20名涨幅股票
    billion_100 = df[df['市值(亿)'] > 100].head(20)
    if not billion_100.empty:
        log_results("\n=== 市值超过100亿的涨幅榜前20名股票 ===")
        log_results(
            "\n" + str(billion_100[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']]))
    else:
        log_results("\n没有市值超过100亿的股票!")

    # 5. 展示市值超过200亿的前20名涨幅股票
    billion_200 = df[df['市值(亿)'] > 200].head(20)
    if not billion_200.empty:
        log_results("\n=== 市值超过200亿的涨幅榜前20名股票 ===")
        log_results(
            "\n" + str(billion_200[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']]))
    else:
        log_results("\n没有市值超过200亿的股票!")

    # 6. 展示市值超过1000亿的前20名涨幅股票
    billion_1000 = df[df['市值(亿)'] > 1000].head(20)
    if not billion_1000.empty:
        log_results("\n=== 市值超过1000亿的涨幅榜前20名股票 ===")
        log_results(
            "\n" + str(billion_1000[['股票代码', '公司名称', '市值(亿)', '板块', '昨天收盘', '今天收盘', '涨跌幅(%)']]))
    else:
        log_results("\n没有市值超过1000亿的股票!")

    # 关闭文件处理器
    file_handler.close()
    result_logger.removeHandler(file_handler)
    logging.info(f"结果已保存到: {result_log_filename}")


def process_final_stock_data(df):
    # 获取日志目录和文件名
    log_dir, log_filename = setup_logging()

    # 生成带时间戳的结果文件名（使用相同的目录）
    timestamp = os.path.basename(log_filename)[:-4]

    output_filename = os.path.join(log_dir, 'stocks_data.json')
    sector_analysis_filename = os.path.join(log_dir, 'sector_analysis.json')

    stocks_data = df.to_dict('records')

    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(stocks_data, f, ensure_ascii=False, indent=2)

    logging.info(f"股票数据已保存到 {output_filename}")

    sector_performance = df.groupby('板块')['涨跌幅(%)'].agg(['mean', 'count']).reset_index()
    sector_performance = sector_performance.rename(columns={
        'mean': '平均涨跌幅',
        'count': '股票数量'
    })
    sector_performance = sector_performance.sort_values('平均涨跌幅', ascending=False)

    logging.info("\n=== 板块涨跌分析 ===")
    logging.info("\n" + str(sector_performance))

    with open(sector_analysis_filename, 'w', encoding='utf-8') as f:
        json.dump(sector_performance.to_dict('records'), f, ensure_ascii=False, indent=2)

    logging.info(f"板块分析已保存到 {sector_analysis_filename}")

    return sector_performance


def run_git_update():
    try:
        subprocess.run(['chmod', '+x', 'update_git.sh'], check=True)

        result = subprocess.run(
            ['./update_git.sh'],
            shell=True,
            stdout=subprocess.PIPE,  # 替换 capture_output
            stderr=subprocess.PIPE,  # 替换 capture_output
            universal_newlines=True,  # 替换 text=True
            check=True
        )
        logging.info("Git更新成功完成")
        logging.debug(f"脚本输出: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git更新脚本执行失败: {e}")
        logging.error(f"错误输出: {e.stderr}")
    except FileNotFoundError:
        logging.error("未找到update_git.sh脚本文件")
    except Exception as e:
        logging.error(f"执行git更新时发生未知错误: {str(e)}")


if __name__ == "__main__":
    gainers_df = get_gainers_multithreaded()

    if not gainers_df.empty:
        run_git_update()

