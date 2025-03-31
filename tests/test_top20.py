import unittest
import json
import time
import pandas as pd
import pytz
import threading
import os
import logging
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime, timedelta

# Mock finnhub客户端创建
def mock_get_finnhub_client():
    return MagicMock()

# 在所有测试前设置mock环境变量
os.environ['API_KEYS_JSON'] = '["test_key1", "test_key2"]'
from top20 import (
    RateLimiter,
    APIKeyRotator,
    is_market_open,
    ThreadSafeList,
    should_include_symbol,
    process_stock,
    get_gainers_multithreaded
)

class TestRateLimiter(unittest.TestCase):
    def setUp(self):
        self.rate_limiter = RateLimiter(max_calls=2, period=1)  # 2 calls per second for testing
    
    def test_rate_limiting(self):
        start_time = time.time()
        for _ in range(3):  # Should trigger rate limiting on 3rd call
            self.rate_limiter.acquire()
        elapsed = time.time() - start_time
        self.assertGreaterEqual(elapsed, 1.0)  # Should take at least 1 second

class TestAPIKeyRotator(unittest.TestCase):
    def test_key_rotation(self):
        rotator = APIKeyRotator(["test_key1", "test_key2"])
        # Test key rotation sequence
        self.assertEqual(rotator.get_next_key(), 'test_key1')
        self.assertEqual(rotator.get_next_key(), 'test_key2')
        self.assertEqual(rotator.get_next_key(), 'test_key1')  # Should wrap around

class TestMarketStatus(unittest.TestCase):
    @patch('top20.holidays.US', return_value={}) # Mock holidays to be empty
    @patch('top20.datetime')
    def test_is_market_open_weekday(self, mock_datetime, mock_holidays):
        # Mock a non-holiday weekday (Tuesday, Jan 3, 2023)
        mock_now_et = MagicMock()
        mock_now_et.date.return_value = datetime(2023, 1, 3).date()
        mock_datetime.now.return_value = mock_now_et # Simulate datetime.now(tz) returning our mock
        self.assertTrue(is_market_open())

    @patch('top20.holidays.US', return_value={}) # Mock holidays to be empty
    @patch('top20.datetime')
    def test_is_market_open_weekend(self, mock_datetime, mock_holidays):
        # Mock a Saturday
        mock_now_et = MagicMock()
        mock_now_et.date.return_value = datetime(2023, 1, 7).date() # Saturday
        mock_datetime.now.return_value = mock_now_et
        self.assertFalse(is_market_open())

class TestThreadSafeList(unittest.TestCase):
    def test_thread_safety(self):
        ts_list = ThreadSafeList()
        def append_items(start, end):
            for i in range(start, end):
                ts_list.append(i)
        
        threads = []
        for i in range(0, 100, 10):
            t = threading.Thread(target=append_items, args=(i, i+10))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        result = ts_list.get_list()
        self.assertEqual(len(result), 100)
        self.assertEqual(sorted(result), list(range(100)))

class TestStockFiltering(unittest.TestCase):
    def setUp(self):
        self.filters = {
            'non_us_stocks': {'BABA': {'exchange': 'NYSE', 'name': 'Alibaba'}},
            'excluded_stocks': {'TSLA'},
            'valid_stocks': {'AAPL', 'MSFT'}
        }
    
    def test_should_include_symbol_valid(self):
        stock_data = {'symbol': 'AAPL', 'type': 'Common Stock'}
        result, reason = should_include_symbol(stock_data, self.filters)
        self.assertTrue(result)
        self.assertEqual(reason, "通过")
    
    def test_should_include_symbol_non_us(self):
        stock_data = {'symbol': 'BABA', 'type': 'Common Stock'}
        result, reason = should_include_symbol(stock_data, self.filters)
        self.assertFalse(result)
        self.assertEqual(reason, "非美股")
    
    def test_should_include_symbol_excluded(self):
        stock_data = {'symbol': 'TSLA', 'type': 'Common Stock'}
        result, reason = should_include_symbol(stock_data, self.filters)
        self.assertFalse(result)
        self.assertEqual(reason, "在排除列表中")

class TestStockProcessing(unittest.TestCase):
    @patch('top20.get_stock_details')
    @patch('top20.yf.Ticker')
    def test_process_stock_success(self, mock_yfinance, mock_get_details):
        dates = [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02')]
        
        # Setup mock yfinance response
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame({
            'Close': [100, 105]
        }, index=dates)
        mock_yfinance.return_value = mock_ticker

        # Setup mock stock details response
        mock_get_details.return_value = {
            'name': 'Apple Inc',
            'market_cap': 10000,  # 1 Trillion / 100
            'sector': 'Technology',
            'exchange': 'NASDAQ'
        }

        result = process_stock({'symbol': 'AAPL'})
        self.assertIsNotNone(result, "process_stock returned None. Check logs for errors.")
        self.assertEqual(result['股票代码'], 'AAPL')
        self.assertEqual(result['公司名称'], 'Apple Inc')
        self.assertEqual(result['市值(亿)'], 10000)
        self.assertEqual(result['板块'], 'Technology')
        self.assertEqual(result['昨天收盘'], '2023-01-01 100')
        self.assertEqual(result['今天收盘'], '2023-01-02 105')
        self.assertEqual(result['涨跌幅(%)'], 5.0)
    
    @patch('top20.yf.Ticker')
    def test_process_stock_no_data(self, mock_yfinance):
        mock_ticker = MagicMock()
        mock_ticker.history.side_effect = Exception("No data found")
        mock_yfinance.return_value = mock_ticker
        
        result = process_stock({'symbol': 'INVALID'})
        self.assertIsNone(result)

class TestIntegration(unittest.TestCase):
    @patch('top20.get_stock_details')
    @patch('top20.get_finnhub_client')
    @patch('top20.yf.Ticker')
    def test_get_gainers_multithreaded(self, mock_yfinance, mock_finnhub, mock_get_details):
        # Setup mock symbols
        symbols = [
            {'symbol': 'AAPL', 'type': 'Common Stock'},
            {'symbol': 'MSFT', 'type': 'Common Stock'},
            {'symbol': 'GOOGL', 'type': 'Common Stock'}
        ]
        
        # Setup mock yfinance responses
        # Setup yfinance mock to return different tickers for different symbols
        # Setup yfinance mock to return different tickers for different symbols
        dates = [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02')]
        prices = {
            'AAPL': [100, 105],
            'MSFT': [200, 210],
            'GOOGL': [300, 315]
        }

        # Create a lock for thread-safe logging
        log_lock = threading.Lock()

        def debug_log(msg):
            with log_lock:
                logging.info(msg)

        def get_mock_ticker(symbol_dict):
            if isinstance(symbol_dict, dict):
                symbol = symbol_dict.get('symbol')
            else:
                symbol = symbol_dict
            debug_log(f"Creating mock ticker for symbol: {symbol}")
            
            if symbol not in prices:
                debug_log(f"Symbol not found in test data: {symbol}")
                return None

            mock_ticker = MagicMock()
            mock_ticker._symbol = symbol
            
            def mock_history(period=None, **kwargs):
                debug_log(f"Mock history called for {symbol} with period={period}, kwargs={kwargs}")
                df = pd.DataFrame({
                    'Close': prices[symbol]
                }, index=dates)
                debug_log(f"Returning data for {symbol}: {df}")
                return df

            mock_ticker.history = MagicMock(side_effect=mock_history)
            return mock_ticker

        mock_yfinance.side_effect = get_mock_ticker

        # Setup get_stock_details mock with logging
        def mock_get_details(symbol_dict):
            if isinstance(symbol_dict, dict):
                symbol = symbol_dict.get('symbol')
            else:
                symbol = symbol_dict

            debug_log(f"Getting stock details for {symbol}")
            
            details = {
                'name': f'{symbol} Company',
                'market_cap': 1000,
                'sector': 'Technology',
                'exchange': 'NASDAQ'
            }
            debug_log(f"Returning details for {symbol}: {details}")
            return details

        mock_get_details.side_effect = mock_get_details

        # Test multithreaded processing with detailed logging
        with self.assertLogs(level='INFO') as log:
            debug_log("Starting multithreaded processing")
            df, invalid = get_gainers_multithreaded(symbols, max_workers=2)
            debug_log(f"Processing complete - df size: {len(df)}, invalid: {invalid}")
            
            # Print all captured logs if the test fails
            if len(df) != 3:
                print("\nTest execution logs:")
                for entry in log.output:
                    print(entry)
                print(f"\nInvalid stocks: {invalid}")
                
        self.assertEqual(len(df), 3, f"Expected 3 stocks but got {len(df)}")
        self.assertEqual(len(invalid), 0)
        self.assertEqual(df['涨跌幅(%)'].tolist(), [5.0, 5.0, 5.0])

if __name__ == '__main__':
    unittest.main()