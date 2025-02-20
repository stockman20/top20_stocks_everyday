[Project Repository - GitHub](https://github.com/stockman20/top20_stocks_everyday)

- [项目介绍 (项目概述)](#项目介绍-项目概述)
- [功能特点](#功能特点)
- [安装与运行](#安装与运行)
- [GitHub Actions 自动化](#github-actions-自动化)
- [结果存储](#结果存储-1)
- [贡献](#贡献)
- [许可证](#许可证)

## 项目介绍 (Project Overview)

本项目旨在每日获取并分析美股市场表现最佳的 20 只股票，并将结果存储到日志文件中，同时自动更新 Git 仓库，并向 Telegram 发送通知。

This project aims to retrieve and analyze the top 20 performing stocks in the US market daily, storing the results in log files, updating the Git repository, and sending a notification to Telegram.

---

## 功能特点 (Features)

- **股票筛选 (Stock Filtering)**: 通过 `Finnhub` API 获取股票数据，并应用筛选规则。
- **数据获取 (Data Retrieval)**: 使用 `yfinance` 获取股票的历史价格。
- **多线程处理 (Multithreading Processing)**: 提高数据处理速度。
- **速率限制 (Rate Limiting)**: 确保对 `Finnhub` 和 `yfinance` 的请求不会超出 API 限制。
- **日志记录 (Logging)**: 详细记录执行过程和结果。
- **GitHub Actions 自动化 (GitHub Actions Automation)**: 每天自动运行脚本，并提交最新结果到 GitHub。
- **Telegram 通知 (Telegram Notification)**: 运行结束后自动发送最新 `top20_result.log` 文件的 URL 到 Telegram。

---

## 安装与运行 (Installation & Execution)

### 1. 克隆项目 (Clone the repository)
```sh
$ git clone https://github.com/stockman20/top20_stocks_everyday.git
$ cd top20_stocks_everyday
```

### 2. 安装依赖 (Install dependencies)
```sh
$ pip install -r requirements.txt
```

### 3. 配置 API Keys (Configure API Keys)
请确保在运行前设置 `Finnhub` API Key，并以 JSON 格式存储在环境变量 `API_KEYS_JSON` 中。例如：

Ensure you set up your `Finnhub` API Key in the environment variable `API_KEYS_JSON` in JSON format:
```sh
export API_KEYS_JSON='["your_api_key1", "your_api_key2"]'
```

### 4. 运行程序 (Run the script)
```sh
$ python top20.py
```

---

## GitHub Actions 自动化 (GitHub Actions Automation)

本项目使用 GitHub Actions 自动执行以下任务：
- 每天美股交易结束后 (UTC 23:00) 运行 `top20.py`。
- 运行完成后，执行 `update_git.sh`，将最新结果推送到 GitHub。
- 更新完成后，自动发送 `top20_result.log` 的 GitHub URL 到 Telegram。

### 配置 GitHub Secrets (Configure GitHub Secrets)
请在 GitHub 仓库的 `Settings -> Secrets and variables -> Actions` 页面添加以下 Secrets：
- `SECRET_API_KEYS` (存储 `Finnhub` API Keys，JSON 格式)
- `GITHUB_TOKEN` (用于 GitHub 提交权限)
- `TELEGRAM_TOKEN` (Telegram Bot Token)
- `TELEGRAM_CHAT_IDS` (Telegram 接收通知的 Chat ID，多个 ID 用 `,` 分隔)

### 手动触发 (Manual Trigger)
可以通过 GitHub Actions 的 `workflow_dispatch` 选项手动运行该工作流。

---

## 结果存储 (Result Storage)

每次运行后，以下文件将被更新并存储：
- `logs/YYYYMMDD_HHMMSS/top20_result.log` (当日 Top 20 股票结果)
- `logs/YYYYMMDD_HHMMSS/execution.log` (执行日志)
- `non_us_stocks.json` (非美股股票缓存文件)
- `excluded_stocks.json` (被排除的股票缓存文件)
- `valid_stocks.json` (有效股票缓存文件)

这些缓存文件用于减少 API 请求，提高运行效率。

---

## 贡献 (Contributing)
欢迎提交 Issue 或 Pull Request 以改进本项目。

Feel free to submit an issue or pull request to improve this project!

---

## 许可证 (License)
本项目采用 MIT 许可证。

This project is licensed under the MIT License.

