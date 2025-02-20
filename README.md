- **中文版本**: [README_CN.md](README_CN.md)
---

# Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Installation & Execution](#installation--execution)
- [GitHub Actions Automation](#github-actions-automation)
- [Result Storage](#result-storage)
- [Contributing](#contributing)
- [License](#license)

---

# Project Overview

This project aims to retrieve and analyze the top 20 performing stocks in the US market daily. It stores the results in log files, updates the Git repository, and sends notifications via Telegram.

---

## Features

- **Stock Filtering**: Retrieves stock data using the `Finnhub` API and applies filtering rules.
- **Data Retrieval**: Uses `yfinance` to obtain historical stock prices.
- **Multithreading Processing**: Enhances data processing speed.
- **Rate Limiting**: Ensures that requests to `Finnhub` and `yfinance` do not exceed API limits.
- **Logging**: Detailed logging of execution process and results.
- **GitHub Actions Automation**: Automatically runs the script daily and commits the latest results to GitHub.
- **Telegram Notification**: Sends the URL of the latest `top20_result.log` file to Telegram upon completion.

---

## Installation & Execution

### 1. Clone the Repository
```sh
$ git clone https://github.com/stockman20/top20_stocks_everyday.git
$ cd top20_stocks_everyday
```

### 2. Install Dependencies
```sh
$ pip install -r requirements.txt
```

### 3. Configure API Keys
Make sure to set the `Finnhub` API Key in the environment variable `API_KEYS_JSON` in JSON format. For example:
```sh
export API_KEYS_JSON='["your_api_key1", "your_api_key2"]'
```

### 4. Run the Script
```sh
$ python top20.py
```

---

## GitHub Actions Automation

This project utilizes GitHub Actions to perform the following tasks:
- Runs `top20.py` daily after the US market closes (UTC 23:00).
- Upon completion, executes `update_git.sh` to push the latest results to GitHub.
- Automatically sends the GitHub URL of `top20_result.log` to Telegram after updating.

### Configure GitHub Secrets
In your GitHub repository under `Settings -> Secrets and variables -> Actions`, add the following secrets:
- `SECRET_API_KEYS` (stores the `Finnhub` API Keys in JSON format)
- `GITHUB_TOKEN` (for GitHub commit permissions)
- `TELEGRAM_TOKEN` (Telegram Bot Token)
- `TELEGRAM_CHAT_IDS` (Telegram Chat IDs to receive notifications; separate multiple IDs with commas)

### Manual Trigger
You can manually run the workflow via GitHub Actions' `workflow_dispatch` option.

---

## Result Storage

After each run, the following files will be updated and stored:
- `logs/YYYYMMDD_HHMMSS/top20_result.log` (Today's Top 20 stocks result)
- `logs/YYYYMMDD_HHMMSS/execution.log` (Execution log)
- `non_us_stocks.json` (Cache file for non-US stocks)
- `excluded_stocks.json` (Cache file for excluded stocks)
- `valid_stocks.json` (Cache file for valid stocks)

These cache files help reduce API requests and improve execution efficiency.

---

## Contributing

Feel free to submit an issue or pull request to improve this project!

---

## License

This project is licensed under the MIT License.
