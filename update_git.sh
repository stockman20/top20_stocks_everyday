#!/bin/bash

# 进入 GitHub Actions 运行的代码仓库目录
cd "$GITHUB_WORKSPACE" || { echo "Error: 目录不存在"; exit 1; }

# 设置 Git 用户（从 GitHub Secrets 读取）
git config --global user.name "$GIT_USERNAME"
git config --global user.email "$GIT_EMAIL"

# 拉取最新代码
git stash
git pull origin master --rebase
git stash pop || echo "No changes to pop"

# 提交 & 推送
git add .
CURRENT_TIME=$(date "+%Y-%m-%d %H:%M:%S")
git commit -m "Auto update at $CURRENT_TIME" || echo "No changes to commit"

# GitHub Actions 推送（使用 GITHUB_TOKEN 认证）
git push https://x-access-token:$GITHUB_TOKEN@github.com/stockman20/top20_stocks_everyday.git HEAD:test_git_flow
