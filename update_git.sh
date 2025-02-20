#!/bin/bash

# 进入 GitHub Actions 运行的代码仓库目录
cd "$GITHUB_WORKSPACE" || { echo "❌ Error: 目录不存在"; exit 1; }

# Debug 输出 Git 变量
echo "🔍 GIT_USERNAME: $GIT_USERNAME"
echo "🔍 GIT_EMAIL: $GIT_EMAIL"

# 设置 Git 用户信息（如果变量为空，则使用默认值）
git config --global user.name "${GIT_USERNAME:-github-actions}"
git config --global user.email "${GIT_EMAIL:-github-actions@github.com}"

git config --global --list  # 检查 Git 配置是否正确

# **先确保所有修改都提交，避免 rebase 失败**
git add -A
# **提交 & 推送**
CURRENT_TIME=$(date "+%Y-%m-%d %H:%M:%S")
git commit -m "Auto update at $CURRENT_TIME" || echo "No changes to commit"

# **拉取最新代码**
git pull origin master --rebase || { echo "❌ git pull 失败"; exit 1; }

# **使用 GitHub Actions Token 进行身份验证**
git push https://x-access-token:$GITHUB_TOKEN@github.com/stockman20/top20_stocks_everyday.git HEAD:master || { echo "❌ git push 失败"; exit 1; }
