#!/bin/bash

# 指定仓库目录
REPO_DIR="/root/top20_stocks_everyday"

# 切换到指定目录
cd "$REPO_DIR"

# 获取当前时间
CURRENT_TIME=$(date "+%Y-%m-%d %H:%M:%S")

# 添加所有修改
git add .

# 使用当前时间作为提交信息
git commit -m "Commit at $CURRENT_TIME"

# 拉取并变基
git pull -r

# 推送到master分支
git push origin master
