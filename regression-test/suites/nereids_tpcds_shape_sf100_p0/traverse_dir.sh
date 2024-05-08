#!/bin/bash

# 检查是否提供了目录路径作为参数
if [ $# -eq 0 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

# 目录路径
directory="$1"

# 检查目录是否存在
if [ ! -d "$directory" ]; then
    echo "Directory '$directory' does not exist."
    exit 1
fi

# 遍历目录下的所有文件
for file in "$directory"/*; do
    # 忽略子目录
    if [ -f "$file" ]; then
        # 执行操作
        echo "Processing file: $file"
        # 在这里执行对单个文件的操作
        bash code3.sh "$file"  # 调用之前的脚本对单个文件执行操作
    fi
done

