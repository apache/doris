#!/bin/bash

# 检查是否提供了文件路径作为参数
if [ $# -eq 0 ]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi

# 输入文件路径
input_file="$1"

# 检查文件是否存在
if [ ! -f "$input_file" ]; then
    echo "File '$input_file' does not exist."
    exit 1
fi

# 检查文件是否已经包含需要添加的行
if grep -q "sql \"set disable_nereids_rules=PRUNE_EMPTY_PARTITION\"" "$input_file"; then
    echo "Line already exists. Skipping addition."
else
    # 使用awk命令在文件中查找并添加行
    awk '/sql '\''SET enable_pipeline_engine = true'\''/ { print; print "    sql \"set disable_nereids_rules=PRUNE_EMPTY_PARTITION\""; next } 1' "$input_file" > temp_file && mv temp_file "$input_file"
    echo "Line added successfully."
fi

