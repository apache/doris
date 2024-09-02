#!/bin/bash

# 指定包含 CSV 文件名的文件
csv_name_file="csv_name"

# API 终端配置
base_url="http://127.0.0.1:10721/api/regression_test_inverted_index_p2/ZfzGu/_stream_load"
username="root"
password=""  # 如果没有密码，可以留空
column_separator=","
format="csv"

# 从 csv_name 文件中逐行读取 CSV 文件名
while IFS= read -r csv_file; do
    # 检查文件是否存在
    if [[ -f "$csv_file" ]]; then
        echo "Uploading $csv_file..."
        
        # 使用 curl 命令上传 CSV 文件
        curl --location-trusted -u "$username:$password" \
            -H "column_separator:$column_separator" \
            -H "format:$format" \
            -T "$csv_file" \
            "$base_url"
        
        echo "Uploaded $csv_file"
    else
        echo "File not found: $csv_file"
    fi
done < "$csv_name_file"

