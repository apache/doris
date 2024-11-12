#!/bin/bash

# 指定要遍历的文件目录
file=$1

# 遍历 .h 和 .cpp 文件
# 检查是否已经包含 compile_check_begin.h
if grep -q '#include "common/compile_check_begin.h"' "$file"; then
    echo "Skipping $file: already contains compile_check_begin.h."
    continue
fi

# 检查是否已经包含 compile_check_end.h (仅在 .h 文件中)
if [[ "$file" == *.h ]] && grep -q '#include "common/compile_check_end.h"' "$file"; then
    echo "Skipping $file: already contains compile_check_end.h."
    continue
fi

# 查找第一个 namespace 的行号
namespace_line=$(grep -n '^namespace' "$file" | head -n 1 | cut -d: -f1)

if [ -z "$namespace_line" ]; then
    echo "Skipping $file: no namespace found."
    continue
fi

# 在第一个 namespace 下插入 compile_check_begin.h
sed -i "${namespace_line}a #include \"common/compile_check_begin.h\"" "$file"

# 如果是 .h 文件，在文件末尾插入 compile_check_end.h
if [[ "$file" == *.h ]]; then
    # 在末尾插入时确保前面有一个换行
    echo -e "\n#include \"common/compile_check_end.h\"" >> "$file"
fi

echo "Processed $file."

