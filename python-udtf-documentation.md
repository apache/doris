# Python UDTF

## 功能概述

Python UDTF (User Defined Table Function) 是 Apache Doris 提供的自定义表函数扩展机制，允许用户使用 Python 语言编写自定义表函数，用于将单行数据转换为多行输出。通过 Python UDTF，用户可以灵活地实现数据拆分、展开、生成等复杂逻辑。

Python UDTF 的核心特点:
- **一行转多行**: 接收单行输入，输出零行、一行或多行结果
- **灵活的输出结构**: 可以定义任意数量和类型的输出列，支持简单类型和复杂 STRUCT 类型
- **侧视图支持**: 配合 `LATERAL VIEW` 使用，实现数据展开和关联
- **函数式编程**: 使用 Python 函数和 `yield` 语句，简洁直观
- **无状态处理**: 每行数据独立处理，通过 `yield` 产出结果

> **环境依赖**: 使用 Python UDTF 前，必须在所有 BE 节点的 Python 环境中预先安装 **`pandas`** 和 **`pyarrow`** 两个库，这是 Doris Python UDTF 功能的强制依赖。详见 [Python UDF 环境配置](python-udf-environment.md)。

## 使用场景

Python UDTF 适用于以下场景:

1. **字符串分割**: 将分隔符分隔的字符串拆分为多行
2. **数组展开**: 将数组或列表类型的数据展开为多行
3. **JSON 解析**: 解析 JSON 数组或对象，转换为多行结构化数据
4. **数据生成**: 根据输入参数生成多行数据，如生成数字序列、日期范围等
5. **笛卡尔积**: 展开多个列表，生成组合结果
6. **数据过滤**: 根据条件过滤输出，跳过不符合条件的行
7. **数据复制**: 将单行数据复制为多行
8. **数据转换**: 将一行数据转换为多种格式输出

## UDTF 基本概念

### 表函数的执行方式

Python UDTF 通过**函数**实现（而非类），函数的执行流程如下:

1. **接收输入**: 函数接收单行数据的各列值作为参数
2. **处理与产出**: 通过 `yield` 语句产出零行或多行结果
3. **无状态**: 每次函数调用独立处理一行，不保留上一行的状态

### 函数要求

Python UDTF 函数必须满足以下要求:

- **使用 def 定义**: 必须是普通函数，不能是类方法
- **使用 yield 产出结果**: 通过 `yield` 语句产出输出行
- **参数类型对应**: 函数参数与 SQL 中定义的参数类型对应
- **输出格式匹配**: `yield` 的数据格式必须与 `RETURNS ARRAY<...>` 定义一致

### 输出方式

- **单列输出**: `yield (value,)` 产出单个值（注意元组格式）
- **多列输出**: `yield (value1, value2, ...)` 产出多个值的元组
- **STRUCT 输出**: `yield (value1, value2, ...)` 产出结构体，列名由 STRUCT 定义
- **条件跳过**: 不调用 `yield`，该行不产生任何输出

## 基本语法

### 创建 Python UDTF

Python UDTF 支持两种创建方式:内联模式 (Inline) 和模块模式 (Module)。

> **注意**: 如果同时指定了 `file` 参数和 `AS $$` 内联 Python 代码，Doris 将会**优先加载内联 Python 代码**，采用内联模式运行 Python UDTF。

#### 内联模式 (Inline Mode)

内联模式允许直接在 SQL 中编写 Python 函数，适合简单的表函数逻辑。

**语法**:
```sql
CREATE TABLES FUNCTION function_name(parameter_type1, parameter_type2, ...)
RETURNS ARRAY<return_type>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "function_name",
    "runtime_version" = "python_version",
    "always_nullable" = "true|false"
)
AS $$
def function_name(param1, param2, ...):
    '''函数说明'''
    # 处理逻辑
    yield (result,)  # 单列输出
    # 或
    yield (result1, result2, ...)  # 多列输出
$$;
```

> **重要语法说明**:
> - 使用 `CREATE TABLES FUNCTION`（注意是 **TABLES**，复数形式）
> - 返回类型是 `RETURNS ARRAY<...>`，而不是 `RETURNS TABLE<...>`
> - 单列输出: `ARRAY<类型>`，如 `ARRAY<INT>`
> - 多列输出: `ARRAY<STRUCT<col1:type1, col2:type2, ...>>`

**示例 1: 字符串分割（单列输出）**
```sql
CREATE TABLES FUNCTION py_split(STRING, STRING)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "split_string_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def split_string_udtf(text, delimiter):
    '''将字符串按分隔符分割为多行'''
    if text is not None and delimiter is not None:
        parts = text.split(delimiter)
        for part in parts:
            yield (part.strip(),)  # 注意: 单列输出需要元组形式
$$;

SELECT part
FROM (SELECT 'apple,banana,orange' as fruits) t
LATERAL VIEW py_split(fruits, ',') tmp AS part;
-- 结果:
-- apple
-- banana
-- orange
```

**示例 2: 生成数字序列（单列输出）**
```sql
CREATE TABLES FUNCTION py_range(INT, INT)
RETURNS ARRAY<INT>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "generate_series_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def generate_series_udtf(start, end):
    '''生成从 start 到 end 的整数序列'''
    if start is not None and end is not None:
        for i in range(start, end + 1):
            yield (i,)
$$;

SELECT num
FROM (SELECT py_range(1, 5)) tmp(num);
-- 结果: 1, 2, 3, 4, 5

-- 生成日期序列
SELECT date_add('2024-01-01', n) as date
FROM (SELECT py_range(0, 6)) tmp(n);
-- 结果: 2024-01-01 到 2024-01-07
```

**示例 3: 多列输出（STRUCT）**
```sql
CREATE TABLES FUNCTION py_duplicate(STRING, INT)
RETURNS ARRAY<STRUCT<output:STRING, idx:INT>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "duplicate_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def duplicate_udtf(text, n):
    '''将文本复制 n 次，每次带序号'''
    if text is not None and n is not None:
        for i in range(n):
            yield (text, i + 1)  # 多列输出，不需要嵌套元组
$$;

SELECT output, idx
FROM (SELECT 'Hello' as text, 3 as times) t
LATERAL VIEW py_duplicate(text, times) tmp AS output, idx;
-- 结果:
-- Hello, 1
-- Hello, 2
-- Hello, 3
```

**示例 4: 笛卡尔积（多列 STRUCT）**
```sql
CREATE TABLES FUNCTION py_cartesian(STRING, STRING)
RETURNS ARRAY<STRUCT<item1:STRING, item2:STRING>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "cartesian_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def cartesian_udtf(list1, list2):
    '''生成两个列表的笛卡尔积'''
    if list1 is not None and list2 is not None:
        items1 = [x.strip() for x in list1.split(',')]
        items2 = [y.strip() for y in list2.split(',')]
        for x in items1:
            for y in items2:
                yield (x, y)
$$;

SELECT item1, item2
FROM (SELECT 'A,B' as list1, 'X,Y,Z' as list2) t
LATERAL VIEW py_cartesian(list1, list2) tmp AS item1, item2;
-- 结果:
-- A, X
-- A, Y
-- A, Z
-- B, X
-- B, Y
-- B, Z
```

**示例 5: 条件过滤（跳过不符合条件的行）**
```sql
CREATE TABLES FUNCTION py_filter_positive(INT)
RETURNS ARRAY<INT>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "filter_positive_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def filter_positive_udtf(value):
    '''只输出正数'''
    if value is not None and value > 0:
        yield (value,)
    # 如果值 <= 0，不调用 yield，跳过该行
$$;

CREATE TABLE mixed_numbers (id INT, value INT);
INSERT INTO mixed_numbers VALUES (1, -5), (2, 0), (3, 3), (4, -2), (5, 7);

SELECT positive_value
FROM mixed_numbers
LATERAL VIEW py_filter_positive(value) tmp AS positive_value;
-- 结果: 3, 7
```

**示例 6: JSON 数组解析**
```sql
CREATE TABLES FUNCTION py_explode_json(STRING)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "explode_json_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
import json

def explode_json_udtf(json_str):
    '''解析 JSON 数组，每个元素输出一行'''
    if json_str is not None:
        try:
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    yield (str(item),)
        except:
            pass  # 解析失败则跳过
$$;

SELECT element
FROM (SELECT '["apple", "banana", "cherry"]' as json_data) t
LATERAL VIEW py_explode_json(json_data) tmp AS element;
-- 结果:
-- apple
-- banana
-- cherry
```

#### 模块模式 (Module Mode)

模块模式适合复杂的表函数逻辑，需要将 Python 代码打包成 `.zip` 压缩包，并在函数创建时引用。

**步骤 1: 编写 Python 模块**

创建 `text_udtf.py` 文件:

```python
import json
import re

def split_lines_udtf(text):
    """按行分割文本"""
    if text:
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if line:  # 过滤空行
                yield (line,)


def extract_emails_udtf(text):
    """提取文本中的所有邮箱地址"""
    if text:
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = re.findall(email_pattern, text)
        for email in emails:
            yield (email,)


def parse_json_object_udtf(json_str):
    """解析 JSON 对象，输出键值对"""
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                for key, value in data.items():
                    yield (key, str(value))
        except:
            pass


def expand_json_array_udtf(json_str):
    """展开 JSON 数组中的对象，输出结构化数据"""
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        # 假设每个对象都有 id, name, score 字段
                        item_id = item.get('id')
                        name = item.get('name')
                        score = item.get('score')
                        yield (item_id, name, score)
        except:
            pass


def ngram_udtf(text, n):
    """生成 N-gram 词组"""
    if text and n and n > 0:
        words = text.split()
        for i in range(len(words) - n + 1):
            ngram = ' '.join(words[i:i+n])
            yield (ngram,)
```

**步骤 2: 打包 Python 模块**

**必须**将 Python 文件打包成 `.zip` 格式(即使只有单个文件):
```bash
zip text_udtf.zip text_udtf.py
```

**步骤 3: 设置 Python 模块压缩包的路径**

支持多种部署方式，通过 `file` 参数指定 `.zip` 包的路径:

**方式 1: 本地文件系统** (使用 `file://` 协议)
```sql
"file" = "file:///path/to/text_udtf.zip"
```

**方式 2: HTTP/HTTPS 远程下载** (使用 `http://` 或 `https://` 协议)
```sql
"file" = "http://example.com/udtf/text_udtf.zip"
"file" = "https://s3.amazonaws.com/bucket/text_udtf.zip"
```

> **注意**: 
> - 使用远程下载方式时，需确保所有 BE 节点都能访问该 URL
> - 首次调用时会下载文件，可能有一定延迟
> - 文件会被缓存，后续调用无需重复下载

**步骤 4: 设置 symbol 参数**

在模块模式下，`symbol` 参数用于指定函数在 ZIP 包中的位置，格式为:

```
[package_name.]module_name.function_name
```

**参数说明**:
- `package_name`(可选): ZIP 压缩包内顶层 Python 包的名称
- `module_name`(必填): 包含目标函数的 Python 模块文件名(不含 `.py` 后缀)
- `function_name`(必填): UDTF 函数名

**解析规则**:
- Doris 会将 `symbol` 字符串按 `.` 分割:
  - 如果得到**两个**子字符串，分别为 `module_name` 和 `function_name`
  - 如果得到**三个及以上**的子字符串，开头为 `package_name`，中间为 `module_name`，结尾为 `function_name`

**步骤 5: 创建 UDTF 函数**

```sql
CREATE TABLES FUNCTION py_split_lines(STRING)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/text_udtf.zip",
    "symbol" = "text_udtf.split_lines_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE TABLES FUNCTION py_extract_emails(STRING)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/text_udtf.zip",
    "symbol" = "text_udtf.extract_emails_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE TABLES FUNCTION py_parse_json(STRING)
RETURNS ARRAY<STRUCT<key:STRING, value:STRING>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/text_udtf.zip",
    "symbol" = "text_udtf.parse_json_object_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE TABLES FUNCTION py_expand_json(STRING)
RETURNS ARRAY<STRUCT<id:INT, name:STRING, score:DOUBLE>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/text_udtf.zip",
    "symbol" = "text_udtf.expand_json_array_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE TABLES FUNCTION py_ngram(STRING, INT)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/text_udtf.zip",
    "symbol" = "text_udtf.ngram_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);
```

**步骤 6: 使用函数**

```sql
-- 按行分割文本
SELECT line
FROM (SELECT 'Line 1\nLine 2\nLine 3' as text) t
LATERAL VIEW py_split_lines(text) tmp AS line;
-- 结果:
-- Line 1
-- Line 2
-- Line 3

SELECT email
FROM documents
LATERAL VIEW py_extract_emails(content) tmp AS email;

SELECT key, value
FROM (SELECT '{"name": "Alice", "age": "25"}' as json_data) t
LATERAL VIEW py_parse_json(json_data) tmp AS key, value;
-- 结果:
-- name, Alice
-- age, 25

SELECT id, name, score
FROM (
    SELECT '[{"id": 1, "name": "Alice", "score": 95.5}, {"id": 2, "name": "Bob", "score": 88.0}]' as data
) t
LATERAL VIEW py_expand_json(data) tmp AS id, name, score;
-- 结果:
-- 1, Alice, 95.5
-- 2, Bob, 88.0

SELECT ngram
FROM (SELECT 'Apache Doris is a fast database' as text) t
LATERAL VIEW py_ngram(text, 2) tmp AS ngram;
-- 结果:
-- Apache Doris
-- Doris is
-- is a
-- a fast
-- fast database
```

### 删除 Python UDTF

```sql
DROP FUNCTION IF EXISTS function_name(parameter_types);
```

示例:
```sql
DROP FUNCTION IF EXISTS py_split(STRING, STRING);
DROP FUNCTION IF EXISTS py_range(INT, INT);
DROP FUNCTION IF EXISTS py_explode_json(STRING);
```

### 修改 Python UDTF

Doris 不支持直接修改已有函数，需要先删除再重新创建:

```sql
DROP FUNCTION IF EXISTS py_split(STRING, STRING);
CREATE TABLES FUNCTION py_split(STRING, STRING) ...;
```

## 参数说明

### CREATE TABLES FUNCTION 参数

| 参数 | 说明 |
|------|------|
| `function_name` | 函数名称，遵循 SQL 标识符命名规则 |
| `parameter_types` | 参数类型列表，如 `INT`， `STRING`， `DOUBLE` 等 |
| `RETURNS ARRAY<...>` | 返回的数组类型，定义输出结构<br>• 单列: `ARRAY<类型>`<br>• 多列: `ARRAY<STRUCT<col1:type1, col2:type2, ...>>` |

### PROPERTIES 参数

| 参数 | 是否必需 | 默认值 | 说明 |
|------|---------|--------|------|
| `type` | 是 | - | 固定为 `"PYTHON_UDF"` |
| `symbol` | 是 | - | Python 函数名。<br>• **内联模式**: 直接写函数名，如 `"split_string_udtf"`<br>• **模块模式**: 格式为 `[package_name.]module_name.function_name` |
| `file` | 否 | - | Python `.zip` 包路径，仅模块模式需要。支持三种协议:<br>• `file://` - 本地文件系统路径<br>• `http://` - HTTP 远程下载<br>• `https://` - HTTPS 远程下载 |
| `runtime_version` | 否 | / | Python 运行时版本，如 `"3.10.12"` 或 `"3.12.11"` |
| `always_nullable` | 否 | `true` | 是否总是返回可空结果 |

### runtime_version 说明

- 必须填写 Python 版本的**完整版本号**，格式为 `x.x.x` 或 `x.x.xx`
- Doris 会在配置的 Python 环境中查找匹配该版本的解释器
- 详见 [Python UDF 环境配置](python-udf-environment.md)

## 数据类型映射

Python UDTF 使用与 Python UDF 完全相同的数据类型映射规则，包括整数、浮点、字符串、日期时间、Decimal、布尔、数组、STRUCT 等所有类型。

**详细的数据类型映射关系请参考**: [Python UDF 文档 - 数据类型映射](python-udf-documentation.md#数据类型映射)

### NULL 值处理

- Doris 会将 SQL 中的 `NULL` 值映射为 Python 的 `None`
- 在函数中，需要检查参数是否为 `None`
- `yield` 产出的值可以包含 `None`，表示该列为 `NULL`

## 实际应用场景

### 场景 1: CSV 数据解析

```sql
CREATE TABLES FUNCTION py_parse_csv(STRING)
RETURNS ARRAY<STRUCT<name:STRING, age:INT, city:STRING>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "parse_csv_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def parse_csv_udtf(csv_data):
    '''解析 CSV 格式的多行数据'''
    if csv_data is None:
        return
    lines = csv_data.strip().split('\n')
    for line in lines:
        parts = line.split(',')
        if len(parts) >= 3:
            name = parts[0].strip()
            age = int(parts[1].strip()) if parts[1].strip().isdigit() else None
            city = parts[2].strip()
            yield (name, age, city)
$$;

SELECT name, age, city
FROM (
    SELECT 'Alice,25,Beijing\nBob,30,Shanghai\nCharlie,28,Guangzhou' as data
) t
LATERAL VIEW py_parse_csv(data) tmp AS name, age, city;
-- 结果:
-- Alice, 25, Beijing
-- Bob, 30, Shanghai
-- Charlie, 28, Guangzhou
```

### 场景 2: 日期范围生成

```sql
CREATE TABLES FUNCTION py_date_range(STRING, STRING)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "date_range_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
from datetime import datetime, timedelta

def date_range_udtf(start_date, end_date):
    '''生成日期范围'''
    if start_date is None or end_date is None:
        return
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        current = start
        while current <= end:
            yield (current.strftime('%Y-%m-%d'),)
            current += timedelta(days=1)
    except:
        pass
$$;

SELECT date
FROM (SELECT py_date_range('2024-01-01', '2024-01-07')) tmp(date);
-- 结果: 2024-01-01 到 2024-01-07，每天一行

SELECT d.date, COALESCE(s.sales, 0) as sales
FROM (
    SELECT date 
    FROM (SELECT py_date_range('2024-01-01', '2024-01-31')) tmp(date)
) d
LEFT JOIN daily_sales s ON d.date = s.sales_date
ORDER BY d.date;
```

### 场景 3: 文本分词

```sql
CREATE TABLES FUNCTION py_tokenize(STRING)
RETURNS ARRAY<STRUCT<word:STRING, position:INT>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "tokenize_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
import re

def tokenize_udtf(text):
    '''将文本分词，输出单词和位置'''
    if text is None:
        return
    # 使用正则提取单词
    words = re.findall(r'\b\w+\b', text.lower())
    for i, word in enumerate(words, 1):
        if len(word) >= 2:  # 过滤单字符
            yield (word, i)
$$;

SELECT word, position
FROM (SELECT 'Apache Doris is a fast OLAP database' as text) t
LATERAL VIEW py_tokenize(text) tmp AS word, position;
-- 结果:
-- apache, 1
-- doris, 2
-- is, 3
-- fast, 4
-- olap, 5
-- database, 6

-- 词频统计
SELECT word, COUNT(*) as frequency
FROM documents
LATERAL VIEW py_tokenize(content) tmp AS word, position
GROUP BY word
ORDER BY frequency DESC
LIMIT 10;
```

### 场景 4: URL 参数解析

```sql
CREATE TABLES FUNCTION py_parse_url_params(STRING)
RETURNS ARRAY<STRUCT<param_name:STRING, param_value:STRING>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "parse_url_params_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
from urllib.parse import urlparse, parse_qs

def parse_url_params_udtf(url):
    '''解析 URL 参数'''
    if url is None:
        return
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        for key, values in params.items():
            for value in values:
                yield (key, value)
    except:
        pass
$$;

SELECT param_name, param_value
FROM (
    SELECT 'https://example.com/page?id=123&category=tech&tag=python&tag=database' as url
) t
LATERAL VIEW py_parse_url_params(url) tmp AS param_name, param_value;
-- 结果:
-- id, 123
-- category, tech
-- tag, python
-- tag, database
```

### 场景 5: IP 范围展开

```sql
CREATE TABLES FUNCTION py_expand_ip_range(STRING, STRING)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "expand_ip_range_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def expand_ip_range_udtf(start_ip, end_ip):
    '''展开 IP 地址范围（仅支持最后一段）'''
    if start_ip is None or end_ip is None:
        return
    try:
        # 假设格式: 192.168.1.10 到 192.168.1.20
        start_parts = start_ip.split('.')
        end_parts = end_ip.split('.')
        
        if len(start_parts) == 4 and len(end_parts) == 4:
            # 只展开最后一段
            if start_parts[:3] == end_parts[:3]:
                prefix = '.'.join(start_parts[:3])
                start_num = int(start_parts[3])
                end_num = int(end_parts[3])
                for i in range(start_num, end_num + 1):
                    yield (f"{prefix}.{i}",)
    except:
        pass
$$;

SELECT ip
FROM (SELECT py_expand_ip_range('192.168.1.10', '192.168.1.15')) tmp(ip);
-- 结果:
-- 192.168.1.10
-- 192.168.1.11
-- 192.168.1.12
-- 192.168.1.13
-- 192.168.1.14
-- 192.168.1.15
```

### 场景 6: 层级路径展开

```sql
CREATE TABLES FUNCTION py_expand_path(STRING, STRING)
RETURNS ARRAY<STRUCT<level:INT, name:STRING>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "expand_path_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def expand_path_udtf(path, delimiter):
    '''展开层级路径，如 /a/b/c 展开为 (1,a), (2,b), (3,c)'''
    if path is None:
        return
    if delimiter is None:
        delimiter = '/'
    parts = [p for p in path.split(delimiter) if p]
    for i, part in enumerate(parts, 1):
        yield (i, part)
$$;

SELECT level, name
FROM (SELECT '/home/user/documents/report.txt' as path) t
LATERAL VIEW py_expand_path(path, '/') tmp AS level, name;
-- 结果:
-- 1, home
-- 2, user
-- 3, documents
-- 4, report.txt

SELECT level, name, COUNT(*) as file_count
FROM file_paths
LATERAL VIEW py_expand_path(path, '/') tmp AS level, name
GROUP BY level, name
ORDER BY level, file_count DESC;
```

## 高级特性

### 使用第三方库

Python UDTF 可以使用 Python 标准库和第三方库:

```sql
CREATE TABLES FUNCTION py_extract_domains(STRING)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "extract_domains_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
import re
from urllib.parse import urlparse

def extract_domains_udtf(text):
    '''从文本中提取所有域名'''
    if text is None:
        return
    # 使用正则匹配 URL
    url_pattern = r'https?://[^\s]+'
    urls = re.findall(url_pattern, text)
    
    domains = set()
    for url in urls:
        try:
            parsed = urlparse(url)
            if parsed.netloc:
                domains.add(parsed.netloc)
        except:
            pass
    
    for domain in sorted(domains):
        yield (domain,)
$$;
```

### 条件输出控制

根据条件决定是否输出，以及输出内容:

```sql
CREATE TABLES FUNCTION py_filter_split(STRING, STRING, INT)
RETURNS ARRAY<STRING>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "filter_split_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def filter_split_udtf(text, delimiter, min_length):
    '''分割字符串，只输出长度 >= min_length 的部分'''
    if text is None or delimiter is None:
        return
    if min_length is None:
        min_length = 0
    
    parts = text.split(delimiter)
    for part in parts:
        part = part.strip()
        if len(part) >= min_length:
            yield (part,)
        # 否则跳过该部分
$$;

SELECT word
FROM (SELECT 'hello world python doris sql api' as text) t
LATERAL VIEW py_filter_split(text, ' ', 5) tmp AS word;
-- 结果:
-- hello
-- world
-- python
-- doris
```

### 动态输出行数

根据输入动态决定输出行数:

```sql
CREATE TABLES FUNCTION py_fibonacci(INT)
RETURNS ARRAY<BIGINT>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "fibonacci_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def fibonacci_udtf(n):
    '''生成前 n 个斐波那契数'''
    if n is None or n <= 0:
        return
    
    # 限制最大输出
    n = min(n, 50)
    
    a, b = 0, 1
    for _ in range(n):
        yield (a,)
        a, b = b, a + b
$$;

SELECT fib
FROM (SELECT py_fibonacci(10)) tmp(fib);
-- 结果: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34
```

## 错误处理

### 异常捕获

在 UDTF 中处理可能出现的异常:

```sql
CREATE TABLES FUNCTION py_safe_parse(STRING)
RETURNS ARRAY<STRUCT<name:STRING, value:INT>>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "safe_parse_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def safe_parse_udtf(data):
    '''安全地解析数据，错误行会被跳过'''
    if data is None:
        return
    
    lines = data.split('\n')
    for line in lines:
        try:
            parts = line.split(':')
            if len(parts) >= 2:
                name = parts[0].strip()
                value = int(parts[1].strip())
                yield (name, value)
        except (ValueError, IndexError):
            # 跳过错误的行
            continue
$$;

SELECT name, value
FROM (SELECT 'Alice:25\nBob:invalid\nCharlie:30\nDavid:abc' as data) t
LATERAL VIEW py_safe_parse(data) tmp AS name, value;
-- 结果:
-- Alice, 25
-- Charlie, 30
-- (Bob 和 David 的行被跳过)
```

### 边界条件处理

```sql
CREATE TABLES FUNCTION py_safe_range(INT, INT, INT)
RETURNS ARRAY<INT>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "safe_range_udtf",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def safe_range_udtf(start, end, max_output):
    '''生成范围，限制最大输出行数'''
    # 检查 NULL 值
    if start is None or end is None:
        return
    
    # 设置默认最大输出
    if max_output is None:
        max_output = 1000
    
    # 确保参数合理
    if start > end:
        return
    
    # 限制输出行数
    count = 0
    for i in range(start, end + 1):
        if count >= max_output:
            break
        yield (i,)
        count += 1
$$;
```

## 性能优化建议

### 1. 控制输出行数

- 对于可能产生大量输出的场景，设置合理的上限
- 避免笛卡尔积爆炸

### 2. 避免重复计算

如果需要多次使用同一个计算结果，预先计算:

```python
# ❌ 不推荐
def bad_split_udtf(text):
    for i in range(len(text.split(','))):  # 每次都 split
        parts = text.split(',')
        yield (parts[i],)

# ✅ 推荐
def good_split_udtf(text):
    parts = text.split(',')  # 只 split 一次
    for part in parts:
        yield (part,)
```

### 3. 使用生成器表达式

利用 Python 的生成器特性，避免创建中间列表:

```python
# ❌ 不推荐
def bad_filter_udtf(text, delimiter):
    parts = text.split(delimiter)
    filtered = [p.strip() for p in parts if p.strip()]  # 创建列表
    for part in filtered:
        yield (part,)

# ✅ 推荐
def good_filter_udtf(text, delimiter):
    parts = text.split(delimiter)
    for part in parts:
        part = part.strip()
        if part:  # 直接过滤
            yield (part,)
```

### 4. 减少字符串操作

字符串拼接和分割开销较大:

**推荐**:
```python
def efficient_udtf(items):
    # 使用 join 比多次 += 高效
    result = ','.join(items)
```

### 5. 避免访问外部资源

- 不要在 UDTF 中访问数据库、文件、网络
- 所有处理应基于输入参数

## 最佳实践

### 1. NULL 值处理

- 始终检查输入参数是否为 `None`
- 明确定义 NULL 输入的行为

```python
def udtf_example(value):
    if value is None:
        return  # 不输出任何行
    # 或
    if value is None:
        yield (None,)  # 输出 NULL 值
```

### 2. 输出格式验证

- 确保 `yield` 的格式与 `RETURNS ARRAY<...>` 定义一致
- 单列输出必须使用元组: `yield (value,)`
- 多列输出: `yield (value1, value2, ...)`

### 3. 文档和注释

- 为函数添加文档字符串，说明功能和参数
- 注释复杂的逻辑

```python
def my_udtf(param):
    '''
    函数功能说明
    
    参数:
        param: 参数说明
    
    返回:
        输出说明
    '''
    pass
```

### 4. 异常处理

- 使用 try-except 处理可能的异常
- 异常处理应该跳过错误数据，而不是中断整个查询

### 5. 测试验证

- 使用小数据集测试 UDTF 功能
- 测试 NULL 值、空字符串、边界值等场景
- 验证输出行数和数据格式

## 限制与注意事项

### 1. 无状态限制

- Python UDTF 是**无状态**的，每次函数调用独立处理一行
- 不能在多次调用之间保留状态
- 如果需要跨行聚合，应使用 UDAF

### 2. 性能考虑

- Python UDTF 性能低于内置表函数
- 适用于逻辑复杂但数据量适中的场景
- 大数据量场景优先考虑优化或使用内置函数

### 3. 输出类型固定

- `RETURNS ARRAY<...>` 定义的类型是固定的
- `yield` 产出的值必须与定义匹配
- 单列: `yield (value,)`，多列: `yield (value1, value2, ...)`

### 4. 函数命名

- 同一函数名在不同数据库中可重复定义
- 调用时建议指定数据库名以避免歧义

### 5. 环境一致性

- 所有 BE 节点的 Python 环境必须一致
- 包括 Python 版本、依赖包版本、环境配置

## 常见问题 FAQ

### Q1: UDTF 和 UDF 的区别是什么?

A: 
- **UDF (标量函数)**: 输入单行，输出单行。一对一关系
- **UDTF (表函数)**: 输入单行，输出零行或多行。一对多关系

示例:
```sql
-- UDF: 每行输入一个结果
SELECT py_upper(name) FROM users;

-- UDTF: 每行可能产生多个结果
SELECT tag FROM users LATERAL VIEW py_split(tags, ',') tmp AS tag;
```

### Q2: 为什么使用 CREATE TABLES FUNCTION 而不是 CREATE TABLE FUNCTION?

A: Doris 的 Python UDTF 语法使用 `CREATE TABLES FUNCTION`（TABLES 是复数），这是 Doris 的设计约定。请务必使用 **TABLES**（复数形式）。

### Q3: 单列输出为什么必须使用元组格式?

A: 由于 Python 的语法特性，`yield value` 会被解释为产出单个对象，而 UDTF 需要产出一行数据（即使只有一列）。因此必须使用 `yield (value,)` 明确表示这是一个单元素元组。

```python
# ❌ 错误
yield value

# ✅ 正确
yield (value,)
```

### Q4: 如何输出多列?

A: 多列输出使用 STRUCT 定义返回类型，并在 `yield` 时产出元组:

```sql
-- 定义
CREATE TABLES FUNCTION func(...)
RETURNS ARRAY<STRUCT<col1:INT, col2:STRING>>
...

-- Python 函数
def func(...):
    yield (123, 'hello')  # 对应 col1 和 col2
```

### Q5: 为什么我的 UDTF 没有输出?

A: 可能的原因:
1. **未调用 yield**: 确保在函数中调用了 `yield`
2. **条件过滤**: 所有数据都被过滤掉了
3. **异常被捕获**: 检查是否有 try-except 吞掉了错误
4. **NULL 输入**: 输入是 NULL 且函数直接返回

### Q6: UDTF 可以维护状态吗?

A: 不能。Python UDTF 是无状态的，每次函数调用独立处理一行。如果需要跨行聚合或维护状态，应使用 Python UDAF。

### Q7: 如何限制 UDTF 的输出行数?

A: 在函数中添加计数器或条件判断:

```python
def limited_udtf(data):
    max_rows = 1000
    count = 0
    for item in data.split(','):
        if count >= max_rows:
            break
        yield (item,)
        count += 1
```

### Q8: 如何在 Python UDTF 中使用第三方库?

A: Python UDTF 可以使用第三方库，但需要由 DBA 或运维人员**在集群的所有 BE 节点上手动安装依赖**。具体步骤:

1. **在每个 BE 节点上安装依赖**:
   ```bash
   # 使用 pip 安装
   pip install numpy pandas requests
   
   # 或使用 conda 安装
   conda install numpy pandas requests -y
   ```

2. **在 UDTF 中导入并使用**:
   ```sql
   CREATE TABLES FUNCTION py_process(STRING)
   RETURNS ARRAY<STRING>
   PROPERTIES (
       "type" = "PYTHON_UDF",
       "symbol" = "process_udtf",
       "runtime_version" = "3.10.12"
   )
   AS $$
   import json
   import re
   
   def process_udtf(data):
       # 使用第三方库
       parsed = json.loads(data)
       for item in parsed:
           yield (str(item),)
   $$;
   ```

**注意事项**:
- **`pandas` 和 `pyarrow` 是强制依赖**，必须在所有 Python 环境中预先安装，否则 Python UDTF 无法运行
- 必须在**所有 BE 节点**上安装相同版本的依赖，否则会导致部分节点执行失败
- 安装路径要与 Python UDTF 使用的 Python 运行时环境一致
- 建议使用虚拟环境管理依赖，避免与系统 Python 环境冲突

### Q9: UDTF 输出的数据类型有限制吗?

A: UDTF 支持所有 Doris 数据类型，包括基本类型（INT、STRING、DOUBLE 等）和复杂类型（ARRAY、STRUCT、MAP 等）。输出类型必须在 `RETURNS ARRAY<...>` 中明确定义。

### Q10: 可以在 UDTF 中访问外部资源吗?

A: 技术上可以，但**强烈不推荐**。UDTF 应该是纯函数式的，只基于输入参数进行处理。访问外部资源（数据库、文件、网络）会导致性能问题和不可预测的行为。

---

## 相关文档

- [Python UDF 使用指南](python-udf-documentation.md)
- [Python UDAF 使用指南](python-udaf-documentation.md)
- [Python UDF 环境配置](python-udf-environment.md)
