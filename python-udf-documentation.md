# Python UDF

## 功能概述

Python UDF (User Defined Function) 是 Apache Doris 提供的自定义标量函数扩展机制，允许用户使用 Python 语言编写自定义函数，用于数据查询和处理。通过 Python UDF，用户可以灵活地实现复杂的业务逻辑，处理各种数据类型，并充分利用 Python 丰富的生态库。

Python UDF 支持两种执行模式:
- **标量模式 (Scalar Mode)**: 逐行处理数据，适用于简单的转换和计算
- **向量化模式 (Vectorized Mode)**: 批量处理数据，利用 Pandas 进行高性能计算

> **环境依赖**: 使用 Python UDF 前，必须在所有 BE 节点的 Python 环境中预先安装 **`pandas`** 和 **`pyarrow`** 两个库，这是 Doris Python UDF 功能的强制依赖。详见 [Python UDF 环境配置](python-udf-environment.md)。

## 使用场景

Python UDF 适用于以下场景:

1. **数据清洗与转换**: 字符串格式化、数据脱敏、类型转换等
2. **复杂业务逻辑**: 需要多步骤计算或条件判断的业务规则
3. **数学与统计计算**: 利用 NumPy、Pandas 等库进行科学计算
4. **文本处理**: 正则表达式匹配、自然语言处理、文本分析
5. **日期时间处理**: 复杂的日期计算和格式转换
6. **自定义算法**: 实现特定的算法逻辑，如信用卡校验、距离计算等

## 基本语法

### 创建 Python UDF

Python UDF 支持两种创建方式:内联模式 (Inline) 和模块模式 (Module)。

> **注意**: 如果同时指定了 `file` 参数和 `AS $$` 内联 Python 代码，Doris 将会**优先加载内联 Python 代码**，采用内联模式运行 Python UDF。

#### 内联模式 (Inline Mode)

内联模式允许直接在 SQL 中编写 Python 代码，适合简单的函数逻辑。

**语法**:
```sql
CREATE FUNCTION function_name(parameter_type1, parameter_type2, ...)
RETURNS return_type
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "entry_function_name",
    "runtime_version" = "python_version",
    "always_nullable" = "true|false"
)
AS $$
def entry_function_name(param1, param2, ...):
    # Python code here
    return result
$$;
```

**示例 1: 整数加法**
```sql
DROP FUNCTION IF EXISTS py_add(INT, INT);

CREATE FUNCTION py_add(INT, INT)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12"
)
AS $$
def evaluate(a, b):
    return a + b
$$;

SELECT py_add(10, 20) AS result; -- 结果: 30
```

**示例 2: 字符串拼接**
```sql
DROP FUNCTION IF EXISTS py_concat(STRING, STRING);

CREATE FUNCTION py_concat(STRING, STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12"
)
AS $$
def evaluate(s1, s2):
    if s1 is None or s2 is None:
        return None
    return s1 + s2
$$;

SELECT py_concat('Hello', ' World') AS result; -- 结果: Hello World
SELECT py_concat(NULL, ' World') AS result; -- 结果: NULL
SELECT py_concat('Hello', NULL) AS result; -- 结果: NULL
```

**示例 3: 数学计算**
```sql
DROP FUNCTION IF EXISTS py_square(DOUBLE);

CREATE FUNCTION py_square(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12"
)
AS $$
def evaluate(x):
    if x is None:
        return None
    return x * x
$$;

SELECT py_square(5.0) AS result; -- 结果: 25.0
SELECT py_square(NULL) AS result; -- 结果: NULL
```

#### 模块模式 (Module Mode)

模块模式适合复杂的函数逻辑，需要将 Python 代码打包成 `.zip` 压缩包，并在函数创建时引用。

**步骤 1: 编写 Python 模块**

创建 `python_udf_scalar_ops.py` 文件:

```python
def add_three_numbers(a, b, c):
    """Add three numbers"""
    if a is None or b is None or c is None:
        return None
    return a + b + c

def reverse_string(s):
    """Reverse a string"""
    if s is None:
        return None
    return s[::-1]

def is_prime(n):
    """Check if a number is prime"""
    if n is None or n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    import math
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True
```

**步骤 2: 打包 Python 模块**

**必须**将 Python 文件打包成 `.zip` 格式(即使只有单个文件):
```bash
zip python_udf_scalar_ops.zip python_udf_scalar_ops.py
```

如果有多个 Python 文件:
```bash
zip python_udf_scalar_ops.zip python_udf_scalar_ops.py utils.py helper.py ...
```

**步骤 3: 设置 Python 模块压缩包的路径**

Python 模块压缩包支持多种部署方式，均通过 `file` 参数指定 `.zip` 包的路径:

**方式 1: 本地文件系统** (使用 `file://` 协议)
```sql
"file" = "file:///path/to/python_udf_scalar_ops.zip"
```
适用于 `.zip` 包已存放在 BE 节点本地文件系统的场景。

**方式 2: HTTP/HTTPS 远程下载** (使用 `http://` 或 `https://` 协议)
```sql
"file" = "http://example.com/udf/python_udf_scalar_ops.zip"
"file" = "https://s3.amazonaws.com/bucket/python_udf_scalar_ops.zip"
```
适用于从对象存储(如 S3、OSS、COS 等)或 HTTP 服务器下载 `.zip` 包的场景。Doris 会自动下载并缓存到本地。

> **注意**: 
> - 使用远程下载方式时，需确保所有 BE 节点都能访问该 URL
> - 首次调用时会下载文件，可能有一定延迟
> - 文件会被缓存，后续调用无需重复下载

**步骤 4: 设置 symbol 参数**

在模块模式下，`symbol` 参数用于指定函数在 ZIP 包中的位置，格式为:

```
[package_name.]module_name.func_name
```

**参数说明**:
- `package_name`(可选): ZIP 压缩包内顶层 Python 包的名称。若函数位于包的根模块下，或者 ZIP 压缩包中无 package，则可省略
- `module_name`(必填): 包含目标函数的 Python 模块文件名(不含 `.py` 后缀)
- `func_name`(必填): 用户定义的函数名或可调用类名

**解析规则**:
- Doris 会将 `symbol` 字符串按 `.` 分割:
  - 如果得到**两个**子字符串，分别为 `module_name` 和 `func_name`
  - 如果得到**三个及以上**的子字符串，开头为 `package_name`，中间为 `module_name`，结尾为 `func_name`
- `module_name` 部分作为模块路径，用于通过 `importlib` 动态导入
- 若指定了 `package_name`，则整个路径需构成一个合法的 Python 导入路径，且 ZIP 包结构必须与该路径一致

**示例说明**:

**示例 A: 无包结构(两段式)**
```
ZIP 结构:
math_ops.py

symbol = "math_ops.add"
```
表示函数 `add` 定义在 ZIP 包根目录下的 `math_ops.py` 文件中。

**示例 B: 有包结构(三段式)**
```
ZIP 结构:
mylib/
├── __init__.py
└── string_helper.py

symbol = "mylib.string_helper.split_text"
```
表示函数 `split_text` 定义在 `mylib/string_helper.py` 文件中，其中:
- `package_name` = `mylib`
- `module_name` = `string_helper`
- `func_name` = `split_text`

**示例 C: 嵌套包结构(四段式)**
```
ZIP 结构:
mylib/
├── __init__.py
└── utils/
    ├── __init__.py
    └── string_helper.py

symbol = "mylib.utils.string_helper.split_text"
```
表示函数 `split_text` 定义在 `mylib/utils/string_helper.py` 文件中，其中:
- `package_name` = `mylib`
- `module_name` = `utils.string_helper`
- `func_name` = `split_text`

> **注意**:
> - 若 `symbol` 格式不合法(如缺少函数名、模块名为空、路径中存在空组件等)，Doris 将在函数调用时报错
> - ZIP 包内的目录结构必须与 `symbol` 指定的路径一致
> - 每个包目录下都需要包含 `__init__.py` 文件(可以为空)

**步骤 5: 创建 UDF 函数**

**示例 1: 使用本地文件(无包结构)**
```sql
DROP FUNCTION IF EXISTS py_add_three(INT, INT, INT);
DROP FUNCTION IF EXISTS py_reverse(STRING);
DROP FUNCTION IF EXISTS py_is_prime(INT);

CREATE FUNCTION py_add_three(INT, INT, INT)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/python_udf_scalar_ops.zip",
    "symbol" = "python_udf_scalar_ops.add_three_numbers",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE FUNCTION py_reverse(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/python_udf_scalar_ops.zip",
    "symbol" = "python_udf_scalar_ops.reverse_string",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE FUNCTION py_is_prime(INT)
RETURNS BOOLEAN
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/python_udf_scalar_ops.zip",
    "symbol" = "python_udf_scalar_ops.is_prime",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);
```

**示例 2: 使用 HTTP/HTTPS 远程文件**
```sql
DROP FUNCTION IF EXISTS py_add_three(INT, INT, INT);
DROP FUNCTION IF EXISTS py_reverse(STRING);
DROP FUNCTION IF EXISTS py_is_prime(INT);

CREATE FUNCTION py_add_three(INT, INT, INT)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "https://your-storage.com/udf/python_udf_scalar_ops.zip",
    "symbol" = "python_udf_scalar_ops.add_three_numbers",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE FUNCTION py_reverse(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "https://your-storage.com/udf/python_udf_scalar_ops.zip",
    "symbol" = "python_udf_scalar_ops.reverse_string",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE FUNCTION py_is_prime(INT)
RETURNS BOOLEAN
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "https://your-storage.com/udf/python_udf_scalar_ops.zip",
    "symbol" = "python_udf_scalar_ops.is_prime",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);
```

**示例 3: 使用包结构**
```sql
DROP FUNCTION IF EXISTS py_multiply(INT);

-- ZIP 结构: my_udf/__init__.py, my_udf/math_ops.py
CREATE FUNCTION py_multiply(INT)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/my_udf.zip",
    "symbol" = "my_udf.math_ops.multiply_by_two",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);
```

**步骤 6: 使用函数**

```sql
SELECT py_add_three(10, 20, 30) AS sum_result; -- 结果: 60
SELECT py_reverse('hello') AS reversed; -- 结果: olleh
SELECT py_is_prime(17) AS is_prime; -- 结果: true
```

### 删除 Python UDF

语法:
```sql
DROP FUNCTION IF EXISTS function_name(parameter_type1, parameter_type2, ...);
```

示例:
```sql
DROP FUNCTION IF EXISTS py_add_three(INT, INT, INT);
DROP FUNCTION IF EXISTS py_reverse(STRING);
DROP FUNCTION IF EXISTS py_is_prime(INT);
```

## 参数说明

### CREATE FUNCTION 参数

| 参数 | 是否必需 | 说明 |
|------|---------|------|
| `function_name` | 是 | 函数名称，需要符合标识符命名规则 |
| `parameter_type` | 是 | 参数类型列表，支持 Doris 的各种数据类型 |
| `return_type` | 是 | 返回值类型 |

### PROPERTIES 参数

| 参数 | 是否必需 | 默认值 | 说明 |
|------|---------|--------|------|
| `type` | 是 | - | 固定为 `"PYTHON_UDF"` |
| `symbol` | 是 | - | Python 函数入口名称。<br>• **内联模式**: 直接写函数名，如 `"evaluate"`<br>• **模块模式**: 格式为 `[package_name.]module_name.func_name`，详见模块模式说明 |
| `file` | 否 | - | Python `.zip` 包路径，仅模块模式需要。支持三种协议:<br>• `file://` - 本地文件系统路径<br>• `http://` - HTTP 远程下载<br>• `https://` - HTTPS 远程下载 |
| `runtime_version` | 否 | / | Python 运行时版本，如 `"3.10.12"` 或 `"3.12"` |
| `always_nullable` | 否 | `true` | 是否总是返回可空结果 |

### 运行时版本说明

- 支持 Python 3.x 版本
- 可以指定完整版本号(如 `"3.10.12"`)或主次版本号(如 `"3.10"`)
- 如果不指定 `runtime_version`，将使用系统默认的 Python 版本
- 建议在生产环境中明确指定版本，确保行为一致性

## 数据类型映射

## 数据类型映射

下表列出了 Doris 数据类型与 Python 类型之间的映射关系：

| 类型分类 | Doris 类型 | Python 类型 | 说明 |
|---------|-----------|------------|------|
| 空类型 | `NULL` | `None` | 空值 |
| 布尔类型 | `BOOLEAN` | `bool` | 布尔值 |
| 整数类型 | `TINYINT` | `int` | 8 位整数 |
| | `SMALLINT` | `int` | 16 位整数 |
| | `INT` | `int` | 32 位整数 |
| | `BIGINT` | `int` | 64 位整数 |
| | `LARGEINT` | `int` | 128 位整数 |
| | `IPV4` | `int` | IPv4 地址(以整数形式) |
| 浮点类型 | `FLOAT` | `float` | 32 位浮点数 |
| | `DOUBLE` | `float` | 64 位浮点数 |
| | `TIME` / `TIMEV2` | `float` | 时间类型(以浮点数表示) |
| 字符串类型 | `CHAR` | `str` | 定长字符串 |
| | `VARCHAR` | `str` | 变长字符串 |
| | `STRING` | `str` | 字符串 |
| | `IPV6` | `str` | IPv6 地址(字符串形式) |
| | `JSONB` | `str` | JSON 二进制格式(转换为字符串) |
| | `VARIANT` | `str` | 变体类型(转换为字符串) |
| 日期时间类型 | `DATE` | `str` | 日期字符串，格式为 `'YYYY-MM-DD'` |
| | `DATEV2` | `datetime.date` | 日期对象 |
| | `DATETIME` | `str` | 日期时间字符串，格式为 `'YYYY-MM-DD HH:MM:SS'` |
| | `DATETIMEV2` | `datetime.datetime` | 日期时间对象 |
| Decimal 类型 | `DECIMAL` / `DECIMALV2` | `decimal.Decimal` | 高精度小数 |
| | `DECIMAL32` | `decimal.Decimal` | 32 位定点数 |
| | `DECIMAL64` | `decimal.Decimal` | 64 位定点数 |
| | `DECIMAL128` | `decimal.Decimal` | 128 位定点数 |
| | `DECIMAL256` | `decimal.Decimal` | 256 位定点数 |
| 二进制类型 | `BITMAP` | `bytes` | 位图数据 |
| | `HLL` | `bytes` | HyperLogLog 数据 |
| | `QUANTILE_STATE` | `bytes` | 分位数状态数据 |
| 复杂数据类型 | `ARRAY<T>` | `list` | 数组，元素类型为 T |
| | `MAP<K,V>` | `dict` | 字典，键类型为 K，值类型为 V |
| | `STRUCT<f1:T1, f2:T2, ...>` | `dict` | 结构体，字段名为键，字段值为值 |

### NULL 值处理

- Doris 的 `NULL` 值在 Python 中映射为 `None`
- 如果函数参数为 `NULL`，Python 函数接收到的是 `None`
- 如果 Python 函数返回 `None`，Doris 将其视为 `NULL`
- 建议在函数中显式处理 `None` 值，避免运行时错误

示例:
```sql
CREATE FUNCTION py_safe_divide(DOUBLE, DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(a, b):
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return a / b
$$;

SELECT py_safe_divide(10.0, 2.0);   -- 结果: 5.0
SELECT py_safe_divide(10.0, 0.0);   -- 结果: NULL
SELECT py_safe_divide(10.0, NULL);  -- 结果: NULL
```

## 向量化模式

向量化模式使用 Pandas 批量处理数据，性能优于标量模式。在向量化模式下，函数参数为 `pandas.Series` 对象，返回值也应为 `pandas.Series`。

> **注意**: 为确保系统正确识别向量化模式，请在函数签名中使用类型注解(如 `a: pd.Series`)并在函数逻辑中直接操作批量数据结构。若未明确使用向量化类型，系统将回退到标量模式(Scalar Mode)。

```python
# 使用向量化模式
def add(a: pd.Series, b: pd.Series) -> pd.Series:
    return a + b + 1

# 回退到标量模式
def add(a, b):
    return a + b + 1
```

### 基本示例

**示例 1: 向量化整数加法**
```sql
CREATE FUNCTION py_vec_add(INT, INT)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "add",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
import pandas as pd

def add(a: pd.Series, b: pd.Series) -> pd.Series:
    return a + b + 1
$$;
```

**示例 2: 向量化字符串处理**
```sql
CREATE FUNCTION py_vec_upper(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "to_upper",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
import pandas as pd

def to_upper(s: pd.Series) -> pd.Series:
    return s.str.upper()
$$;
```

**示例 3: 向量化数学运算**
```sql
CREATE FUNCTION py_vec_sqrt(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "sqrt",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
import pandas as pd
import numpy as np

def sqrt(x: pd.Series) -> pd.Series:
    return np.sqrt(x)
$$;
```

### 向量化模式的优势

1. **性能优化**: 批量处理数据，减少 Python 与 Doris 之间的交互次数
2. **利用 Pandas/NumPy**: 充分发挥向量化计算的性能优势
3. **简洁代码**: 使用 Pandas API 可以更简洁地表达复杂逻辑

### 使用向量化函数

首先创建测试表和向量化函数：

```sql
DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
    id INT,
    value INT,
    text STRING,
    score DOUBLE
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO test_table VALUES
(1, 10, 'hello', 85.5),
(2, 20, 'world', 92.0),
(3, 30, 'python', 78.3);

SELECT 
    id,
    py_vec_add(value, value) AS sum_result,
    py_vec_upper(text) AS upper_text,
    py_vec_sqrt(score) AS sqrt_score
FROM test_table;

+------+------------+------------+-------------------+
| id   | sum_result | upper_text | sqrt_score        |
+------+------------+------------+-------------------+
|    1 |         21 | HELLO      | 9.246621004453464 |
|    2 |         41 | WORLD      | 9.591663046625438 |
|    3 |         61 | PYTHON     | 8.848728722251575 |
+------+------------+------------+-------------------+
```

## 复杂数据类型处理

### ARRAY 类型

**示例: 数组元素求和**
```sql
DROP FUNCTION IF EXISTS py_array_sum(ARRAY<INT>);

CREATE FUNCTION py_array_sum(ARRAY<INT>)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(arr):
    if arr is None:
        return None
    return sum(arr)
$$;

SELECT py_array_sum([1, 2, 3, 4, 5]) AS result; -- 结果: 15
```

**示例: 数组过滤**
```sql
DROP FUNCTION IF EXISTS py_array_filter_positive(ARRAY<INT>);

CREATE FUNCTION py_array_filter_positive(ARRAY<INT>)
RETURNS ARRAY<INT>
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(arr):
    if arr is None:
        return None
    return [x for x in arr if x > 0]
$$;

SELECT py_array_filter_positive([1, -2, 3, -4, 5]) AS result; -- 结果: [1, 3, 5]
```

### MAP 类型

**示例: 获取 MAP 的键数量**
```sql
CREATE FUNCTION py_map_size(MAP<STRING, INT>)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(m):
    if m is None:
        return None
    return len(m)
$$;

SELECT py_map_size({'a': 1, 'b': 2, 'c': 3}) AS result; -- 结果: 3
```

**示例: 获取 MAP 中的值**
```sql
DROP FUNCTION IF EXISTS py_map_get(MAP<STRING, STRING>, STRING);

CREATE FUNCTION py_map_get(MAP<STRING, STRING>, STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(m, key):
    if m is None or key is None:
        return None
    return m.get(key)
$$;

SELECT py_map_get({'name': 'Alice', 'age': '30'}, 'name') AS result; -- 结果: Alice
```

### STRUCT 类型

**示例: 访问 STRUCT 字段**
```sql
DROP FUNCTION IF EXISTS py_struct_get_name(STRUCT<name: STRING, age: INT>);

CREATE FUNCTION py_struct_get_name(STRUCT<name: STRING, age: INT>)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(s):
    if s is None:
        return None
    return s.get('name')
$$;

SELECT py_struct_get_name({'Alice', 30}) AS result; -- 结果: Alice
```

## 实际应用场景

### 场景 1: 数据脱敏

```sql
DROP FUNCTION IF EXISTS py_mask_email(STRING);

CREATE FUNCTION py_mask_email(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12"
)
AS $$
def evaluate(email):
    if email is None or '@' not in email:
        return None
    parts = email.split('@')
    if len(parts[0]) <= 1:
        return email
    masked_user = parts[0][0] + '***'
    return f"{masked_user}@{parts[1]}"
$$;

SELECT py_mask_email('user@example.com') AS masked; -- 结果: u***@example.com
```

### 场景 2: 字符串相似度计算

```sql
DROP FUNCTION IF EXISTS py_levenshtein_distance(STRING, STRING);

CREATE FUNCTION py_levenshtein_distance(STRING, STRING)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12"
)
AS $$
def evaluate(s1, s2):
    if s1 is None or s2 is None:
        return None
    if len(s1) < len(s2):
        return evaluate(s2, s1)
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]
$$;

SELECT py_levenshtein_distance('kitten', 'sitting') AS distance; -- 结果: 3
```

### 场景 3: 日期计算

```sql
DROP FUNCTION IF EXISTS py_days_between(DATE, DATE);

CREATE FUNCTION py_days_between(DATE, DATE)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12"
)
AS $$
from datetime import datetime

def evaluate(date1_str, date2_str):
    if date1_str is None or date2_str is None:
        return None
    try:
        d1 = datetime.strptime(str(date1_str), '%Y-%m-%d')
        d2 = datetime.strptime(str(date2_str), '%Y-%m-%d')
        return abs((d2 - d1).days)
    except:
        return None
$$;

SELECT py_days_between('2024-01-01', '2024-12-31') AS days; -- 结果: 365
```

### 场景 4: 身份证号校验

```sql
DROP FUNCTION IF EXISTS py_validate_id_card(STRING);

CREATE FUNCTION py_validate_id_card(STRING)
RETURNS BOOLEAN
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12"
)
AS $$
def evaluate(id_card):
    if id_card is None or len(id_card) != 18:
        return False
    
    # 校验前17位是否为数字
    if not id_card[:17].isdigit():
        return False
    
    # 校验码权重
    weights = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    check_codes = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']
    
    # 计算校验码
    total = sum(int(id_card[i]) * weights[i] for i in range(17))
    check_code = check_codes[total % 11]
    
    return id_card[17].upper() == check_code
$$;

SELECT py_validate_id_card('11010519491231002X') AS is_valid; -- 结果: True

SELECT py_validate_id_card('110105194912310021x') AS is_valid; -- 结果: False
```

## 错误处理

### 异常捕获

在 Python UDF 中建议使用 try-except 捕获异常，避免函数执行失败:

```sql
DROP FUNCTION IF EXISTS py_safe_int_parse(STRING);

CREATE FUNCTION py_safe_int_parse(STRING)
RETURNS INT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(s):
    if s is None:
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None
$$;

SELECT py_safe_int_parse('123');   -- 结果: 123
SELECT py_safe_int_parse('abc');   -- 结果: NULL
```

### 边界条件处理

在处理字符串、数组等可能越界的操作时，需要进行边界检查以避免运行时错误。

**示例: 安全的字符串索引访问**

```sql
DROP FUNCTION IF EXISTS py_safe_get_char(STRING, INT);

CREATE FUNCTION py_safe_get_char(STRING, INT)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "evaluate",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
def evaluate(s, index):
    # 检查空值
    if s is None or index is None:
        return None
    # 检查索引是否越界
    if index < 0 or index >= len(s):
        return None
    return s[index]
$$;

SELECT py_safe_get_char('Hello', 0);   -- 结果: H
SELECT py_safe_get_char('Hello', 4);   -- 结果: o
SELECT py_safe_get_char('Hello', 10);  -- 结果: NULL (索引越界)
SELECT py_safe_get_char('Hello', -1);  -- 结果: NULL (负数索引)
SELECT py_safe_get_char(NULL, 0);      -- 结果: NULL
```

- 在访问序列元素前，先检查索引是否在有效范围内
- 对于负数索引、超出长度的索引，返回 NULL 而不是抛出异常
- 确保函数在各种边界情况下都能安全运行

## 性能优化建议

### 1. 优先使用向量化模式

向量化模式性能显著优于标量模式:

```python
# 标量模式 - 逐行处理
def scalar_process(x):
    return x * 2

# 向量化模式 - 批量处理
import pandas as pd
def vector_process(x: pd.Series) -> pd.Series:
    return x * 2
```

### 2. 减少不必要的类型转换

避免在函数中进行频繁的类型转换:

```python
# ❌ 不推荐
def bad_example(x):
    return str(int(float(x)))

# ✅ 推荐
def good_example(x):
    if x is None:
        return None
    return x  # 直接返回
```

### 3. 使用模块模式管理复杂逻辑

将复杂的函数逻辑放在独立的 Python 文件中，便于维护和复用。

### 4. 合理设置 always_nullable

如果确定函数不会返回 NULL，可以设置 `"always_nullable" = "false"` 以提升性能。

### 5. 避免在函数中执行 I/O 操作

不建议在 UDF 中进行文件读写、网络请求等 I/O 操作，这会严重影响性能。

## 最佳实践



## 限制与注意事项

### 1. Python 版本支持

- 仅支持 Python 3.x 版本
- 建议使用 Python 3.10 或更高版本
- 确保 Doris 集群已安装对应的 Python 运行时

### 2. 依赖库

- 内置支持 Python 标准库
- 支持 Pandas、NumPy 等常用科学计算库
- 如需使用第三方库，需要在集群环境中预先安装

### 3. 性能考虑

- Python UDF 性能低于原生 UDF(C++ 实现)
- 对于性能敏感场景，建议优先考虑 Doris 内置函数
- 大数据量场景建议使用向量化模式

### 4. 安全性

- UDF 代码在 Doris 进程中执行，需要确保代码安全可信
- 避免在 UDF 中执行危险操作(如系统命令、文件删除等)
- 生产环境建议对 UDF 代码进行审核

### 5. 资源限制

- UDF 执行会占用 BE 节点的 CPU 和内存资源
- 大量使用 UDF 可能影响集群整体性能
- 建议监控 UDF 的资源消耗情况

## 常见问题

### Q1: 如何在 Python UDF 中使用第三方库?

A: 需要在所有 BE 节点上安装对应的 Python 库。例如:
```bash
pip3 install numpy pandas
```

### Q2: Python UDF 是否支持递归函数?

A: 支持，但需要注意递归深度，避免栈溢出。

### Q3: 如何调试 Python UDF?

A: 可以在本地 Python 环境中先调试函数逻辑，确保无误后再创建 UDF。可以查看 BE 日志获取错误信息。

### Q4: Python UDF 是否支持全局变量?

A: 支持，但不建议使用全局变量，因为在分布式环境中全局变量的行为可能不符合预期。

### Q5: 如何更新已存在的 Python UDF?

A: 先删除旧的 UDF，再创建新的:
```sql
DROP FUNCTION IF EXISTS function_name(parameter_types);
CREATE FUNCTION function_name(...) ...;
```

### Q6: Python UDF 能否访问外部资源?

A: 技术上可以，但**强烈不建议**。Python UDF 中可以使用网络请求库(如 `requests`)访问外部 API、数据库等，但这会严重影响性能和稳定性。原因包括:
- 网络延迟会导致查询变慢
- 外部服务不可用时会导致 UDF 失败
- 大量并发请求可能造成外部服务压力
- 难以控制超时和错误处理

**推荐做法**: 将外部数据预先导入 Doris，通过 JOIN 等 SQL 方式处理。

### Q7: 如何在 Python UDF 中使用第三方库?

A: Python UDF 可以使用第三方库，但需要由 DBA 或运维人员**在集群的所有 BE 节点上手动安装依赖**。具体步骤:

1. **在每个 BE 节点上安装依赖**:
   ```bash
   # 使用 pip 安装
   pip install numpy pandas requests
   
   # 或使用 conda 安装
   conda install numpy pandas requests
   ```

2. **在 UDF 中导入并使用**:
   ```sql
   CREATE FUNCTION py_use_numpy(DOUBLE)
   RETURNS DOUBLE
   PROPERTIES (
       "type" = "PYTHON_UDF",
       "symbol" = "evaluate",
       "runtime_version" = "3.10.12"
   )
   AS $$
   import numpy as np
   
   def evaluate(x):
       return np.sqrt(x)
   $$;
   ```

**注意事项**:
- **`pandas` 和 `pyarrow` 是强制依赖**，必须在所有 Python 环境中预先安装，否则 Python UDF 无法运行
- 必须在**所有 BE 节点**上安装相同版本的依赖，否则会导致部分节点执行失败
- 安装路径要与 Python UDF 使用的 Python 运行时环境一致
- 建议使用虚拟环境管理依赖，避免与系统 Python 环境冲突
- 常用库如 `numpy`、`scipy`、`scikit-learn` 等都可以正常使用
