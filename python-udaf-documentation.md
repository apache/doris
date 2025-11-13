# Python UDAF

## 功能概述

Python UDAF (User Defined Aggregate Function) 是 Apache Doris 提供的自定义聚合函数扩展机制，允许用户使用 Python 语言编写自定义聚合函数，用于数据分组聚合和窗口计算。通过 Python UDAF，用户可以灵活地实现复杂的聚合逻辑，如统计分析、数据收集、自定义指标计算等。

Python UDAF 的核心特点:
- **分布式聚合**: 支持分布式环境下的数据聚合，自动处理数据的分区、合并和最终计算
- **状态管理**: 通过类实例维护聚合状态，支持复杂的状态对象
- **窗口函数支持**: 可用于窗口函数 (OVER 子句)，实现移动聚合、排名等高级功能
- **灵活性强**: 可实现任意复杂的聚合逻辑，不受内置聚合函数限制

> **环境依赖**: 使用 Python UDAF 前，必须在所有 BE 节点的 Python 环境中预先安装 **`pandas`** 和 **`pyarrow`** 两个库，这是 Doris Python UDAF 功能的强制依赖。详见 [Python UDF 环境配置](python-udf-environment.md)。

## 使用场景

Python UDAF 适用于以下场景:

1. **统计分析**: 计算方差、标准差、中位数、百分位数等统计指标
2. **数据收集**: 收集并合并分组内的数据，如 collect_list、collect_set
3. **自定义指标**: 实现业务特定的聚合指标，如加权平均、几何平均
4. **复杂计算**: 需要维护多个状态变量的聚合，如协方差、线性回归
5. **窗口分析**: 配合窗口函数实现移动平均、累计求和、排名等
6. **数据去重与计数**: 实现自定义的去重逻辑和计数规则

## UDAF 基本概念

### 聚合函数的生命周期

Python UDAF 通过类来实现，一个聚合函数的执行包含以下阶段:

1. **初始化 (__init__)**: 创建聚合状态对象，初始化状态变量
2. **累积 (accumulate)**: 处理单行数据，更新聚合状态
3. **合并 (merge)**: 合并多个分区的聚合状态(分布式场景)
4. **完成 (finish)**: 计算并返回最终聚合结果

### 必需的类方法和属性

一个完整的 Python UDAF 类必须实现以下方法:

| 方法/属性 | 说明 | 是否必需 |
|----------|------|---------|
| `__init__(self)` | 初始化聚合状态 | 是 |
| `accumulate(self, *args)` | 累积单行数据 | 是 |
| `merge(self, other_state)` | 合并其他分区的状态 | 是 |
| `finish(self)` | 返回最终聚合结果 | 是 |
| `aggregate_state` (属性) | 返回可序列化的聚合状态，**必须支持 pickle 序列化** | 是 |

## 基本语法

### 创建 Python UDAF

Python UDAF 支持两种创建方式:内联模式 (Inline) 和模块模式 (Module)。

> **注意**: 如果同时指定了 `file` 参数和 `AS $$` 内联 Python 代码，Doris 将会**优先加载内联 Python 代码**，采用内联模式运行 Python UDAF。

#### 内联模式 (Inline Mode)

内联模式允许直接在 SQL 中编写 Python 类，适合简单的聚合逻辑。

**语法**:
```sql
CREATE AGGREGATE FUNCTION function_name(parameter_type1, parameter_type2, ...)
RETURNS return_type
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "ClassName",
    "runtime_version" = "python_version",
    "always_nullable" = "true|false"
)
AS $$
class ClassName:
    def __init__(self):
        # 初始化状态变量
        
    @property
    def aggregate_state(self):
        # 返回可序列化的状态
        
    def accumulate(self, *args):
        # 累积数据
        
    def merge(self, other_state):
        # 合并状态
        
    def finish(self):
        # 返回最终结果
$$;
```

**示例 1: 求和聚合**

```sql
DROP TABLE IF EXISTS sales;

CREATE TABLE IF NOT EXISTS sales (
    id INT,
    category VARCHAR(50),
    amount INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO sales VALUES 
(1, 'Electronics', 1000),
(2, 'Electronics', 1500),
(3, 'Books', 200),
(4, 'Books', 300),
(5, 'Clothing', 500),
(6, 'Clothing', 800),
(7, 'Electronics', 2000),
(8, 'Books', 150);

DROP FUNCTION IF EXISTS py_sum(INT);

CREATE AGGREGATE FUNCTION py_sum(INT)
RETURNS BIGINT
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "SumUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class SumUDAF:
    def __init__(self):
        self.total = 0
    
    @property
    def aggregate_state(self):
        return self.total
    
    def accumulate(self, value):
        if value is not None:
            self.total += value
    
    def merge(self, other_state):
        self.total += other_state
    
    def finish(self):
        return self.total
$$;

SELECT category, py_sum(amount) as total_amount
FROM sales
GROUP BY category
ORDER BY category;

+-------------+--------------+
| category    | total_amount |
+-------------+--------------+
| Books       |          650 |
| Clothing    |         1300 |
| Electronics |         4500 |
+-------------+--------------+
```

**示例 2: 平均值聚合**

```sql
DROP TABLE IF EXISTS employees;

CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DOUBLE
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO employees VALUES 
(1, 'Alice', 'Engineering', 80000.0),
(2, 'Bob', 'Engineering', 90000.0),
(3, 'Charlie', 'Sales', 60000.0),
(4, 'David', 'Sales', 80000.0),
(5, 'Eve', 'HR', 50000.0),
(6, 'Frank', 'Engineering', 70000.0),
(7, 'Grace', 'HR', 70000.0);

DROP FUNCTION IF EXISTS py_avg(DOUBLE);

CREATE AGGREGATE FUNCTION py_avg(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "AvgUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class AvgUDAF:
    def __init__(self):
        self.sum = 0.0
        self.count = 0
    
    @property
    def aggregate_state(self):
        return (self.sum, self.count)
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
            self.count += 1
    
    def merge(self, other_state):
        other_sum, other_count = other_state
        self.sum += other_sum
        self.count += other_count
    
    def finish(self):
        if self.count == 0:
            return None
        return self.sum / self.count
$$;

SELECT department, py_avg(salary) as avg_salary
FROM employees
GROUP BY department
ORDER BY department;

+-------------+------------+
| department  | avg_salary |
+-------------+------------+
| Engineering |      80000 |
| HR          |      60000 |
| Sales       |      70000 |
+-------------+------------+
```

**示例 3: 字符串收集聚合**

```sql
DROP TABLE IF EXISTS orders;

CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    user_id INT,
    product_name VARCHAR(100)
) DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO orders VALUES 
(1, 101, 'Laptop'),
(2, 101, 'Mouse'),
(3, 102, 'Keyboard'),
(4, 101, 'Monitor'),
(5, 103, 'Headphones'),
(6, 102, 'Mouse'),
(7, 103, 'Laptop'),
(8, 102, 'Cable');

DROP FUNCTION IF EXISTS py_collect_list(STRING);

CREATE AGGREGATE FUNCTION py_collect_list(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "CollectListUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class CollectListUDAF:
    def __init__(self):
        self.items = []
    
    @property
    def aggregate_state(self):
        return self.items
    
    def accumulate(self, value):
        if value is not None:
            self.items.append(value)
    
    def merge(self, other_state):
        if other_state:
            self.items.extend(other_state)
    
    def finish(self):
        if not self.items:
            return None
        return ','.join(sorted(self.items))
$$;

SELECT user_id, py_collect_list(product_name) as purchased_products
FROM orders
GROUP BY user_id
ORDER BY user_id;

+---------+----------------------+
| user_id | purchased_products   |
+---------+----------------------+
|     101 | Laptop,Monitor,Mouse |
|     102 | Cable,Keyboard,Mouse |
|     103 | Headphones,Laptop    |
+---------+----------------------+
```

#### 模块模式 (Module Mode)

模块模式适合复杂的聚合逻辑，需要将 Python 代码打包成 `.zip` 压缩包，并在函数创建时引用。

**步骤 1: 编写 Python 模块**

创建 `stats_udaf.py` 文件:

```python
import math

class VarianceUDAF:
    """计算总体方差"""
    
    def __init__(self):
        self.count = 0
        self.sum_val = 0.0
        self.sum_sq = 0.0
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum_val, self.sum_sq)
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum_val += value
            self.sum_sq += value * value
    
    def merge(self, other_state):
        other_count,  other_sum,  other_sum_sq = other_state
        self.count += other_count
        self.sum_val += other_sum
        self.sum_sq += other_sum_sq
    
    def finish(self):
        if self.count == 0:
            return None
        mean = self.sum_val / self.count
        variance = (self.sum_sq / self.count) - (mean * mean)
        return variance


class StdDevUDAF:
    """计算总体标准差"""
    
    def __init__(self):
        self.count = 0
        self.sum_val = 0.0
        self.sum_sq = 0.0
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum_val, self.sum_sq)
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum_val += value
            self.sum_sq += value * value
    
    def merge(self, other_state):
        other_count, other_sum, other_sum_sq = other_state
        self.count += other_count
        self.sum_val += other_sum
        self.sum_sq += other_sum_sq
    
    def finish(self):
        if self.count == 0:
            return None
        mean = self.sum_val / self.count
        variance = (self.sum_sq / self.count) - (mean * mean)
        return math.sqrt(max(0， variance))


class MedianUDAF:
    """计算中位数"""
    
    def __init__(self):
        self.values = []
    
    @property
    def aggregate_state(self):
        return self.values
    
    def accumulate(self, value):
        if value is not None:
            self.values.append(value)
    
    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)
    
    def finish(self):
        if not self.values:
            return None
        sorted_vals = sorted(self.values)
        n = len(sorted_vals)
        if n % 2 == 0:
            return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2.0
        else:
            return sorted_vals[n//2]
```

**步骤 2: 打包 Python 模块**

**必须**将 Python 文件打包成 `.zip` 格式(即使只有单个文件):
```bash
zip stats_udaf.zip stats_udaf.py
```

**步骤 3: 设置 Python 模块压缩包的路径**

支持多种部署方式，通过 `file` 参数指定 `.zip` 包的路径:

**方式 1: 本地文件系统** (使用 `file://` 协议)
```sql
"file" = "file:///path/to/stats_udaf.zip"
```

**方式 2: HTTP/HTTPS 远程下载** (使用 `http://` 或 `https://` 协议)
```sql
"file" = "http://example.com/udaf/stats_udaf.zip"
"file" = "https://s3.amazonaws.com/bucket/stats_udaf.zip"
```

> **注意**: 
> - 使用远程下载方式时，需确保所有 BE 节点都能访问该 URL
> - 首次调用时会下载文件，可能有一定延迟
> - 文件会被缓存，后续调用无需重复下载

**步骤 4: 设置 symbol 参数**

在模块模式下，`symbol` 参数用于指定类在 ZIP 包中的位置，格式为:

```
[package_name.]module_name.ClassName
```

**参数说明**:
- `package_name`(可选): ZIP 压缩包内顶层 Python 包的名称
- `module_name`(必填): 包含目标类的 Python 模块文件名(不含 `.py` 后缀)
- `ClassName`(必填): UDAF 类名

**解析规则**:
- Doris 会将 `symbol` 字符串按 `.` 分割:
  - 如果得到**两个**子字符串，分别为 `module_name` 和 `ClassName`
  - 如果得到**三个及以上**的子字符串，开头为 `package_name`，中间为 `module_name`，结尾为 `ClassName`

**步骤 5: 创建 UDAF 函数**

```sql
DROP FUNCTION IF EXISTS py_variance(DOUBLE);
DROP FUNCTION IF EXISTS py_stddev(DOUBLE);
DROP FUNCTION IF EXISTS py_median(DOUBLE);

CREATE AGGREGATE FUNCTION py_variance(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/stats_udaf.zip",
    "symbol" = "stats_udaf.VarianceUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE AGGREGATE FUNCTION py_stddev(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/stats_udaf.zip",
    "symbol" = "stats_udaf.StdDevUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);

CREATE AGGREGATE FUNCTION py_median(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "file" = "file:///path/to/stats_udaf.zip",
    "symbol" = "stats_udaf.MedianUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
);
```

**步骤 6: 使用函数**

```sql
DROP TABLE IF EXISTS exam_results;

CREATE TABLE IF NOT EXISTS exam_results (
    id INT,
    student_name VARCHAR(100),
    category VARCHAR(50),
    score DOUBLE
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO exam_results VALUES 
(1, 'Alice', 'Math', 85.0),
(2, 'Bob', 'Math', 92.0),
(3, 'Charlie', 'Math', 78.0),
(4, 'David', 'Math', 88.0),
(5, 'Eve', 'Math', 95.0),
(6, 'Frank', 'English', 75.0),
(7, 'Grace', 'English', 82.0),
(8, 'Henry', 'English', 88.0),
(9, 'Iris', 'English', 79.0),
(10, 'Jack', 'Physics', 90.0),
(11, 'Kate', 'Physics', 85.0),
(12, 'Lily', 'Physics', 92.0),
(13, 'Mike', 'Physics', 88.0);

SELECT 
    category,
    py_variance(score) as variance,
    py_stddev(score) as std_dev,
    py_median(score) as median
FROM exam_results
GROUP BY category
ORDER BY category;

+----------+-------------------+-------------------+--------+
| category | variance          | std_dev           | median |
+----------+-------------------+-------------------+--------+
| English  |              22.5 | 4.743416490252569 |   80.5 |
| Math     | 34.64000000000033 | 5.885575587824892 |     88 |
| Physics  |            6.6875 |  2.58602010819715 |     89 |
+----------+-------------------+-------------------+--------+
```

### 删除 Python UDAF

```sql
DROP FUNCTION IF EXISTS function_name(parameter_types);
```

示例:
```sql
DROP FUNCTION IF EXISTS py_sum(INT);
DROP FUNCTION IF EXISTS py_avg(DOUBLE);
DROP FUNCTION IF EXISTS py_variance(DOUBLE);
```

## 参数说明

### CREATE AGGREGATE FUNCTION 参数

| 参数 | 说明 |
|------|------|
| `function_name` | 函数名称，遵循 SQL 标识符命名规则 |
| `parameter_types` | 参数类型列表，如 `INT`， `DOUBLE`， `STRING` 等 |
| `RETURNS return_type` | 返回值类型 |

### PROPERTIES 参数

| 参数 | 是否必需 | 默认值 | 说明 |
|------|---------|--------|------|
| `type` | 是 | - | 固定为 `"PYTHON_UDF"` |
| `symbol` | 是 | - | Python 类名。<br>• **内联模式**: 直接写类名，如 `"SumUDAF"`<br>• **模块模式**: 格式为 `[package_name.]module_name.ClassName` |
| `file` | 否 | - | Python `.zip` 包路径，仅模块模式需要。支持三种协议:<br>• `file://` - 本地文件系统路径<br>• `http://` - HTTP 远程下载<br>• `https://` - HTTPS 远程下载 |
| `runtime_version` | 否 | / | Python 运行时版本，如 `"3.10.12"` 或 `"3.12.11"` |
| `always_nullable` | 否 | `true` | 是否总是返回可空结果 |

### runtime_version 说明

- 必须填写 Python 版本的**完整版本号**，格式为 `x.x.x` 或 `x.x.xx`
- Doris 会在配置的 Python 环境中查找匹配该版本的解释器
- 详见 [Python UDF 环境配置](python-udf-environment.md)

## 高级特性

### 多参数聚合函数

```sql
DROP FUNCTION IF EXISTS py_weighted_avg(DOUBLE, INT);

CREATE AGGREGATE FUNCTION py_weighted_avg(DOUBLE, INT)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "WeightedAvgUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class WeightedAvgUDAF:
    def __init__(self):
        self.weighted_sum = 0.0
        self.weight_sum = 0
    
    @property
    def aggregate_state(self):
        return (self.weighted_sum, self.weight_sum)
    
    def accumulate(self, value, weight):
        if value is not None and weight is not None and weight > 0:
            self.weighted_sum += value * weight
            self.weight_sum += weight
    
    def merge(self, other_state):
        other_weighted_sum, other_weight_sum = other_state
        self.weighted_sum += other_weighted_sum
        self.weight_sum += other_weight_sum
    
    def finish(self):
        if self.weight_sum == 0:
            return None
        return self.weighted_sum / self.weight_sum
$$;

DROP TABLE IF EXISTS student_grades;

CREATE TABLE IF NOT EXISTS student_grades (
    student_id INT,
    student_name VARCHAR(100),
    class_id INT,
    score DOUBLE,
    credit INT
) DUPLICATE KEY(student_id)
DISTRIBUTED BY HASH(student_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO student_grades VALUES 
(1, 'Alice', 101, 85.0, 3),
(2, 'Bob', 101, 92.0, 4),
(3, 'Charlie', 101, 78.0, 3),
(4, 'David', 102, 88.0, 4),
(5, 'Eve', 102, 95.0, 3),
(6, 'Frank', 102, 82.0, 2),
(7, 'Grace', 103, 90.0, 4),
(8, 'Henry', 103, 75.0, 3);

SELECT class_id, py_weighted_avg(score, credit) as weighted_avg_score
FROM student_grades
GROUP BY class_id
ORDER BY class_id;

+----------+--------------------+
| class_id | weighted_avg_score |
+----------+--------------------+
|      101 |               85.7 |
|      102 |                 89 |
|      103 |  83.57142857142857 |
+----------+--------------------+
```

### 窗口函数 (Window Functions)

Python UDAF 可以与窗口函数 (OVER 子句) 结合使用:

```sql
DROP FUNCTION IF EXISTS py_running_sum(DOUBLE);

CREATE AGGREGATE FUNCTION py_running_sum(DOUBLE)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "RunningSumUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class RunningSumUDAF:
    def __init__(self):
        self.total = 0.0
    
    @property
    def aggregate_state(self):
        return self.total
    
    def accumulate(self, value):
        if value is not None:
            self.total += value
    
    def merge(self, other_state):
        self.total += other_state
    
    def finish(self):
        return self.total
$$;

CREATE TABLE IF NOT EXISTS daily_sales_data (
    id INT,
    sales_date DATE,
    region VARCHAR(50),
    daily_sales DOUBLE
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO daily_sales_data VALUES 
(1, '2024-01-01', 'North', 1000.0),
(2, '2024-01-02', 'North', 1200.0),
(3, '2024-01-03', 'North', 900.0),
(4, '2024-01-04', 'North', 1500.0),
(5, '2024-01-05', 'North', 1100.0),
(6, '2024-01-01', 'South', 800.0),
(7, '2024-01-02', 'South', 950.0),
(8, '2024-01-03', 'South', 1100.0),
(9, '2024-01-04', 'South', 850.0),
(10, '2024-01-05', 'South', 1300.0);

SELECT 
    sales_date,
    daily_sales,
    py_running_sum(daily_sales) OVER (ORDER BY sales_date) as cumulative_sales
FROM daily_sales_data
ORDER BY sales_date;

+------------+-------------+------------------+
| sales_date | daily_sales | cumulative_sales |
+------------+-------------+------------------+
| 2024-01-01 |        1000 |             1800 |
| 2024-01-01 |         800 |             1800 |
| 2024-01-02 |        1200 |             3950 |
| 2024-01-02 |         950 |             3950 |
| 2024-01-03 |         900 |             5950 |
| 2024-01-03 |        1100 |             5950 |
| 2024-01-04 |        1500 |             8300 |
| 2024-01-04 |         850 |             8300 |
| 2024-01-05 |        1100 |            10700 |
| 2024-01-05 |        1300 |            10700 |
+------------+-------------+------------------+

SELECT 
    region,
    sales_date,
    daily_sales,
    py_running_sum(daily_sales) OVER (PARTITION BY region ORDER BY sales_date) as region_cumulative_sales
FROM daily_sales_data
ORDER BY region, sales_date;

+--------+------------+-------------+-------------------------+
| region | sales_date | daily_sales | region_cumulative_sales |
+--------+------------+-------------+-------------------------+
| North  | 2024-01-01 |        1000 |                    1000 |
| North  | 2024-01-02 |        1200 |                    2200 |
| North  | 2024-01-03 |         900 |                    3100 |
| North  | 2024-01-04 |        1500 |                    4600 |
| North  | 2024-01-05 |        1100 |                    5700 |
| South  | 2024-01-01 |         800 |                     800 |
| South  | 2024-01-02 |         950 |                    1750 |
| South  | 2024-01-03 |        1100 |                    2850 |
| South  | 2024-01-04 |         850 |                    3700 |
| South  | 2024-01-05 |        1300 |                    5000 |
+--------+------------+-------------+-------------------------+

SELECT 
    sales_date,
    daily_sales,
    py_running_sum(daily_sales) OVER (
        ORDER BY sales_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as last_3_days_sum
FROM daily_sales_data
ORDER BY sales_date;

+------------+-------------+-----------------+
| sales_date | daily_sales | last_3_days_sum |
+------------+-------------+-----------------+
| 2024-01-01 |        1000 |            1000 |
| 2024-01-01 |         800 |            1800 |
| 2024-01-02 |        1200 |            3000 |
| 2024-01-02 |         950 |            2950 |
| 2024-01-03 |         900 |            3050 |
| 2024-01-03 |        1100 |            2950 |
| 2024-01-04 |        1500 |            3500 |
| 2024-01-04 |         850 |            3450 |
| 2024-01-05 |        1100 |            3450 |
| 2024-01-05 |        1300 |            3250 |
+------------+-------------+-----------------+
```

### 复杂状态对象

Python UDAF 支持复杂的状态对象，可以使用字典、列表等数据结构:

```sql
DROP FUNCTION IF EXISTS py_stats(DOUBLE);

CREATE AGGREGATE FUNCTION py_stats(DOUBLE)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "StatsUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
import json
import math

class StatsUDAF:
    """返回多个统计指标的 JSON 字符串"""
    
    def __init__(self):
        self.count = 0
        self.sum = 0.0
        self.sum_sq = 0.0
        self.min_val = None
        self.max_val = None
    
    @property
    def aggregate_state(self):
        return {
            'count': self.count,
            'sum': self.sum,
            'sum_sq': self.sum_sq,
            'min': self.min_val,
            'max': self.max_val
        }
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum += value
            self.sum_sq += value * value
            if self.min_val is None or value < self.min_val:
                self.min_val = value
            if self.max_val is None or value > self.max_val:
                self.max_val = value
    
    def merge(self, other_state):
        self.count += other_state['count']
        self.sum += other_state['sum']
        self.sum_sq += other_state['sum_sq']
        if other_state['min'] is not None:
            if self.min_val is None or other_state['min'] < self.min_val:
                self.min_val = other_state['min']
        if other_state['max'] is not None:
            if self.max_val is None or other_state['max'] > self.max_val:
                self.max_val = other_state['max']
    
    def finish(self):
        if self.count == 0:
            return None
        mean = self.sum / self.count
        variance = (self.sum_sq / self.count) - (mean * mean)
        std_dev = math.sqrt(max(0, variance))
        
        result = {
            'count': self.count,
            'sum': self.sum,
            'mean': mean,
            'std_dev': std_dev,
            'min': self.min_val,
            'max': self.max_val,
            'range': self.max_val - self.min_val
        }
        return json.dumps(result)
$$;

DROP TABLE IF EXISTS products;

CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DOUBLE
) DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO products VALUES 
(1, 'Laptop', 'Electronics', 1200.0),
(2, 'Mouse', 'Electronics', 25.0),
(3, 'Keyboard', 'Electronics', 75.0),
(4, 'Monitor', 'Electronics', 300.0),
(5, 'Desk', 'Furniture', 250.0),
(6, 'Chair', 'Furniture', 150.0),
(7, 'Lamp', 'Furniture', 45.0),
(8, 'Notebook', 'Stationery', 5.0),
(9, 'Pen', 'Stationery', 2.0),
(10, 'Eraser', 'Stationery', 1.0);

SELECT category, py_stats(price) as statistics
FROM products
GROUP BY category
ORDER BY category;

+-------------+---------------------------------------------------------------------------------------------------------------------------------+
| category    | statistics                                                                                                                      |
+-------------+---------------------------------------------------------------------------------------------------------------------------------+
| Electronics | {"count": 4, "sum": 1600.0, "mean": 400.0, "std_dev": 473.3524057190372, "min": 25.0, "max": 1200.0, "range": 1175.0}           |
| Furniture   | {"count": 3, "sum": 445.0, "mean": 148.33333333333334, "std_dev": 83.69919686326477, "mi,n": 45.0, "max": 250.0, "range": 205.0} |
| Stationery  | {"count": 3, "sum": 8.0, "mean": 2.6666666666666665, "std_dev": 1.699673171197595, "min": 1.0, "max": 5.0, "range": 4.0}        |
+-------------+---------------------------------------------------------------------------------------------------------------------------------+
```

## 数据类型映射

Python UDAF 使用与 Python UDF 完全相同的数据类型映射规则，包括整数、浮点、字符串、日期时间、Decimal、布尔等所有类型。

**详细的数据类型映射关系请参考**: [Python UDF 文档 - 数据类型映射](python-udf-documentation.md#数据类型映射)

### NULL 值处理

- Doris 会将 SQL 中的 `NULL` 值映射为 Python 的 `None`
- 在 `accumulate` 方法中，需要检查参数是否为 `None`
- 聚合函数可以返回 `None` 表示结果为 `NULL`

## 实际应用场景

### 场景 1: 计算百分位数

```sql
DROP FUNCTION IF EXISTS py_percentile(DOUBLE, INT);

CREATE AGGREGATE FUNCTION py_percentile(DOUBLE, INT)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "PercentileUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class PercentileUDAF:
    """计算百分位数，第二个参数为百分位(0-100)"""
    
    def __init__(self):
        self.values = []
        self.percentile = 50  # 默认中位数
    
    @property
    def aggregate_state(self):
        return self.values
    
    def accumulate(self, value, percentile):
        if value is not None:
            self.values.append(value)
        if percentile is not None:
            self.percentile = percentile
    
    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)
    
    def finish(self):
        if not self.values:
            return None
        sorted_vals = sorted(self.values)
        n = len(sorted_vals)
        k = (n - 1) * (self.percentile / 100.0)
        f = int(k)
        c = k - f
        if f + 1 < n:
            return sorted_vals[f] + (sorted_vals[f + 1] - sorted_vals[f]) * c
        else:
            return sorted_vals[f]
$$;

DROP TABLE IF EXISTS api_logs;

CREATE TABLE IF NOT EXISTS api_logs (
    log_id INT,
    api_name VARCHAR(100),
    category VARCHAR(50),
    response_time DOUBLE
) DUPLICATE KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO api_logs VALUES 
(1, '/api/users', 'User', 120.5),
(2, '/api/users', 'User', 95.3),
(3, '/api/users', 'User', 150.0),
(4, '/api/users', 'User', 80.2),
(5, '/api/users', 'User', 200.8),
(6, '/api/orders', 'Order', 250.0),
(7, '/api/orders', 'Order', 180.5),
(8, '/api/orders', 'Order', 300.2),
(9, '/api/orders', 'Order', 220.0),
(10, '/api/products', 'Product', 50.0),
(11, '/api/products', 'Product', 60.5),
(12, '/api/products', 'Product', 45.0),
(13, '/api/products', 'Product', 70.2),
(14, '/api/products', 'Product', 55.8);

SELECT 
    category,
    py_percentile(response_time, 25) as p25,
    py_percentile(response_time, 50) as p50,
    py_percentile(response_time, 75) as p75,
    py_percentile(response_time, 95) as p95
FROM api_logs
GROUP BY category
ORDER BY category;

+----------+-------+-------+-------+-------+
| category | p25   | p50   | p75   | p95   |
+----------+-------+-------+-------+-------+
| Order    |   235 |   235 |   235 |   235 |
| Product  |  55.8 |  55.8 |  55.8 |  55.8 |
| User     | 120.5 | 120.5 | 120.5 | 120.5 |
+----------+-------+-------+-------+-------+
```

### 场景 2: 字符串去重合并

```sql
DROP FUNCTION IF EXISTS py_collect_set(STRING);

CREATE AGGREGATE FUNCTION py_collect_set(STRING)
RETURNS STRING
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "CollectSetUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class CollectSetUDAF:
    """去重收集字符串，返回逗号分隔的字符串"""
    
    def __init__(self):
        self.items = set()
    
    @property
    def aggregate_state(self):
        return list(self.items)
    
    def accumulate(self, value):
        if value is not None:
            self.items.add(value)
    
    def merge(self, other_state):
        if other_state:
            self.items.update(other_state)
    
    def finish(self):
        if not self.items:
            return None
        return ','.join(sorted(self.items))
$$;

DROP TABLE IF EXISTS page_views;

CREATE TABLE IF NOT EXISTS page_views (
    view_id INT,
    user_id INT,
    page_url VARCHAR(200),
    view_time DATETIME
) DUPLICATE KEY(view_id)
DISTRIBUTED BY HASH(view_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO page_views VALUES 
(1, 1001, '/home', '2024-01-01 10:00:00'),
(2, 1001, '/products', '2024-01-01 10:05:00'),
(3, 1001, '/home', '2024-01-01 10:10:00'),
(4, 1001, '/cart', '2024-01-01 10:15:00'),
(5, 1002, '/home', '2024-01-01 11:00:00'),
(6, 1002, '/about', '2024-01-01 11:05:00'),
(7, 1002, '/products', '2024-01-01 11:10:00'),
(8, 1003, '/products', '2024-01-01 12:00:00'),
(9, 1003, '/products', '2024-01-01 12:05:00'),
(10, 1003, '/cart', '2024-01-01 12:10:00'),
(11, 1003, '/checkout', '2024-01-01 12:15:00');

SELECT 
    user_id,
    py_collect_set(page_url) as visited_pages
FROM page_views
GROUP BY user_id
ORDER BY user_id;

+---------+---------------------------+
| user_id | visited_pages             |
+---------+---------------------------+
|    1001 | /cart,/home,/products     |
|    1002 | /about,/home,/products    |
|    1003 | /cart,/checkout,/products |
+---------+---------------------------+
```

### 场景 3: 移动平均

```sql
DROP TABLE IF EXISTS daily_sales;

CREATE TABLE IF NOT EXISTS daily_sales (
    id INT,
    date DATE,
    sales DOUBLE
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO daily_sales VALUES 
(1, '2024-01-01', 1000.0),
(2, '2024-01-02', 1200.0),
(3, '2024-01-03', 900.0),
(4, '2024-01-04', 1500.0),
(5, '2024-01-05', 1100.0),
(6, '2024-01-06', 1300.0),
(7, '2024-01-07', 1400.0),
(8, '2024-01-08', 1000.0),
(9, '2024-01-09', 1600.0),
(10, '2024-01-10', 1250.0);

SELECT 
    date,
    sales,
    py_avg(sales) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7days
FROM daily_sales
ORDER BY date;

+------------+-------+-------------------+
| date       | sales | moving_avg_7days  |
+------------+-------+-------------------+
| 2024-01-01 |  1000 |              1000 |
| 2024-01-02 |  1200 |              1100 |
| 2024-01-03 |   900 | 1033.333333333333 |
| 2024-01-04 |  1500 |              1150 |
| 2024-01-05 |  1100 |              1140 |
| 2024-01-06 |  1300 | 1166.666666666667 |
| 2024-01-07 |  1400 |              1200 |
| 2024-01-08 |  1000 |              1200 |
| 2024-01-09 |  1600 | 1257.142857142857 |
| 2024-01-10 |  1250 | 1307.142857142857 |
+------------+-------+-------------------+
```

### 场景 4: 自定义评分聚合

```sql
DROP FUNCTION IF EXISTS py_score_agg(DOUBLE, STRING);

CREATE AGGREGATE FUNCTION py_score_agg(DOUBLE, STRING)
RETURNS DOUBLE
PROPERTIES (
    "type" = "PYTHON_UDF",
    "symbol" = "ScoreAggUDAF",
    "runtime_version" = "3.10.12",
    "always_nullable" = "true"
)
AS $$
class ScoreAggUDAF:
    """根据评分类型计算加权分数"""
    
    def __init__(self):
        self.scores = {
            'quality': 0.0,
            'service': 0.0,
            'price': 0.0
        }
        self.counts = {
            'quality': 0,
            'service': 0,
            'price': 0
        }
    
    @property
    def aggregate_state(self):
        return {'scores': self.scores, 'counts': self.counts}
    
    def accumulate(self, score, score_type):
        if score is not None and score_type in self.scores:
            self.scores[score_type] += score
            self.counts[score_type] += 1
    
    def merge(self, other_state):
        for key in self.scores:
            self.scores[key] += other_state['scores'][key]
            self.counts[key] += other_state['counts'][key]
    
    def finish(self):
        # 加权平均: quality 50%， service 30%， price 20%
        weights = {'quality': 0.5, 'service': 0.3, 'price': 0.2}
        weighted_score = 0.0
        total_weight = 0.0
        
        for key, weight in weights.items():
            if self.counts[key] > 0:
                avg = self.scores[key] / self.counts[key]
                weighted_score += avg * weight
                total_weight += weight
        
        if total_weight == 0:
            return None
        return weighted_score / total_weight * 100
$$;

DROP TABLE IF EXISTS merchant_ratings;

CREATE TABLE IF NOT EXISTS merchant_ratings (
    rating_id INT,
    merchant_id INT,
    rating DOUBLE,
    rating_type VARCHAR(50)
) DUPLICATE KEY(rating_id)
DISTRIBUTED BY HASH(rating_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO merchant_ratings VALUES 
(1, 101, 4.5, 'quality'),
(2, 101, 4.2, 'service'),
(3, 101, 4.0, 'price'),
(4, 101, 4.8, 'quality'),
(5, 101, 4.3, 'service'),
(6, 102, 3.5, 'quality'),
(7, 102, 4.0, 'service'),
(8, 102, 4.5, 'price'),
(9, 102, 3.8, 'quality'),
(10, 103, 5.0, 'quality'),
(11, 103, 4.8, 'service'),
(12, 103, 4.2, 'price');

SELECT 
    merchant_id,
    py_score_agg(rating, rating_type) as rating_score
FROM merchant_ratings
GROUP BY merchant_id;

+-------------+-------------------+
| merchant_id | rating_score      |
+-------------+-------------------+
|         102 |             392.5 |
|         101 | 440.0000000000001 |
|         103 |               478 |
+-------------+-------------------+
```

## 性能优化建议

### 1. 优化状态对象大小

- 避免在状态对象中存储大量原始数据
- 尽量使用聚合后的统计量而不是完整数据列表
- 对于必须存储数据的场景(如中位数)，考虑采样或限制数据量

**不推荐如下用法**:
```python
class BadMedianUDAF:
    def __init__(self):
        self.all_values = []  # 可能非常大
    
    def accumulate(self, value):
        if value is not None:
            self.all_values.append(value)
```

### 2. 减少对象创建

- 复用状态对象，避免频繁创建新对象
- 使用原始数据类型而非复杂对象

### 3. 简化 merge 逻辑

- `merge` 方法在分布式环境下会被频繁调用
- 确保 merge 操作高效且正确

### 4. 使用增量计算

- 对于可以增量计算的指标(如平均值)，使用增量方式而非存储所有数据

### 5. 避免使用外部资源

- 不要在 UDAF 中访问数据库或外部 API
- 所有计算应基于传入的数据和内部状态

## 最佳实践

## 限制与注意事项

### 1. 性能考虑

- Python UDAF 性能低于内置聚合函数
- 建议用于逻辑复杂但数据量适中的场景
- 大数据量场景优先考虑使用内置函数或优化 UDAF 实现

### 2. 状态序列化

- `aggregate_state` 返回的对象**必须支持 pickle 序列化**
- 支持的类型：基本类型（int、float、str、bool）、列表、字典、元组、set，以及支持 pickle 序列化的自定义类实例
- 不支持：文件句柄、数据库连接、socket 连接、线程锁等不可 pickle 序列化的对象
- 如果状态对象不能被 pickle 序列化，函数执行时会报错
- **建议优先使用内置类型**（dict、list、tuple）作为状态对象，以确保兼容性和可维护性

### 3. 内存限制

- 状态对象会占用内存，避免存储过多数据
- 大状态对象会影响性能和稳定性

### 4. 函数命名

- 同一函数名在不同数据库中可重复定义
- 调用时需指定数据库名 (如 `db.func()`) 以避免歧义

### 5. 环境一致性

- 所有 BE 节点的 Python 环境必须一致
- 包括 Python 版本、依赖包版本、环境配置

## 常见问题 FAQ

### Q1: UDAF 和 UDF 的区别是什么?

A: 
- **UDF (标量函数)**: 处理单行数据，返回单行结果。每行调用一次函数
- **UDAF (聚合函数)**: 处理多行数据，返回单个聚合结果。配合 GROUP BY 使用

示例:
```sql
-- UDF: 每行都会调用
SELECT id, py_upper(name) FROM users;

-- UDAF: 每组调用一次
SELECT category, py_sum(amount) FROM sales GROUP BY category;
```

### Q2: aggregate_state 属性的作用是什么?

A: `aggregate_state` 用于在分布式环境下序列化和传输聚合状态:
- **序列化**: 将状态对象转换为可传输的格式，使用 **pickle 协议**进行序列化
- **合并**: 在不同节点间合并部分聚合结果
- **必须支持 pickle 序列化**: 可以返回基本类型、列表、字典、元组、set，以及支持 pickle 序列化的自定义类实例
- **禁止返回**: 文件句柄、数据库连接、socket 连接、线程锁等不可 pickle 序列化的对象，否则函数执行会报错
- **建议**: 优先使用内置类型（dict、list、tuple）以确保兼容性

### Q3: 如何调试 Python UDAF?

A: 
1. **使用小数据集测试**: 先在少量数据上验证逻辑
2. **日志输出**: 可以在方法中打印日志(会输出到 BE 日志)
3. **对比内置函数**: 与内置聚合函数对比结果验证正确性
4. **单元测试**: 将 UDAF 类单独提取出来进行单元测试

### Q4: UDAF 可以在窗口函数中使用吗?

A: 可以。Python UDAF 完全支持窗口函数 (OVER 子句)，包括:
- `PARTITION BY`: 分区
- `ORDER BY`: 排序
- `ROWS/RANGE`: 窗口框架

### Q5: 如何处理大数据量场景下的内存问题?

A: 
1. **使用增量计算**: 存储统计量而非原始数据
2. **采样**: 对于需要存储数据的场景(如中位数)，使用采样算法
3. **限制状态大小**: 设置状态对象的最大大小
4. **使用近似算法**: 如 HyperLogLog 估计基数，T-Digest 估计分位数

### Q6: merge 方法什么时候会被调用?

A: `merge` 方法在以下情况被调用:
- **分布式聚合**: 合并不同 BE 节点的部分聚合结果
- **并行处理**: 合并同一节点内不同线程的部分结果
- **窗口函数**: 合并窗口框架内的部分结果

因此 `merge` 的实现必须正确，否则会导致结果错误。

### Q7: 如何在 Python UDAF 中使用第三方库?

A: Python UDAF 可以使用第三方库，但需要由 DBA 或运维人员**在集群的所有 BE 节点上手动安装依赖**。具体步骤:

1. **在每个 BE 节点上安装依赖**:
   ```bash
   # 使用 pip 安装
   pip install numpy pandas requests
   
   # 或使用 conda 安装
   conda install numpy pandas requests -y
   ```

2. **在 UDAF 中导入并使用**:
   ```sql
   CREATE AGGREGATE FUNCTION py_stats(DOUBLE)
   RETURNS DOUBLE
   PROPERTIES (
       "type" = "PYTHON_UDF",
       "symbol" = "StatsUDAF",
       "runtime_version" = "3.10.12"
   )
   AS $$
   import numpy as np
   
   class StatsUDAF:
       def __init__(self):
           self.values = []
       
       def accumulate(self, value):
           if value is not None:
               self.values.append(value)
       
       def finish(self):
           return np.std(self.values) if self.values else None
   $$;
   ```

**注意事项**:
- **`pandas` 和 `pyarrow` 是强制依赖**，必须在所有 Python 环境中预先安装，否则 Python UDAF 无法运行
- 必须在**所有 BE 节点**上安装相同版本的依赖，否则会导致部分节点执行失败
- 安装路径要与 Python UDAF 使用的 Python 运行时环境一致
- 建议使用虚拟环境管理依赖，避免与系统 Python 环境冲突

---

## 相关文档

- [Python UDF 使用指南](python-udf-documentation.md)
- [Python UDTF 使用指南](python-udtf-documentation.md)
- [Python UDF 环境配置](python-udf-environment.md)
