# Iceberg 写回中分区演进适配分析

## 一、写回流程概述

Doris 在向 Iceberg 表写入数据时，需要：
1. **选择分区规范**：确定使用哪个 `specId` 的分区规范
2. **应用分区转换**：根据分区规范中的转换函数，将源列值转换为分区值
3. **生成分区路径**：根据分区值生成文件路径
4. **写入数据文件**：将数据写入对应的分区目录

## 二、FE 端实现（分区规范选择）

### 2.1 代码位置

**文件**：`fe/fe-core/src/main/java/org/apache/doris/planner/IcebergTableSink.java`

### 2.2 实现逻辑

```java
@Override
public void bindDataSink(Optional<InsertCommandContext> insertCtx)
        throws AnalysisException {
    TIcebergTableSink tSink = new TIcebergTableSink();
    Table icebergTable = targetTable.getIcebergTable();
    
    // schema
    tSink.setSchemaJson(SchemaParser.toJson(icebergTable.schema()));
    
    // partition spec
    if (icebergTable.spec().isPartitioned()) {
        // ✅ 传递所有分区规范的 JSON
        tSink.setPartitionSpecsJson(
            Maps.transformValues(icebergTable.specs(), PartitionSpecParser::toJson));
        
        // ⚠️ 只使用当前默认的 specId
        tSink.setPartitionSpecId(icebergTable.spec().specId());
    }
    
    // ... 其他配置
}
```

### 2.3 关键点

**优点**：
- ✅ **传递所有分区规范**：`setPartitionSpecsJson()` 传递了所有 `specId` 的分区规范 JSON
- ✅ **支持多分区规范**：BE 端可以根据需要选择不同的分区规范

**问题**：
- ⚠️ **只使用当前默认的 specId**：`icebergTable.spec().specId()` 返回的是当前默认的分区规范 ID
- ⚠️ **无法选择历史分区规范**：如果用户想要使用历史的分区规范（例如回退到旧的分区策略），当前实现不支持
- ⚠️ **没有考虑分区演化的场景**：如果表有多个分区规范，写入时总是使用最新的规范

### 2.4 分区规范选择逻辑

**当前实现**：
- 使用 `icebergTable.spec()` 获取当前默认的分区规范
- 该规范通常是表的最新分区规范（`specId` 最大）

**Iceberg 的分区规范选择**：
- Iceberg 表的 `spec()` 方法返回的是**当前默认的分区规范**
- 默认分区规范通常是 `specId` 最大的规范（即最新的规范）
- 新写入的数据文件应该使用默认分区规范

**结论**：
- ✅ **当前实现是正确的**：新写入的数据应该使用最新的分区规范
- ⚠️ **但缺少灵活性**：如果用户想要使用历史分区规范，当前实现不支持

## 三、BE 端实现（分区转换和应用）

### 3.1 代码位置

**文件**：`be/src/vec/sink/writer/iceberg/viceberg_table_writer.cpp`

### 3.2 初始化流程

```cpp
Status VIcebergTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    // 1. 解析 Schema
    _schema = iceberg::SchemaParser::from_json(
        _t_sink.iceberg_table_sink.schema_json);
    
    // 2. 根据 specId 选择分区规范
    std::string partition_spec_json =
        _t_sink.iceberg_table_sink
            .partition_specs_json[_t_sink.iceberg_table_sink.partition_spec_id];
    
    // 3. 解析分区规范
    if (!partition_spec_json.empty()) {
        _partition_spec = iceberg::PartitionSpecParser::from_json(
            _schema, partition_spec_json);
        
        // 4. 创建分区转换器
        _iceberg_partition_columns = _to_iceberg_partition_columns();
    }
    
    return Status::OK();
}
```

### 3.3 分区转换器创建

**代码位置**：`VIcebergTableWriter::_to_iceberg_partition_columns()`

```cpp
std::vector<VIcebergTableWriter::IcebergPartitionColumn>
VIcebergTableWriter::_to_iceberg_partition_columns() {
    std::vector<IcebergPartitionColumn> partition_columns;
    
    // 1. 建立 field_id 到 column_idx 的映射
    std::unordered_map<int, int> id_to_column_idx;
    for (int i = 0; i < _schema->columns().size(); i++) {
        id_to_column_idx[_schema->columns()[i].field_id()] = i;
    }
    
    // 2. 遍历分区规范中的所有分区字段
    for (const auto& partition_field : _partition_spec->fields()) {
        // 3. 根据 source_id 找到对应的列索引
        int column_idx = id_to_column_idx[partition_field.source_id()];
        
        // 4. 创建分区转换器
        std::unique_ptr<PartitionColumnTransform> partition_column_transform =
            PartitionColumnTransforms::create(
                partition_field, 
                _vec_output_expr_ctxs[column_idx]->root()->data_type());
        
        // 5. 保存分区列信息
        partition_columns.emplace_back(
            partition_field,
            _vec_output_expr_ctxs[column_idx]->root()->data_type()->get_primitive_type(),
            column_idx, 
            std::move(partition_column_transform));
    }
    
    return partition_columns;
}
```

**关键点**：
- ✅ **正确使用 specId**：根据 FE 传递的 `partition_spec_id` 从 `partition_specs_json` 中选择对应的分区规范
- ✅ **支持多分区规范**：可以处理不同的 `specId`，只要 FE 传递了对应的 JSON
- ✅ **正确映射源列**：通过 `source_id` 找到对应的源列

### 3.4 数据写入流程

**代码位置**：`VIcebergTableWriter::write()`

```cpp
Status VIcebergTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    // 1. 应用分区转换
    Block transformed_block;
    transformed_block.reserve(_iceberg_partition_columns.size());
    for (auto& iceberg_partition_columns : _iceberg_partition_columns) {
        transformed_block.insert(
            iceberg_partition_columns.partition_column_transform().apply(
                output_block, iceberg_partition_columns.source_idx()));
    }
    
    // 2. 对每一行数据计算分区值
    for (int i = 0; i < output_block.rows(); ++i) {
        // 2.1 获取分区数据
        PartitionData partition_data = _get_partition_data(&transformed_block, i);
        
        // 2.2 生成分区路径
        std::string partition_name = _partition_to_path(partition_data);
        
        // 2.3 创建或获取分区写入器
        auto writer_iter = _partitions_to_writers.find(partition_name);
        if (writer_iter == _partitions_to_writers.end()) {
            // 创建新的分区写入器
            auto writer = _create_partition_writer(&transformed_block, i, ...);
            _partitions_to_writers.insert({partition_name, writer});
        }
        
        // 2.4 写入数据
        // ...
    }
}
```

**关键点**：
- ✅ **正确应用分区转换**：对每个分区字段应用对应的转换函数
- ✅ **按分区组织数据**：相同分区的数据写入同一个文件
- ✅ **支持多分区规范**：不同 `specId` 的分区规范会产生不同的分区路径

## 四、分区转换器实现

### 4.1 代码位置

**文件**：`be/src/vec/sink/writer/iceberg/partition_transformers.cpp`

### 4.2 支持的转换函数

**代码位置**：`PartitionColumnTransforms::create()`

```cpp
std::unique_ptr<PartitionColumnTransform> PartitionColumnTransforms::create(
        const doris::iceberg::PartitionField& field, 
        const DataTypePtr& source_type) {
    auto& transform = field.transform();
    
    // 1. 处理带参数的转换（bucket[N], truncate[W]）
    static const std::regex has_width(R"((\w+)\[(\d+)\])");
    std::smatch width_match;
    if (std::regex_match(transform, width_match, has_width)) {
        std::string name = width_match[1];
        int parsed_width = std::stoi(width_match[2]);
        
        if (name == "truncate") {
            // 支持多种类型的 truncate
            switch (source_type->get_primitive_type()) {
            case TYPE_INT: return std::make_unique<IntegerTruncatePartitionColumnTransform>(...);
            case TYPE_BIGINT: return std::make_unique<BigintTruncatePartitionColumnTransform>(...);
            case TYPE_VARCHAR: case TYPE_CHAR: case TYPE_STRING:
                return std::make_unique<StringTruncatePartitionColumnTransform>(...);
            // ... 支持 Decimal 类型
            }
        } else if (name == "bucket") {
            // 支持多种类型的 bucket
            switch (source_type->get_primitive_type()) {
            case TYPE_INT: return std::make_unique<IntBucketPartitionColumnTransform>(...);
            case TYPE_BIGINT: return std::make_unique<BigintBucketPartitionColumnTransform>(...);
            case TYPE_VARCHAR: case TYPE_CHAR: case TYPE_STRING:
                return std::make_unique<StringBucketPartitionColumnTransform>(...);
            case TYPE_DATEV2: return std::make_unique<DateBucketPartitionColumnTransform>(...);
            case TYPE_DATETIMEV2: return std::make_unique<TimestampBucketPartitionColumnTransform>(...);
            // ... 支持 Decimal 类型
            }
        }
    }
    
    // 2. 处理简单转换（identity, year, month, day, hour, void）
    if (transform == "identity") {
        return std::make_unique<IdentityPartitionColumnTransform>(source_type);
    } else if (transform == "year") {
        switch (source_type->get_primitive_type()) {
        case TYPE_DATEV2: return std::make_unique<DateYearPartitionColumnTransform>(source_type);
        case TYPE_DATETIMEV2: return std::make_unique<TimestampYearPartitionColumnTransform>(source_type);
        }
    } else if (transform == "month") {
        // ... 类似处理
    } else if (transform == "day") {
        // ... 类似处理
    } else if (transform == "hour") {
        // ... 类似处理
    } else if (transform == "void") {
        return std::make_unique<VoidPartitionColumnTransform>(source_type);
    }
    
    throw doris::Exception("Unsupported transform: " + transform);
}
```

### 4.3 支持的转换函数列表

| 转换函数 | 支持的数据类型 | 实现类 |
|---------|---------------|--------|
| `identity` | 所有类型 | `IdentityPartitionColumnTransform` |
| `year` | `DATEV2`, `DATETIMEV2` | `DateYearPartitionColumnTransform`, `TimestampYearPartitionColumnTransform` |
| `month` | `DATEV2`, `DATETIMEV2` | `DateMonthPartitionColumnTransform`, `TimestampMonthPartitionColumnTransform` |
| `day` | `DATEV2`, `DATETIMEV2` | `DateDayPartitionColumnTransform`, `TimestampDayPartitionColumnTransform` |
| `hour` | `DATETIMEV2` | `TimestampHourPartitionColumnTransform` |
| `bucket[N]` | `INT`, `BIGINT`, `VARCHAR/CHAR/STRING`, `DATEV2`, `DATETIMEV2`, `DECIMAL*` | `*BucketPartitionColumnTransform` |
| `truncate[W]` | `INT`, `BIGINT`, `VARCHAR/CHAR/STRING`, `DECIMAL*` | `*TruncatePartitionColumnTransform` |
| `void` | 所有类型 | `VoidPartitionColumnTransform` |

### 4.4 关键点

**优点**：
- ✅ **支持所有 Iceberg 标准转换函数**：identity、year、month、day、hour、bucket、truncate、void
- ✅ **支持多种数据类型**：整数、字符串、日期、时间戳、Decimal 等
- ✅ **类型安全**：根据源数据类型选择对应的转换器实现

**潜在问题**：
- ⚠️ **不支持自定义转换函数**：如果 Iceberg 表使用了自定义转换函数，会抛出异常
- ⚠️ **错误处理**：如果转换函数不支持，会抛出异常，可能导致整个写入任务失败

## 五、分区演进适配分析

### 5.1 当前实现的适配情况

#### 5.1.1 支持分区演进（✅）

**场景**：表有多个分区规范（分区演进）

**示例**：
```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts));

-- 分区演进：添加 bucket 分区字段（specId = 1）
ALTER TABLE events ADD PARTITION FIELD bucket(16, id);
```

**当前实现**：
1. ✅ FE 端传递所有分区规范的 JSON：`setPartitionSpecsJson()` 传递了所有 `specId` 的规范
2. ✅ FE 端使用最新的 specId：`setPartitionSpecId(icebergTable.spec().specId())` 使用最新的规范
3. ✅ BE 端根据 specId 选择分区规范：从 `partition_specs_json` 中选择对应的规范
4. ✅ BE 端正确应用分区转换：根据选择的规范创建对应的转换器

**结论**：✅ **当前实现支持分区演进**

#### 5.1.2 使用最新分区规范（✅）

**场景**：新写入的数据使用最新的分区规范

**当前实现**：
- FE 端使用 `icebergTable.spec().specId()` 获取最新的 specId
- BE 端根据该 specId 选择对应的分区规范
- 新写入的数据文件使用最新的分区规范

**结论**：✅ **当前实现正确使用最新分区规范**

### 5.2 潜在问题

#### 5.2.1 无法选择历史分区规范（⚠️）

**问题描述**：
当前实现总是使用最新的分区规范，无法选择历史的分区规范。

**场景**：
```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts));

-- 分区演进：添加 bucket 分区字段（specId = 1）
ALTER TABLE events ADD PARTITION FIELD bucket(16, id);

-- 用户想要回退到旧的分区规范（specId = 0）
-- 当前实现不支持
```

**影响**：
- ⚠️ 如果用户想要使用历史分区规范，当前实现不支持
- ⚠️ 限制了分区演进的灵活性

**改进建议**：
- 可以考虑添加一个配置选项，允许用户指定使用哪个 `specId`
- 或者提供 SQL 语法，允许用户指定分区规范

#### 5.2.2 分区规范验证不足（⚠️）

**问题描述**：
当前实现没有验证选择的分区规范是否与表结构兼容。

**场景**：
```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts));

-- Schema 演进：删除列 id
ALTER TABLE events DROP COLUMN id;

-- 如果使用 specId = 1 的分区规范（包含 bucket(16, id)），会失败
```

**当前实现**：
- ⚠️ 没有验证分区规范中的源列是否存在于当前 Schema 中
- ⚠️ 如果源列不存在，会在 BE 端运行时失败

**改进建议**：
- 在 FE 端验证分区规范与 Schema 的兼容性
- 如果分区规范引用的列不存在，应该提前报错

#### 5.2.3 分区转换错误处理（⚠️）

**问题描述**：
如果分区转换失败，会导致整个写入任务失败。

**场景**：
```sql
-- 表定义
CREATE TABLE events (
    id BIGINT,
    category STRING
) PARTITIONED BY (truncate(4, category));

-- 如果 category 列的值长度小于 4，truncate 转换可能失败
```

**当前实现**：
- ⚠️ 如果转换失败，会抛出异常，导致整个写入任务失败
- ⚠️ 没有部分失败的处理机制

**改进建议**：
- 可以考虑对转换失败的行进行特殊处理（例如写入 NULL 分区或跳过该行）
- 或者提供更详细的错误信息，帮助用户定位问题

## 六、具体示例

### 6.1 示例1：分区演进（添加分区字段）

**表定义**：
```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts));

-- 分区演进：添加 bucket 分区字段（specId = 1）
ALTER TABLE events ADD PARTITION FIELD bucket(16, id);
```

**写回流程**：
1. **FE 端**：
   - 获取所有分区规范：`specId = 0`（`day(ts)`）和 `specId = 1`（`day(ts), bucket(16, id)`）
   - 传递所有规范的 JSON：`setPartitionSpecsJson({0: "...", 1: "..."})`
   - 使用最新的 specId：`setPartitionSpecId(1)`

2. **BE 端**：
   - 根据 specId = 1 选择分区规范：`day(ts), bucket(16, id)`
   - 创建分区转换器：
     - `DateDayPartitionColumnTransform` 用于 `ts` 列
     - `BigintBucketPartitionColumnTransform` 用于 `id` 列
   - 应用转换并写入数据

**结果**：
- ✅ 新写入的数据使用 `specId = 1` 的分区规范
- ✅ 历史数据（`specId = 0`）保持不变
- ✅ 支持分区演进

### 6.2 示例2：分区演进（删除分区字段）

**表定义**：
```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts), bucket(16, id));

-- 分区演进：删除 bucket 分区字段（specId = 1）
ALTER TABLE events DROP PARTITION FIELD bucket(16, id);
```

**写回流程**：
1. **FE 端**：
   - 获取所有分区规范：`specId = 0`（`day(ts), bucket(16, id)`）和 `specId = 1`（`day(ts)`）
   - 传递所有规范的 JSON
   - 使用最新的 specId：`setPartitionSpecId(1)`

2. **BE 端**：
   - 根据 specId = 1 选择分区规范：`day(ts)`
   - 创建分区转换器：
     - `DateDayPartitionColumnTransform` 用于 `ts` 列
   - 应用转换并写入数据

**结果**：
- ✅ 新写入的数据使用 `specId = 1` 的分区规范（只有 `day(ts)`）
- ✅ 历史数据（`specId = 0`）保持不变
- ✅ 支持分区演进

### 6.3 示例3：分区演进（修改分区字段）

**表定义**：
```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (bucket(16, id));

-- 分区演进：修改 bucket 数量（specId = 1）
ALTER TABLE events DROP PARTITION FIELD bucket(16, id);
ALTER TABLE events ADD PARTITION FIELD bucket(32, id);
```

**写回流程**：
1. **FE 端**：
   - 获取所有分区规范：`specId = 0`（`bucket(16, id)`）和 `specId = 1`（`bucket(32, id)`）
   - 传递所有规范的 JSON
   - 使用最新的 specId：`setPartitionSpecId(1)`

2. **BE 端**：
   - 根据 specId = 1 选择分区规范：`bucket(32, id)`
   - 创建分区转换器：
     - `BigintBucketPartitionColumnTransform`（bucket_num = 32）用于 `id` 列
   - 应用转换并写入数据

**结果**：
- ✅ 新写入的数据使用 `specId = 1` 的分区规范（`bucket(32, id)`）
- ✅ 历史数据（`specId = 0`）保持不变（`bucket(16, id)`）
- ✅ 支持分区演进

## 七、与其他引擎的对比（Spark）

### 7.1 Spark 对 specId 的支持情况

**结论**：❌ **Spark 不支持直接指定 specId**

#### 7.1.1 Spark 的默认行为

Spark 在写入 Iceberg 表时，**总是使用最新的分区规范**（`table.spec().specId()`），与 Doris 当前实现相同。

**示例**：
```python
# Spark 写入 Iceberg 表
df.write.format("iceberg").mode("append").save("my_catalog.my_db.my_table")

# 或者使用 SQL
spark.sql("""
    INSERT INTO my_catalog.my_db.my_table
    SELECT * FROM source_table
""")
```

**行为**：
- ✅ 自动使用最新的分区规范（`specId` 最大）
- ❌ 无法指定使用历史的分区规范

#### 7.1.2 Spark 的替代方案

虽然 Spark 不支持直接指定 specId，但可以通过以下方式实现类似的功能：

**方案1：使用时间旅行查询（Time Travel）**

**场景**：查询特定快照的数据，该快照可能使用了历史的分区规范

**示例**：
```python
# 查询特定快照的数据
df = spark.read.format("iceberg") \
    .option("snapshot-id", 1234567890123456789) \
    .load("my_catalog.my_db.my_table")

# 或者使用 SQL
spark.sql("""
    SELECT * FROM my_catalog.my_db.my_table 
    VERSION AS OF 1234567890123456789
""")
```

**说明**：
- ✅ 可以查询特定快照的数据
- ⚠️ 但写入时仍然使用最新的分区规范

**方案2：使用增量查询（Incremental Read）**

**场景**：读取特定快照范围内的数据

**示例**：
```python
# 增量读取
df = spark.read.format("iceberg") \
    .option("start-snapshot-id", 1234567890123456789) \
    .option("end-snapshot-id", 1234567890123456790) \
    .load("my_catalog.my_db.my_table")
```

**说明**：
- ✅ 可以读取特定快照范围的数据
- ⚠️ 但写入时仍然使用最新的分区规范

**方案3：通过 Iceberg API 直接操作**

**场景**：需要精确控制分区规范时，可以使用 Iceberg Java API

**示例**：
```java
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;

// 获取表
Table table = catalog.loadTable(TableIdentifier.of("my_db", "my_table"));

// 获取特定的分区规范
PartitionSpec spec = table.specs().get(0);  // 使用 specId = 0

// 使用该规范创建写入器
// ... 使用 Iceberg API 写入数据
```

**说明**：
- ✅ 可以精确控制分区规范
- ⚠️ 需要直接使用 Iceberg API，不是 Spark 的标准接口

### 7.2 对比总结

| 特性 | Spark | Doris（当前实现） |
|------|-------|------------------|
| 支持直接指定 specId | ❌ 不支持 | ❌ 不支持 |
| 默认使用最新 specId | ✅ 是 | ✅ 是 |
| 传递所有分区规范 | ✅ 是（内部） | ✅ 是 |
| 支持分区演进 | ✅ 是 | ✅ 是 |
| 时间旅行查询 | ✅ 支持 | ✅ 支持 |
| 通过 API 指定 specId | ✅ 支持（Java API） | ⚠️ 不支持 |

### 7.3 结论

**Spark 和 Doris 的行为一致**：
- ✅ 都默认使用最新的分区规范
- ✅ 都不支持直接指定 specId
- ✅ 都支持分区演进的核心功能

**如果需要指定 specId**：
- Spark：需要使用 Iceberg Java API
- Doris：当前不支持，可以考虑添加该功能

## 八、总结

### 8.1 当前实现的优点

1. ✅ **支持分区演进**：
   - FE 端传递所有分区规范的 JSON
   - BE 端根据 specId 选择对应的分区规范
   - 新写入的数据使用最新的分区规范

2. ✅ **支持所有标准转换函数**：
   - identity、year、month、day、hour、bucket、truncate、void
   - 支持多种数据类型

3. ✅ **正确应用分区转换**：
   - 根据分区规范创建对应的转换器
   - 正确映射源列到分区字段

4. ✅ **与 Spark 行为一致**：
   - 都默认使用最新的分区规范
   - 都支持分区演进的核心功能

### 8.2 潜在问题和改进建议

1. ⚠️ **无法选择历史分区规范**：
   - **问题**：总是使用最新的分区规范
   - **对比**：Spark 也不支持直接指定 specId
   - **改进**：可以考虑添加配置选项，允许用户指定 specId（类似 Spark 通过 API 的方式）

2. ⚠️ **分区规范验证不足**：
   - **问题**：没有验证分区规范与 Schema 的兼容性
   - **改进**：在 FE 端添加验证逻辑

3. ⚠️ **分区转换错误处理**：
   - **问题**：转换失败会导致整个写入任务失败
   - **改进**：提供更详细的错误信息和部分失败处理机制

### 8.3 结论

**当前实现基本支持分区演进**：
- ✅ 可以正确处理多个分区规范
- ✅ 新写入的数据使用最新的分区规范
- ✅ 支持所有标准的转换函数
- ✅ 与 Spark 行为一致

**但存在一些限制**：
- ⚠️ 无法选择历史分区规范（与 Spark 相同）
- ⚠️ 分区规范验证不足
- ⚠️ 错误处理可以改进

**总体评价**：
- ✅ **适配情况良好**：基本支持分区演进的核心功能，与 Spark 行为一致
- ⚠️ **可以进一步优化**：增加灵活性和错误处理

