# Iceberg 动态分区写入逻辑分析

## 一、概述

Doris 的 Iceberg 动态分区写入功能允许用户通过 `INSERT INTO` 语句向 Iceberg 表写入数据，系统会根据数据中的分区列值自动计算分区，并将数据写入对应的分区目录。整个过程分为 **FE（Frontend）准备阶段** 和 **BE（Backend）执行阶段**。

## 二、整体流程

```
SQL: INSERT INTO iceberg_table SELECT ...
    ↓
[FE] Parser → Analyzer → Planner
    ↓
[FE] IcebergTableSink.bindDataSink() - 准备分区规范信息
    ↓
[BE] VIcebergTableWriter.open() - 初始化分区转换器
    ↓
[BE] VIcebergTableWriter.write() - 动态计算分区并写入
    ↓
[BE] VIcebergPartitionWriter - 按分区组织数据文件
    ↓
[FE] IcebergTransaction.finishInsert() - 提交到 Iceberg
```

## 三、FE 端实现

### 3.1 分区规范信息准备

**文件**：`fe/fe-core/src/main/java/org/apache/doris/planner/IcebergTableSink.java`

**关键代码**（`bindDataSink` 方法）：

```java
// 1. 获取 Iceberg 表的 Schema
Table icebergTable = targetTable.getIcebergTable();
tSink.setSchemaJson(SchemaParser.toJson(icebergTable.schema()));

// 2. 传递分区规范信息
if (icebergTable.spec().isPartitioned()) {
    // 传递所有分区规范的 JSON（支持分区演进）
    tSink.setPartitionSpecsJson(
        Maps.transformValues(icebergTable.specs(), PartitionSpecParser::toJson));
    
    // 使用当前默认的 specId
    tSink.setPartitionSpecId(icebergTable.spec().specId());
}
```

**关键点**：
- ✅ **传递所有分区规范**：`setPartitionSpecsJson()` 传递了所有 `specId` 的分区规范 JSON，支持分区演进
- ✅ **使用默认 specId**：当前使用 `icebergTable.spec().specId()` 作为写入时的分区规范
- ✅ **Schema 序列化**：将 Iceberg Schema 序列化为 JSON 传递给 BE

### 3.2 事务管理

**文件**：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergTransaction.java`

**关键方法**：

1. **`beginInsert()`**：初始化 Iceberg 事务
   ```java
   this.table = IcebergUtils.getIcebergTable(dorisTable);
   this.transaction = table.newTransaction();
   ```

2. **`updateIcebergCommitData()`**：接收 BE 写入完成的数据文件信息
   ```java
   public void updateIcebergCommitData(List<TIcebergCommitData> commitDataList) {
       synchronized (this) {
           this.commitDataList.addAll(commitDataList);
       }
   }
   ```

3. **`finishInsert()`**：提交写入操作
   ```java
   private void updateManifestAfterInsert(TUpdateMode updateMode) {
       PartitionSpec spec = transaction.table().spec();
       FileFormat fileFormat = IcebergUtils.getFileFormat(transaction.table());
       
       // 将 BE 返回的 commitDataList 转换为 WriteResult
       WriteResult writeResult = IcebergWriterHelper
           .convertToWriterResult(fileFormat, spec, commitDataList);
       
       if (updateMode == TUpdateMode.APPEND) {
           commitAppendTxn(pendingResults);  // 动态分区写入走这里
       } else {
           // OVERWRITE 模式...
       }
   }
   ```

### 3.3 数据文件元数据转换

**文件**：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/helper/IcebergWriterHelper.java`

**关键方法**：`convertToWriterResult()`

```java
public static WriteResult convertToWriterResult(
        FileFormat format,
        PartitionSpec spec,
        List<TIcebergCommitData> commitDataList) {
    List<DataFile> dataFiles = new ArrayList<>();
    for (TIcebergCommitData commitData : commitDataList) {
        String location = commitData.getFilePath();
        long fileSize = commitData.getFileSize();
        long recordCount = commitData.getRowCount();
        
        // 提取分区值（BE 端计算好的）
        Optional<List<String>> partValues = Optional.empty();
        if (spec.isPartitioned()) {
            List<String> partitionValues = commitData.getPartitionValues();
            // 处理 "null" 字符串
            partitionValues = partitionValues.stream()
                .map(s -> s.equals("null") ? null : s)
                .collect(Collectors.toList());
            partValues = Optional.of(partitionValues);
        }
        
        // 构建 DataFile 对象
        DataFile dataFile = genDataFile(format, location, spec, partValues, stat);
        dataFiles.add(dataFile);
    }
    return WriteResult.builder().addDataFiles(dataFiles).build();
}
```

**关键点**：
- ✅ **分区值传递**：BE 端计算好的分区值通过 `TIcebergCommitData.partitionValues` 传递
- ✅ **NULL 值处理**：将字符串 "null" 转换为真正的 `null`
- ✅ **DataFile 构建**：使用 Iceberg 的 `DataFiles.builder()` 构建数据文件元数据

## 四、BE 端实现

### 4.1 分区转换器初始化

**文件**：`be/src/vec/sink/writer/iceberg/viceberg_table_writer.cpp`

**关键方法**：`open()`

```cpp
Status VIcebergTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    // 1. 解析 Schema
    _schema = iceberg::SchemaParser::from_json(
        _t_sink.iceberg_table_sink.schema_json);
    
    // 2. 解析分区规范
    std::string partition_spec_json =
        _t_sink.iceberg_table_sink.partition_specs_json[
            _t_sink.iceberg_table_sink.partition_spec_id];
    _partition_spec = iceberg::PartitionSpecParser::from_json(
        _schema, partition_spec_json);
    
    // 3. 构建分区列转换器
    _iceberg_partition_columns = _to_iceberg_partition_columns();
}
```

**关键方法**：`_to_iceberg_partition_columns()`

```cpp
std::vector<VIcebergTableWriter::IcebergPartitionColumn>
VIcebergTableWriter::_to_iceberg_partition_columns() {
    std::vector<IcebergPartitionColumn> partition_columns;
    
    // 构建 field_id -> column_index 映射
    std::unordered_map<int, int> id_to_column_idx;
    for (int i = 0; i < _schema->columns().size(); i++) {
        id_to_column_idx[_schema->columns()[i].field_id()] = i;
    }
    
    // 为每个分区字段创建转换器
    for (const auto& partition_field : _partition_spec->fields()) {
        int column_idx = id_to_column_idx[partition_field.source_id()];
        
        // 创建分区转换器（identity, bucket, truncate, day, hour 等）
        std::unique_ptr<PartitionColumnTransform> transform =
            PartitionColumnTransforms::create(
                partition_field,
                _vec_output_expr_ctxs[column_idx]->root()->data_type());
        
        partition_columns.emplace_back(
            partition_field,
            _vec_output_expr_ctxs[column_idx]->root()->data_type()->get_primitive_type(),
            column_idx,
            std::move(transform));
    }
    return partition_columns;
}
```

**关键点**：
- ✅ **分区转换器创建**：根据 `PartitionField` 的 `transform` 类型（identity, bucket, truncate, day, hour 等）创建对应的转换器
- ✅ **列索引映射**：通过 `field_id` 找到对应的输出列索引
- ✅ **支持多种转换**：支持 Iceberg 的所有分区转换函数

### 4.2 动态分区计算与写入

**文件**：`be/src/vec/sink/writer/iceberg/viceberg_table_writer.cpp`

**关键方法**：`write()`

```cpp
Status VIcebergTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    // 1. 执行表达式，获取输出 Block
    Block output_block;
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(
        _vec_output_expr_ctxs, block, &output_block, false));
    materialize_block_inplace(output_block);
    
    // 2. 应用分区转换，生成 transformed_block
    Block transformed_block;
    transformed_block.reserve(_iceberg_partition_columns.size());
    for (auto& iceberg_partition_columns : _iceberg_partition_columns) {
        transformed_block.insert(
            iceberg_partition_columns.partition_column_transform().apply(
                output_block, iceberg_partition_columns.source_idx()));
    }
    
    // 3. 对每一行数据计算分区并写入
    for (int i = 0; i < output_block.rows(); ++i) {
        // 3.1 获取分区数据
        PartitionData partition_data = _get_partition_data(&transformed_block, i);
        
        // 3.2 生成分区路径（如：dt=2025-01-25/region=bj）
        std::string partition_name = _partition_to_path(partition_data);
        
        // 3.3 查找或创建分区写入器
        auto writer_iter = _partitions_to_writers.find(partition_name);
        if (writer_iter == _partitions_to_writers.end()) {
            // 创建新的分区写入器
            auto writer = _create_partition_writer(&transformed_block, i, ...);
            _partitions_to_writers.insert({partition_name, writer});
        } else {
            // 检查文件大小，如果超过阈值则创建新文件
            if (writer_iter->second->written_len() > _target_file_size_bytes) {
                // 关闭旧文件，创建新文件
                writer_iter->second->close(Status::OK());
                auto writer = _create_partition_writer(&transformed_block, i, ...);
                _partitions_to_writers[partition_name] = writer;
            }
        }
        
        // 3.4 写入数据
        writer->write(output_block);
    }
}
```

**关键方法**：`_get_partition_data()`

```cpp
PartitionData VIcebergTableWriter::_get_partition_data(
        vectorized::Block* transformed_block, int position) {
    std::vector<std::any> values;
    values.reserve(_iceberg_partition_columns.size());
    
    int column_idx = 0;
    for (auto& iceberg_partition_column : _iceberg_partition_columns) {
        const ColumnWithTypeAndName& partition_column =
            transformed_block->get_by_position(column_idx);
        
        // 从 transformed_block 中提取分区值
        auto value = _get_iceberg_partition_value(
            iceberg_partition_column.partition_column_transform()
                .get_result_type()->get_primitive_type(),
            partition_column, position);
        values.emplace_back(value);
        ++column_idx;
    }
    return PartitionData(std::move(values));
}
```

**关键方法**：`_partition_to_path()`

```cpp
std::string VIcebergTableWriter::_partition_to_path(
        const doris::iceberg::StructLike& data) {
    std::stringstream ss;
    for (size_t i = 0; i < _iceberg_partition_columns.size(); i++) {
        auto& iceberg_partition_column = _iceberg_partition_columns[i];
        
        // 将分区值转换为字符串
        std::string value_string =
            iceberg_partition_column.partition_column_transform().to_human_string(
                iceberg_partition_column.partition_column_transform().get_result_type(),
                data.get(i));
        
        if (i > 0) {
            ss << "/";
        }
        // 格式：col_name=value
        ss << _escape(iceberg_partition_column.field().name())
           << '='
           << _escape(value_string);
    }
    return ss.str();  // 如：dt=2025-01-25/region=bj
}
```

**关键方法**：`_partition_values()`

```cpp
std::vector<std::string> VIcebergTableWriter::_partition_values(
        const doris::iceberg::StructLike& data) {
    std::vector<std::string> partition_values;
    partition_values.reserve(_iceberg_partition_columns.size());
    
    for (size_t i = 0; i < _iceberg_partition_columns.size(); i++) {
        auto& iceberg_partition_column = _iceberg_partition_columns[i];
        
        // 获取分区值的字符串表示（用于 DataFile 元数据）
        partition_values.emplace_back(
            iceberg_partition_column.partition_column_transform().get_partition_value(
                iceberg_partition_column.partition_column_transform().get_result_type(),
                data.get(i)));
    }
    return partition_values;  // 如：["2025-01-25", "bj"]
}
```

**关键点**：
- ✅ **逐行计算分区**：对每一行数据，根据分区列的值动态计算分区
- ✅ **分区路径生成**：格式为 `col1=val1/col2=val2`，符合 Hive/Iceberg 分区路径规范
- ✅ **分区写入器管理**：使用 `_partitions_to_writers` Map 管理不同分区的写入器
- ✅ **文件大小控制**：当文件大小超过 `_target_file_size_bytes` 时，自动创建新文件

### 4.3 分区写入器

**文件**：`be/src/vec/sink/writer/iceberg/viceberg_partition_writer.cpp`

**关键方法**：`_build_iceberg_commit_data()`

```cpp
TIcebergCommitData VIcebergPartitionWriter::_build_iceberg_commit_data() {
    TIcebergCommitData iceberg_commit_data;
    
    // 文件路径（包含分区路径）
    iceberg_commit_data.__set_file_path(
        fmt::format("{}/{}", _write_info.original_write_path, _get_target_file_name()));
    
    // 文件统计信息
    iceberg_commit_data.__set_row_count(_row_count);
    iceberg_commit_data.__set_file_size(_file_format_transformer->written_len());
    iceberg_commit_data.__set_file_content(TFileContent::DATA);
    
    // 分区值（关键！用于 FE 端构建 DataFile）
    iceberg_commit_data.__set_partition_values(_partition_values);
    
    return iceberg_commit_data;
}
```

**关键点**：
- ✅ **分区值保存**：在创建 `VIcebergPartitionWriter` 时，分区值被保存到 `_partition_values` 中
- ✅ **文件元数据**：关闭文件时，通过 `_build_iceberg_commit_data()` 构建完整的文件元数据
- ✅ **路径信息**：包含 `original_write_path`（Iceberg 表的数据目录）和文件名

## 五、分区转换器

### 5.1 支持的转换类型

**文件**：`be/src/vec/sink/writer/iceberg/partition_transformers.cpp`

**支持的转换函数**：

1. **Identity**：直接使用源列值
   ```cpp
   if (transform == "identity") {
       return std::make_unique<IdentityPartitionColumnTransform>(source_type);
   }
   ```

2. **Bucket**：哈希分桶
   ```cpp
   if (transform == "bucket") {
       return std::make_unique<BucketPartitionColumnTransform>(source_type, bucket_count);
   }
   ```

3. **Truncate**：截断（字符串、整数、小数）
   ```cpp
   if (name == "truncate") {
       // 根据类型创建对应的 Truncate 转换器
   }
   ```

4. **Day**：按天分区（Date/Datetime）
   ```cpp
   if (transform == "day") {
       return std::make_unique<DateDayPartitionColumnTransform>(source_type);
   }
   ```

5. **Hour**：按小时分区（Datetime）
   ```cpp
   if (transform == "hour") {
       return std::make_unique<TimestampHourPartitionColumnTransform>(source_type);
   }
   ```

6. **Void**：空分区（所有数据在同一分区）
   ```cpp
   if (transform == "void") {
       return std::make_unique<VoidPartitionColumnTransform>(source_type);
   }
   ```

### 5.2 转换器接口

```cpp
class PartitionColumnTransform {
public:
    // 应用转换：从源 Block 中提取并转换分区列
    virtual ColumnWithTypeAndName apply(const Block& block, int column_pos) = 0;
    
    // 获取转换后的数据类型
    virtual DataTypePtr get_result_type() const = 0;
    
    // 将分区值转换为人类可读的字符串（用于路径）
    virtual std::string to_human_string(const DataTypePtr type, const std::any& value) const;
    
    // 将分区值转换为字符串（用于 DataFile 元数据）
    virtual std::string get_partition_value(const DataTypePtr type, const std::any& value) const;
};
```

## 六、数据提交流程

### 6.1 BE → FE 数据传递

```
[BE] VIcebergPartitionWriter.close()
    ↓
[BE] _build_iceberg_commit_data() - 构建 TIcebergCommitData
    ↓
[BE] _state->add_iceberg_commit_datas(commit_data) - 添加到状态
    ↓
[FE] IcebergTransaction.updateIcebergCommitData() - 接收数据
    ↓
[FE] commitDataList 累积所有文件信息
```

### 6.2 FE → Iceberg 提交

```
[FE] IcebergTransaction.finishInsert()
    ↓
[FE] updateManifestAfterInsert(TUpdateMode.APPEND)
    ↓
[FE] IcebergWriterHelper.convertToWriterResult() - 转换为 WriteResult
    ↓
[FE] commitAppendTxn(pendingResults)
    ↓
[FE] AppendFiles.appendFile() - 添加数据文件
    ↓
[FE] AppendFiles.commit() - 提交到 Iceberg
```

**关键代码**：

```java
private void commitAppendTxn(List<WriteResult> pendingResults) {
    AppendFiles appendFiles = transaction.newAppend()
        .scanManifestsWith(ops.getThreadPoolWithPreAuth());
    
    if (branchName != null) {
        appendFiles = appendFiles.toBranch(branchName);
    }
    
    for (WriteResult result : pendingResults) {
        Preconditions.checkState(result.referencedDataFiles().length == 0,
                "Should have no referenced data files for append.");
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
    }
    
    appendFiles.commit();  // 提交到 Iceberg
}
```

## 七、关键数据结构

### 7.1 TIcebergCommitData（Thrift）

```thrift
struct TIcebergCommitData {
    1: required string file_path;           // 文件路径
    2: required i64 row_count;               // 行数
    3: required i64 file_size;              // 文件大小
    4: required TFileContent file_content;   // 文件类型
    5: optional list<string> partition_values; // 分区值列表
}
```

### 7.2 IcebergPartitionColumn

```cpp
struct IcebergPartitionColumn {
    doris::iceberg::PartitionField field;              // 分区字段定义
    PrimitiveType primitive_type;                      // 原始类型
    int source_idx;                                    // 源列索引
    std::unique_ptr<PartitionColumnTransform> transform; // 转换器
};
```

### 7.3 PartitionData

```cpp
class PartitionData : public doris::iceberg::StructLike {
private:
    std::vector<std::any> _partition_values;  // 分区值列表
};
```

## 八、与静态分区覆盖的对比

| 特性 | 动态分区写入 | 静态分区覆盖 |
|------|------------|------------|
| **语法** | `INSERT INTO table SELECT ...` | `INSERT OVERWRITE table PARTITION (col='val') SELECT ...` |
| **分区计算** | BE 端根据数据动态计算 | FE 端从 SQL 中解析 |
| **分区值来源** | 数据中的分区列值 | SQL 中的常量表达式 |
| **提交方式** | `AppendFiles.appendFile()` | `OverwriteFiles.overwriteByRowFilter()` |
| **文件删除** | 不删除 | 自动删除匹配分区的旧文件 |
| **适用场景** | 数据追加 | 分区数据更新 |

## 九、总结

### 9.1 核心流程

1. **FE 准备阶段**：
   - 解析 SQL，生成执行计划
   - 准备 Iceberg Schema 和 PartitionSpec 信息
   - 通过 Thrift 传递给 BE

2. **BE 执行阶段**：
   - 初始化分区转换器
   - 对每行数据应用分区转换
   - 动态计算分区路径
   - 按分区组织数据文件写入

3. **FE 提交阶段**：
   - 收集 BE 返回的文件元数据
   - 转换为 Iceberg DataFile
   - 使用 `AppendFiles` API 提交到 Iceberg

### 9.2 关键设计

- ✅ **分区转换器模式**：支持 Iceberg 的所有分区转换函数
- ✅ **动态分区计算**：BE 端逐行计算，无需预先知道分区值
- ✅ **分区写入器管理**：使用 Map 管理不同分区的写入器，支持文件大小控制
- ✅ **元数据传递**：通过 Thrift 协议传递文件路径、大小、行数、分区值等完整信息
- ✅ **分区演进支持**：传递所有分区规范，支持分区策略变更

### 9.3 性能优化

- ✅ **文件大小控制**：自动切分大文件，避免单个文件过大
- ✅ **分区写入器复用**：相同分区的数据复用同一个写入器
- ✅ **批量提交**：所有文件一次性提交到 Iceberg，减少事务开销

