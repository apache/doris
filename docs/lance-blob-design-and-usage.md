# Doris Lance Blob Resource 读取实现

本文按当前工作区源码说明 Lance Blob 的读取实现，代码状态截至 2026-11-25。讨论对象是 FE 下发的 `TLanceBlobResource`、BE 的 `QueryContext` 和 `LanceBlobReader`，以及 `ColumnFile` 中的 FILE 值和两组数值 ID。

核心路径如下：

```text
FE 规划 Lance 扫描
    -> 为 Blob 来源列分配查询内 source ID
    -> 下发完整 lance_blob_resources
    -> BE QueryContext 保存 Resource，创建 LanceBlobReader

BE 扫描
    -> Lance descriptor + 原生 _rowid
    -> FILE 公开字段 + blob_source_id + blob_row_id
    -> 随 Block 过滤、复制、重排或跨 BE 传输

执行 fl_get_binary(FILE)
    -> 按 source ID 查 Resource
    -> 打开固定版本的 dataset
    -> 按列名和原生 row ID 获取 Blob 句柄
    -> 读取内容，返回 Nullable(VARBINARY)
```

全文的源码链接指向当前仓库文件。源码行为与待讨论、待验收事项分别列出；当前版本兼容和跨 BE 行为尚未完成运行验收。

## 1 SQL 行为与支持范围

### 1.1 两个读取入口

```sql
SET enable_file_scanner_v2 = true;

SELECT image FROM lance_catalog.demo.assets;
-- 输出 FILE。

SELECT fl_get_binary(image) FROM lance_catalog.demo.assets;
-- 显式读取内容，输出 Nullable(VARBINARY)。
```

FE 的 `LanceTypeConverter` 将带有 `lance.blob.v2` 扩展名、物理类型为 Struct 的顶层列映射为 FILE。若复杂类型的子字段包含 Blob，当前返回 UNSUPPORTED。

Blob 的扫描输出由 `build_lance_blob_descriptors()` 构造，`inline` 保持 NULL。即使底层 Blob 的 kind 为 Inline，也不会在这里把内容填入 `FILE.inline`。

`fl_get_binary()` 自行处理外层 NULL 和已有 inline 内容；需要读取 Lance 内容时才使用 QueryContext 中的 Reader。

源码入口：

- [LanceTypeConverter.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/metadata/LanceTypeConverter.java)：`toDorisType()`、`extensionType()`、`containsBlob()`。
- [lance_blob_column_reader.cpp](../be/src/format_v2/lance/lance_blob_column_reader.cpp)：`build_lance_blob_descriptors()`。
- [function_fl_get_binary.cpp](../be/src/exprs/function/function_fl_get_binary.cpp)：`FunctionFlGetBinary::execute_impl()`。

### 1.2 当前实现的边界

| 项目 | 当前行为 |
| --- | --- |
| Lance 列类型 | 顶层 Blob v2 映射为 FILE |
| 扫描器 | Blob 投影要求 FileScannerV2 |
| dataset 版本 | Blob 读取要求固定的正数版本 |
| 存储位置 | 要求远端存储，拒绝本地 Blob 来源 |
| 内容读取时机 | 在 fl_get_binary 执行时读取 |
| 内容读取方式 | 当前 C++ 调用链同步执行 SDK 读取 |
| 单值长度 | 读取前检查 VARBINARY 的 UInt32 长度表示范围 |
| 引用有效域 | 本次查询；不同 BE 使用 FE 下发的相同 Resource 定义 |
| 内容缓存 | Reader 缓存 dataset，不缓存 payload |

当前函数执行位置由物理计划决定。SQL 中有 LIMIT、TopN 或 CASE，不足以推断函数只读取最终保留行或命中分支；这些行为需要检查实际计划及表达式执行过程。

## 2 当前数据结构

### 2.1 TLanceBlobResource

[PlanNodes.thrift](../gensrc/thrift/PlanNodes.thrift) 中定义：

```thrift
struct TLanceBlobResource {
    1: optional string dataset_uri
    2: optional i64 version
    3: optional string column_name
    4: optional map<string, string> storage_options
}
```

| 字段 | 用途 |
| --- | --- |
| dataset_uri | 传给 SDK，用于打开 dataset |
| version | 指定本次查询固定的 dataset 快照 |
| column_name | 指定 row ID 对应的 Blob 列 |
| storage_options | 传给 Lance 的原生存储键值选项 |

Thrift 使用 optional 表达字段存在性；BE 初始化仍检查 URI、版本、列名是否完整。不存在的 URI、非正数版本、空列名不能形成可读取 Resource。

Resource 中不保存 SDK 指针，也不保存每行 row ID。它描述一个来源列的公共读取配置。

### 2.2 查询级资源表

[PaloInternalService.thrift](../gensrc/thrift/PaloInternalService.thrift) 的 `TPipelineFragmentParams` 中定义：

```thrift
48: optional map<i32, PlanNodes.TLanceBlobResource> lance_blob_resources
```

FE 使用 `Map<Integer, TLanceBlobResource>`，BE 使用 `std::map<int32_t, TLanceBlobResource>`。

示意：

```text
source ID    Resource
1            dataset A，version 42，image，options A
2            dataset A，version 42，audio，options A
3            dataset B，version 18，image，options B
```

source ID 是查询内的编号。source ID 1 在两个查询中可以表示不同来源。它不是表 ID、ScanNode ID，也不是对象地址。

FE 当前按“扫描节点中的 Blob 列”分配 ID。同一个 dataset 被两个节点扫描时，可以得到不同 ID；没有按 Resource 内容跨扫描节点合并编号。

### 2.3 ColumnFile

[column_file.h](../be/src/core/column/column_file.h) 中的相关成员为：

```cpp
FileColumns _columns;
IColumn::WrappedPtr _blob_source_ids;
IColumn::WrappedPtr _blob_row_ids;
```

`_columns` 包含 FILE 的六个公开字段。两组私有 ID 分别使用 UInt32、UInt64 列。

```text
ColumnFile
  +-- uri
  +-- offset
  +-- size
  +-- content_type
  +-- checksum
  +-- inline
  +-- _blob_source_ids，可为空
  +-- _blob_row_ids，可为空
```

ColumnFile 中没有 Resource 对象、QueryContext、存储选项或 dataset 句柄。读取配置保存在 QueryContext，列只保存数值形式的引用。

### 2.4 单值 File

单值结构实际定义在 [field.h](../be/src/core/field.h)：

```cpp
struct File : public FieldVector {
    using FieldVector::FieldVector;
    uint32_t blob_source_id = 0;
    uint64_t blob_row_id = 0;
};
```

`FieldVector` 表达公开字段，额外两个成员表达内部引用。ColumnFile 的 `get()`、`insert()`、`insert_from()` 等路径需要让公开字段与 ID 一起流转。

[file_value.h](../be/src/core/value/file_value.h) 提供的是 FILE 值校验等函数声明；它不是上述单值结构的定义位置。

### 2.5 三种行身份

| 名称 | 含义 | 保存位置 |
| --- | --- | --- |
| blob_source_id | 本次查询资源表中的来源列编号 | File、ColumnFile、BlobRequest |
| blob_row_id | Lance 返回的原生 row ID | File、ColumnFile、BlobRequest |
| output_row_index | 当前 read_blob 调用中结果回填的行位置 | BlobRequest |

`blob_source_id == 0` 表示没有绑定来源。不能用 `blob_row_id == 0` 判断引用是否有效，因为原生 row ID 0 可以是有效行。

排序和过滤改变 Block 中的行位置，不改变该行所携带的 Lance row ID。`output_row_index` 也不要求等于 Lance row ID。

## 3 FE 如何形成 Resource

### 3.1 扫描节点先准备公共配置

`LanceScanNode::createScanRangeLocations()` 把规划出的 dataset URI、版本写入公共 `TLanceScanParams`，并在父类生成扫描范围之后填入 Lance 存储选项。

投影 Blob 列由 `blobReadColumns()` 计算：它检查规划 schema 中的字段，同时考虑物化列和延迟物化需要的列。Resource 使用真实 Lance 列名。

TVF 的 `createScanRangeLocations()` 同样准备 URI、版本和 Lance 存储选项。Blob TVF 检查本地文件类型、远端 URI 形式和固定版本。

源码入口：

- [LanceScanNode.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceScanNode.java)：`projectedColumnNames()`、`blobReadColumns()`、`createScanRangeLocations()`。
- [TVFScanNode.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/tvf/source/TVFScanNode.java)：`blobReadColumns()`、`createScanRangeLocations()`。

### 3.2 LanceBlobResourcePlanner 分配 ID

[LanceBlobResourcePlanner.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceBlobResourcePlanner.java) 是资源表的生成入口。

`plan(scanNodes, beExecVersion)` 的执行顺序：

1. 收集 FileQueryScanNode，用 TreeMap 按扫描节点 ID 排序。
2. 从 source ID 1 开始，调用各扫描节点的 `assignBlobSourceIds()`。
3. Lance 扫描调用 `assign()`，清理原映射，并对 Blob 列名用 TreeSet 排序、去重。
4. 将“列名 -> source ID”写入该扫描节点的 `TLanceScanParams.blob_read_source_ids`。
5. 如果当前查询执行版本小于 Blob 最低版本，拒绝生成 Blob Resource。
6. 遍历映射，将 URI、版本、列名及存储选项复制到 TLanceBlobResource。
7. 返回“source ID -> Resource”的完整表。

`assign()` 在分配前要求远端 URI 和正数版本；当 nextSourceId 非正数或等于 Integer.MAX_VALUE 时拒绝继续分配。

同一来源的扫描参数和 Resource 从同一份 FE 规划配置产生。扫描端不需要再注册一套 BE 本地 ID。

### 3.3 两个方向的映射

```text
扫描端：
  TLanceScanParams.blob_read_source_ids
  image -> 1

消费端：
  TPipelineFragmentParams.lance_blob_resources
  1 -> {dataset_uri, version, image, storage_options}
```

第一个映射用于给扫描输出赋 source ID，第二个映射用于消费 FILE 时取得完整配置。

BlobReader 不使用 source ID 反查 ScanNode 参数，也不回 FE 查 Catalog 或表对象。

## 4 FE 如何把资源表发到每个 BE

### 4.1 主执行路径

[ThriftPlansBuilder.java](../fe/fe-core/src/main/java/org/apache/doris/qe/runtime/ThriftPlansBuilder.java) 的 `plansToThrift()` 在 fragment 序列化前调用 Resource Planner，并传入本次查询选项中的执行版本。

```text
plansToThrift
  -> 收集各分布式计划的 scanNodes
  -> LanceBlobResourcePlanner.plan
  -> 生成各 BE 的 fragment 参数列表
  -> 反转为最终发送顺序
  -> 为每个 BE 的第一个 fragment 设置完整资源表
```

实际赋值为：

```java
fragmentParams.setLanceBlobResources(lanceBlobResources);
```

这个位置在按 BE 遍历最终参数列表的循环中。只消费 FILE、不执行 Lance 扫描的 BE 也会取得同一张完整表。

同一 BE 后续的 fragment 使用简化公共参数，不重复携带资源表。ID 定义在序列化任何 fragment 之前已经固定，消费端启动不依赖扫描端先运行。

### 4.2 RPC 和 BE 接收

```text
PipelineExecutionTaskBuilder.serializeFragments
  -> TFastSerializer 序列化 TPipelineFragmentParamsList
  -> MultiFragmentsPipelineTask.sendPhaseOneRpc
  -> BackendServiceProxy.execPlanFragmentsAsync
  -> BE PInternalService::_exec_plan_fragment_impl
  -> 按 params_list 顺序调用 FragmentMgr
```

源码入口：

- [PipelineExecutionTaskBuilder.java](../fe/fe-core/src/main/java/org/apache/doris/qe/runtime/PipelineExecutionTaskBuilder.java)：`serializeFragments()`。
- [MultiFragmentsPipelineTask.java](../fe/fe-core/src/main/java/org/apache/doris/qe/runtime/MultiFragmentsPipelineTask.java)：`sendPhaseOneRpc()` 及发送 RPC 的方法。
- [internal_service.cpp](../be/src/service/internal_service.cpp)：`_exec_plan_fragment_impl()`。

### 4.3 Coordinator 路径

[Coordinator.java](../fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java) 在 `processFragmentAssignmentAndParams()` 中调用同一个 Planner。

它保存返回的 lanceBlobResources，在生成执行参数时设置 `params.setLanceBlobResources()`；`PipelineExecContext::unsetFields()` 会在简化后续公共参数时移除这个字段。

两个 FE 路径共享同一个 Resource 生成器，而不是各自维护 ID 分配规则。

## 5 QueryContext 初始化与所有权

### 5.1 按字段存在性初始化

[fragment_mgr.cpp](../be/src/runtime/fragment_mgr.cpp) 创建 QueryContext 时执行：

```cpp
if (params.__isset.lance_blob_resources) {
    RETURN_IF_ERROR(query_ctx->init_lance_blob_resources(
            params.lance_blob_resources));
}
```

没有下发这个字段的查询，不经过该初始化路径，也不创建 LanceBlobReader。

### 5.2 初始化做了什么

[query_context.cpp](../be/src/runtime/query_context.cpp) 的 `init_lance_blob_resources()` 检查每项 Resource：

- source ID 大于 0。
- dataset_uri、version、column_name 字段存在。
- version 大于 0，列名非空。
- URI 和列名没有嵌入 NUL。
- URI 具备远端形式，排除 file 和 memory。
- 存储选项的键和值没有嵌入 NUL。

检查通过后执行：

```cpp
_lance_blob_resources = std::move(resources);
_lance_blob_reader = std::make_unique<LanceBlobReader>();
```

这一步不会打开 dataset，也不会读取 Blob。Reader 的 dataset cache 此时为空。

### 5.3 成员与查询域

[query_context.h](../be/src/runtime/query_context.h) 中，两个成员位于 `_s3_resources` 后面：

```cpp
std::map<int32_t, TLanceBlobResource> _lance_blob_resources;
std::unique_ptr<LanceBlobReader> _lance_blob_reader;
```

同一 BE 上同一查询的执行上下文使用这份 QueryContext。不同查询、不同 BE 各自保存自己的资源表和 Reader。

初始化之后，当前执行调用链只读取资源表。`get_lance_blob_resource()` 返回 const 指针；不存在的 ID 返回 nullptr。

`get_lance_blob_reader()` 返回可空指针。`fl_get_binary()` 明确检查它，而不是依赖 DCHECK 后直接解引用。

### 5.4 生命周期

```text
QueryContext 创建
  -> 安装资源表，创建 Reader
  -> 执行扫描及下游算子
  -> 首次需要内容时，Reader 打开 dataset
  -> Scanner EOF/关闭，查询中的 Reader 继续存活
  -> QueryContext 析构
       先 reset Reader
       再 clear Resource 表
```

dataset cache 由 Reader 持有，其句柄使用 shared_ptr 管理。每次读取的 Blob 句柄由读函数中的 Defer 关闭。

因此，读取能力不会随原 Scanner 结束而消失。释放时机取决于 QueryContext 和相关共享引用的生命周期，不是任意一个扫描 fragment 完成的时刻。

## 6 扫描如何生成 FILE

### 6.1 类型识别与原生 row ID

`LanceTableReader::init()` 初始化 `LanceRecordBatchConverter`。如果有 Blob 列，会检查执行版本及 FE 的 blob_read_source_ids。

配置 SDK scanner 时，若需要全局行号或存在 Blob 列，会调用：

```cpp
lance_scanner_with_row_id(scanner, true)
```

在 `_convert_record_batch()` 中，Reader 获取 Arrow UInt64 类型的 `_rowid` 数组，并要求它与批次行数一致且没有 NULL。

源码入口：[lance_reader.cpp](../be/src/format_v2/table/lance_reader.cpp) 的 `init()`、`_open_scanner()`、`_convert_record_batch()`。

### 6.2 检查 Resource 与扫描一致

处理每个 FILE 投影列时：

1. 根据列名在扫描参数中找到 source ID。
2. 在 QueryContext 中找到 Resource。
3. 比较 Resource 列名、URI、版本和存储选项。
4. 检查 `lance_dataset_version()` 返回的实际版本与 Resource 版本相同。
5. 将 descriptor、原生 row ID、列定义及 source ID 交给构造函数。

固定版本不仅用于回读，也用于检查扫描和引用没有指向不同的快照。

### 6.3 构造公开字段和内部 ID

[lance_blob_column_reader.cpp](../be/src/format_v2/lance/lance_blob_column_reader.cpp) 的 `build_lance_blob_descriptors()` 检查 descriptor 行数，以及 kind、size、blob_id、blob_uri 子数组的类型。

对有效行创建 `File`，赋值如下：

| 内容 | 赋值规则 |
| --- | --- |
| uri | `lance-blob://<原生 row ID>` |
| uri 的可选查询参数 | blob_uri 非空时追加 URL 编码后的 `?blob_uri=...` |
| offset | 保持 NULL |
| size | 非 External 保存 size；External 的 size=0 保持 NULL |
| content_type | 保持 NULL |
| checksum | 保持 NULL |
| inline | 保持 NULL |
| blob_source_id | FE 分配的 source ID |
| blob_row_id | 当前行的原生 row ID |

struct 或 kind 子项为 NULL 时，生成 NULL FILE；如果目标列不允许 NULL，则报错。

kind 大于 3、必需 size 为空、External 的 URI/base ID 结构不完整时会报错。构造结果还会经过 `validate_file()`。

公开 URI 是展示值。消费端不会从它解析 source ID 或存储凭证；两个 dataset 中相同的 row ID 可以展示相同的 URI。

### 6.4 descriptor 中没有完整的文件来源

当前 FILE 中没有逐行保存完整 descriptor。position 没有写入 FILE.offset，kind、blob_id 也不是公开 FILE 字段。

`LanceSplit` 的构造函数将 datasetUri 作为路径，fragment IDs 表达扫描范围；BE 通过 SDK 设置 fragment ID。split URI 不是某个具体数据文件 URI。

因此当前读取端保留 row ID，并把物理位置解析交给 SDK。仅保存 descriptor 的 kind、position、size、blob_id、blob_uri，也不能在所有情况下确定它所属的数据文件。

源码入口：[LanceSplit.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceSplit.java)、[LanceSplitBuilder.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceSplitBuilder.java)、[lance_reader.cpp](../be/src/format_v2/table/lance_reader.cpp)。

## 7 ColumnFile 的内存与列操作

### 7.1 两组 ID 的不变量

`ColumnFile::sanity_check()` 检查：

```text
两组 ID 都不存在
    -> 所有行的 ID 隐式为 0

两组 ID 都存在
    -> source 列为 UInt32
    -> row 列为 UInt64
    -> 两列长度都等于公开字段行数
```

只存在其中一列，或行数不对齐，都会报错。

### 7.2 按需创建

`_append_blob_reference()` 对“尚无 ID 数组且两个 ID 都为零”的追加直接返回。需要保存非零引用时，调用 `_ensure_blob_ids(previous_rows)`：

```text
已有普通 FILE 两行：
  source_ids = nullptr
  row_ids    = nullptr

追加来源 1、原生 row 101：
  source_ids = [0, 0, 1]
  row_ids    = [0, 0, 101]
```

前两行的零值由创建时补齐。后续如果 ID 数组已存在，普通 FILE 追加也需补对应的零值。

### 7.3 对外访问与清理

| 方法 | 当前行为 |
| --- | --- |
| blob_source_id_at(row) | 数组存在时读取该行，否则返回 0 |
| blob_row_id_at(row) | 数组存在时读取该行，否则返回 0 |
| blob_source_ids()/blob_row_ids() | 返回可空 const IColumn 指针 |
| has_blob_references() | 判断 source ID 数组中是否有非零值 |
| fill_missing_blob_ids() | 已有数组时补零对齐；未分配时不创建数组 |
| set_blob_ids() | 成对设置，检查类型和行数 |
| reset_blob_ids() | 两组成员同时赋 nullptr |

两组 ID 占用约 12N 字节数组数据，不包括列对象、容量和公开字段。纯普通 FILE 不需要这部分数组。

### 7.4 复制、过滤和重排

`get()` 把某行的公开字段与 ID 写入单值 File。`insert()`、`insert_from()`、范围插入、选择插入等路径同时维护引用。

clone、filter、permute、replicate 等操作需要让 ID 与公开字段按相同规则移动。COW 修改遵循列对象的可变所有权约束。

source ID 的含义由查询级 Resource 表确定，数值可以直接随行复制，不需要在目标 ColumnFile 中重新登记来源。

源码入口：[column_file.cpp](../be/src/core/column/column_file.cpp) 的 `_ensure_blob_ids()`、`_append_blob_reference()`、`_append_blob_references()` 及各列操作实现。

## 8 fl_get_binary 的执行过程

### 8.1 逐行处理

[function_fl_get_binary.cpp](../be/src/exprs/function/function_fl_get_binary.cpp) 的逻辑是：

```text
输入全为 NULL
    -> 直接生成 NULL 结果

否则先展开常量列，再逐行处理：
    外层 NULL        -> 输出 NULL
    inline 非 NULL   -> 将已有 bytes 复制到输出列
    inline 为 NULL   -> 要求 source ID 非零，构造 BlobRequest
```

inline 非 NULL 且长度为零时，返回非 NULL 的空 VARBINARY，不按 SQL NULL 处理。

没有 inline 内容且 source ID 为零时，函数报错。它不会根据公开 URI 自动选择访问配置，也不会把这个错误变成 NULL。

### 8.2 Reader 的取得

只有存在待读取请求时，才检查 FunctionContext、RuntimeState、QueryContext 和 Reader：

```cpp
auto* reader = context->state()->get_query_ctx()->get_lance_blob_reader();
if (reader == nullptr) {
    return Status::InvalidArgument(
            "External FILE content requires Lance BLOB resources from FE");
}
RETURN_IF_ERROR(reader->read_blob(context->state(), requests, values.get()));
```

因此普通 inline FILE 不要求查询创建 Lance Reader。

结果写入新的 ColumnVarbinary，输入 FILE.inline 不会被改写。

## 9 LanceBlobReader 的分组与读取

### 9.1 请求结构

[lance_blob_reader.h](../be/src/runtime/lance_blob_reader.h) 定义：

```cpp
struct BlobRequest {
    uint32_t blob_source_id;
    uint64_t blob_row_id;
    size_t output_row_index;
};
```

它描述一个 Blob 与一个输出行之间的关系。同一个 Blob 可以被多个请求指向。

### 9.2 groups 的含义

`read_blob()` 构造：

```cpp
std::map<uint32_t, std::map<uint64_t, std::vector<size_t>>> groups;
```

```text
source ID -> 原生 row ID -> 所有需要这份内容的输出行位置
```

例如：

```text
请求                       分组
(1, 101, 0)                1 -> 101 -> [0, 3]
(1, 103, 1)                     103 -> [1]
(3,   9, 2)                3 ->   9 -> [2]
(1, 101, 3)
```

Reader 检查输出位置不能越界，并按 source ID 分组。相同组内的相同 row ID 只获取一个内容结果，再写入所有输出位置。

去重范围是单次 read_blob 调用。同一个引用在另一批次、另一次函数执行、另一个 BE 中出现，仍会进入新的读取过程。

### 9.3 每个来源组的读取步骤

[lance_blob_reader.cpp](../be/src/runtime/lance_blob_reader.cpp) 对每个组执行：

1. 检查查询取消状态。
2. `query_ctx->get_lance_blob_resource(source_id)` 查找完整 Resource。
3. `_get_or_open_dataset()` 获取固定版本的 dataset。
4. 将去重后的 row IDs 整理成数组。
5. 为这些 ID 分配 Blob 句柄输出数组。
6. `lance_dataset_take_blobs()` 按 Resource 中的列名解析句柄。
7. 对每个句柄检查非 NULL，并获取实际长度。
8. 检查长度不超过 UInt32 表示范围。
9. 在输出 ColumnVarbinary 中分配内容空间。
10. 从 Blob 相对偏移 0 调用 `lance_blob_file_read_range()` 读取完整内容。
11. 把同一份结果视图填入对应的所有输出行。

输出列拥有 payload 的内存。一次调用中重复引用可以共享这一份内容空间。

长度使用 `lance_blob_file_size()` 返回的值。读取函数没有 expected_size 参数，不以公开 FILE.size 作为实际读取长度的强校验条件。

### 9.4 句柄清理与取消

每组的句柄数组创建后，立即设置 Defer 清理。正常结束以及后续错误返回都关闭非 NULL 句柄。

取消检查位于组开始和逐个 Blob 读取前。函数中的 SDK 调用是同步调用；不能由这些检查推断 SDK 内部阻塞操作一定能立即中断。

`FunctionFlGetBinary::is_blockable()` 返回 true；当前这条调用链没有新增异步恢复状态机。

## 10 dataset cache

### 10.1 缓存键

`LanceBlobReader::DatasetKey` 的字段为：

```cpp
std::string uri;
int64_t version;
std::map<std::string, std::string> storage_options;
```

使用上述三项组成 map 的 key。column_name 不参与 key。

因此同一个 dataset、版本和选项下的 image、audio 两列，可以共享打开的 dataset；不同版本或不同存储选项不会命中同一 key。

缓存成员为 `_datasets_by_open_spec`，值是使用 `lance_dataset_close` 作为 deleter 的 shared_ptr。

### 10.2 并发打开

`_get_or_open_dataset()` 的顺序为：

```text
加锁查询 cache
    -> 命中则返回 shared_ptr
    -> 未命中则解锁
    -> 转换存储选项，调用 LanceSessionManager.open_dataset
    -> 再加锁，用 try_emplace 安装结果
```

打开动作在锁外，并发首次访问可能打开多个等价 dataset。最终使用缓存中保留的一个结果，其余对象释放。当前不提供同一 key 严格只打开一次的保证。

### 10.3 缓存范围

缓存属于该 BE 上该查询的 Reader，不是 BE 上所有查询共享的 payload cache。

LanceSessionManager 是另外的 SDK session 管理层。缓存 dataset 句柄与 SDK 自身的元数据、对象存储缓存是不同概念，不能据此声称重复 fl_get_binary 一定不会再次访问存储。

源码入口：[lance_blob_reader.cpp](../be/src/runtime/lance_blob_reader.cpp) 的 `_get_or_open_dataset()`，以及 [lance_session_manager.cpp](../be/src/format_v2/lance/lance_session_manager.cpp)。

## 11 跨 BE 读取

### 11.1 配置和数据通过两条路径传输

```text
FE 的执行计划 RPC：
    BE-A <- 完整 Resource 表 -> BE-B

运行中的 Block：
    BE-A 扫描 FILE、source ID、row ID
      -> Exchange
      -> BE-B 收到相同的数值引用
      -> 使用本地 QueryContext 查 Resource
      -> 独立打开远端 dataset、读取 Blob
```

BE-B 不需要向 BE-A 取得 Scanner、dataset 指针或 SDK Blob 句柄。扫描器即使已结束，QueryContext 仍可为消费端提供读取配置和缓存。

### 11.2 为什么类型序列化不需要 RuntimeState

Resource 定义通过执行计划下发。Block 序列化只写公开值及数值 ID，不在过程中查 Resource 或打开 dataset。

`DataTypeFile::deserialize()` 只恢复列数据。随后函数执行时，才通过当前 RuntimeState 取得 QueryContext 并解析 ID。

因此当前代码没有给 DataTypeFile 的 serialize、deserialize 接口增加 RuntimeState 参数。

### 11.3 查询域限制

Resource ID 的定义只存在于本次查询的 QueryContext 中。公开 URI 没有编码完整来源身份，也没有导出私有 ID 的机制。

将公开 FILE JSON 保存后再作为新查询输入，不会恢复原查询的 Resource 关联。相同数字的 source ID 不能跨查询直接复用。

## 12 当前序列化协议

### 12.1 Block/PBlock 路径

[block.cpp](../be/src/core/block/block.cpp) 先调用数据类型的 `get_uncompressed_serialized_bytes()` 计算缓冲区大小，再调用 serialize。PBlock 保存 be_exec_version；反序列化按该版本调用类型的 deserialize。

[data_type_file.cpp](../be/src/core/data_type/data_type_file.cpp) 的三个函数统一使用 `SUPPORT_LANCE_BLOB_RESOURCES_VERSION` 判断。

执行版本小于 20：

```text
列头 + 6 个公开字段
```

执行版本大于或等于 20：

```text
列头 + 6 个公开字段 + has_blob_ids，1 字节
                               |
                       0       |       1
                       |               |
                       结束            source_ids，4R 字节
                                       row_ids，8R 字节
```

R 为实际保存的物理行数。常量列的物理数据行数可以与展开后的逻辑行数不同。

`has_blob_ids` 依据 `ColumnFile::has_blob_references()`，不依据整个查询是否有 Resource。当前无引用列的新格式只增加一个标志字节，不写 12R 字节的零数组。

### 12.2 三个函数的行为

| 函数 | 旧执行版本 | 新执行版本 |
| --- | --- | --- |
| get_uncompressed_serialized_bytes | 计算公开字段；存在引用时报错 | 增加标志；有引用时计入两组 ID |
| serialize | 只写公开字段；拒绝丢弃真实引用 | 写标志，按需写两组 ID |
| deserialize | 读公开字段后结束，清空内部 ID | 读标志；为 1 时创建 ID 列，为 0 时保持 nullptr |

新格式标志只能为 0 或 1。其他值导致反序列化错误。

计算序列化大小只做大小计算，不创建两组 ID 列。真正需要输出缓冲区的是 Block 调用方。

### 12.3 PValues 路径

[data_type_file_serde.cpp](../be/src/core/data_type_serde/data_type_file_serde.cpp) 的 `write_column_to_pb()`、`read_column_from_pb()` 使用另一种自描述结构：

```text
6 个 children：公开字段
8 个 children：公开字段 + source IDs + row IDs
```

写入端在 `has_blob_references()` 为真时追加 UInt32、UInt64 两个子元素。这里按整列判断，因此所写切片本身全零时也可能有八个 children。

读取端接受六个或八个 children，检查类型、行数和 NULL 信息。若 ID 全零，仍可恢复为未分配 ID 数组的 ColumnFile。

该接口没有执行版本参数，不能把 Block 中的版本分支套用到这里。升级验收需要分别检查它的实际调用方。

### 12.4 arena 和行级序列化

[column_file.cpp](../be/src/core/column/column_file.cpp) 的 `serialize_size_at()`、`serialize_impl()`、`deserialize_impl()` 使用：

```text
6 个公开字段的行编码 + UInt32 source ID + UInt64 row ID
```

这些接口没有执行版本参数，当前即使 ID 为零也保存 12 字节。serialize_vec 和 arena 接口调用这一行编码。

同文件中的 `hash_file_rows()` 使用行编码计算哈希。由此可见，Block 按版本读写只是一个协议入口；它不能自动覆盖 arena、哈希和其他中间状态路径的兼容性。

### 12.5 JSON 路径

公开 JSON 只序列化六个字段，不携带 Resource 和私有 ID。JSON 恢复的 File 默认 ID 为零。

```text
FILE -> JSON -> FILE
```

该往返保留公开值，不保留 Lance 内容读取关联。若没有 inline 内容，之后执行 fl_get_binary 会进入缺少来源的错误路径。

## 13 执行版本和升级顺序

### 13.1 当前版本常量

```text
BE：SUPPORT_LANCE_BLOB_RESOURCES_VERSION = 20
FE：LANCE_BLOB_MIN_BE_EXEC_VERSION = 20
```

BE 支持的最高执行版本使用该常量；FE 的 max_be_exec_version 也使用对应常量，默认 be_exec_version 跟随它。

源码入口：[be_exec_version_manager.h](../be/src/agent/be_exec_version_manager.h)、[be_exec_version_manager.cpp](../be/src/agent/be_exec_version_manager.cpp)、[Config.java](../fe/fe-common/src/main/java/org/apache/doris/common/Config.java)。

本次查询的执行版本通过查询选项传入 Resource Planner。它不是 Reader 自行选择，也不是按每个 BE 自动协商。

### 13.2 先 BE 后 FE

| 阶段 | 执行版本 | 当前代码预期的 FILE 行为 |
| --- | --- | --- |
| 旧 FE，新旧 BE 混部 | 由旧 FE 下发，例如 13 | 新 BE 继续以六字段格式收发普通 FILE |
| BE 全部升级，FE 未升级 | 仍为旧值 | 保持同一格式 |
| BE 全部升级，再升级 FE | 默认 20 | 使用标志和可选 ID，允许 Resource Blob 读取 |
| 新 FE 显式配置旧执行版本 | 小于 20 | 普通 FILE 使用旧格式，Blob 规划被拒绝 |

新 BE 在旧版本查询中既要能读取旧格式，也要能写出旧格式。Blob 引用遇到旧执行版本时不允许静默丢弃 ID。

这些是源码中的分支行为，尚未等价于已通过新旧节点混部测试。

### 13.3 Blob 的版本检查位置

- FE Planner 检查非空 Blob 来源的查询执行版本。
- BE LanceTableReader 在 Blob 投影初始化时检查版本。
- DataTypeFile 写入前的 `has_serializable_blob_ids()` 检查真实引用与执行版本是否匹配。

### 13.4 旧 FE 的 Lance 参数接收

当前源码同时保留 `TLanceFileDesc.dataset_uri`、`version` 两个废弃字段，供接收旧 FE 的执行参数。

`LanceTableReader::fetch_schema()` 在公共 dataset 身份缺失时，从 range 的 TLanceFileDesc 取得配置。

扫描初始化时，如果执行版本小于 20 且公共参数没有 dataset 身份，Reader 创建 `_legacy_scan_params` 副本。prepare_split 把旧 split 中的 URI、版本填入该副本，再进行参数校验。这样不会修改其他 Scanner 共享的公共扫描参数。

这条兼容路径保留在当前源码中，因此即使新 FE 不再设置 split 的这两个字段，文档仍需说明新 BE 如何接收旧 FE。

源码入口：[lance_reader.h](../be/src/format_v2/table/lance_reader.h)、[lance_reader.cpp](../be/src/format_v2/table/lance_reader.cpp) 的 `_legacy_scan_params`、`set_legacy_dataset_identity()`、`fetch_schema()`、`init()`、`prepare_split()`。

## 14 split 和扫描参数各自保存什么

[PlanNodes.thrift](../gensrc/thrift/PlanNodes.thrift) 当前定义如下职责：

| 结构 | 当前用途 | 关键字段 |
| --- | --- | --- |
| TLanceScanParams | 扫描节点公共配置 | URI、版本、存储选项、过滤/搜索请求、blob_read_source_ids |
| TLanceFileDesc | 单个 split 的工作 | fragment_ids、limit、index_segment_uuids、use_scalar_index |
| TLanceBlobResource | 下游 FILE 消费端配置 | URI、固定版本、列名、存储选项 |

TLanceScanParams 中 URI、版本和列映射分别是字段 4、5、6。TLanceFileDesc 的 URI、版本字段保留原编号 1、3，并标记为 Deprecated。

正常扫描通过 `lance_scanner_set_fragment_ids()` 设置范围。一个 split 可以包含多个 fragment；索引 split 可以包含一个物理 segment 及其覆盖范围。URI 从 LanceSplit 构造开始就是 dataset URI。

读取 Blob 时不使用 TLanceFileDesc.limit 或 fragment 列表重新定义来源。来源由 Resource 的固定快照、列和 row ID 决定。

## 15 本地来源、长度和错误处理

### 15.1 本地来源检查

| 入口 | 代码检查 |
| --- | --- |
| FE Blob TVF | FILE_LOCAL、非远端形式、非正数版本时拒绝 |
| FE Resource 分配 | URI 形式与固定版本检查 |
| QueryContext | 拒绝本地 Resource URI及无效字段 |
| TableReader 打开 dataset | 读取 base-path 元数据，任意注册本地 base 时拒绝 |
| descriptor 构造 | External 且 blob_id=0 时，检查 blob_uri 是否为本地 |

`lance_blob_storage_is_local()` 把 file、memory，以及不具备有效 scheme 形式的字符串视为本地。

完整 Resource 广播不能让 BE-A 的磁盘文件被 BE-B 直接访问。当前代码没有按来源 BE 转发读取，也没有把消费算子固定在本地文件所在 BE。

注册 base 的检查遍历整个注册表，不只检查本次实际命中的 base，这是当前实现的实际限制。

源码入口：[lance_blob_column_reader.cpp](../be/src/format_v2/lance/lance_blob_column_reader.cpp)、[lance_reader.cpp](../be/src/format_v2/table/lance_reader.cpp)、[query_context.cpp](../be/src/runtime/query_context.cpp)。

### 15.2 长度和公开元数据

descriptor 的已知 size 若超过 BIGINT 表示范围，在构造 FILE 时拒绝。External size=0 保持未知长度。

读取内容时，SDK 返回的长度若超过 UInt32 范围，在分配输出 payload 前拒绝；长度为零则不申请 payload 空间，也不执行非零范围读取。

当前没有 Lance 专用可配置最大 Blob 大小，没有 expected_size 参数。固定 dataset 版本也不会自动锁定任意 External 对象的内容，外部对象覆盖或删除仍可能影响稍后的读取。

### 15.3 主要错误路径

| 条件 | 报错位置 |
| --- | --- |
| Resource 字段无效、NUL 或本地 URI | QueryContext 初始化 |
| Resource 与扫描快照、选项、列不一致 | TableReader 转换批次 |
| descriptor 类型、长度、kind 无效 | build_lance_blob_descriptors |
| 原生 row ID 缺失、含 NULL 或长度不一致 | TableReader 转换批次 |
| FILE 没有 inline，也没有有效 source ID | fl_get_binary |
| QueryContext 或 Reader 缺失 | fl_get_binary/read_blob |
| source ID 没有对应 Resource | read_blob |
| 句柄为 NULL、SDK 出错、内容超长 | read_blob |
| 新格式 ID 标志非法 | DataTypeFile 反序列化 |
| 旧版本写出 Blob 引用 | DataTypeFile 序列化前检查 |

有效空内容、SQL NULL 和读取失败是三种不同结果。代码不会把无效来源或对象读取错误统一转成 NULL。

## 16 用一个批次串起完整实现

### 16.1 FE 下发配置

假设查询投影两个来源列，FE 产生：

```text
Resource 1：s3://bucket/a.lance，version 42，image，options A
Resource 2：s3://bucket/b.lance，version 18，image，options B
```

所有参与 BE 都有这两项。扫描参数分别把各自的 image 映射为 1、2。

### 16.2 扫描和列流转

扫描产生的四行引用经合并或重排后可以是：

```text
输出位置       0       1       2       3
source ID      1       2       1       1
Lance row    101       9     103     101
```

公开 FILE.uri 分别包含自己的原生 row ID；读取过程不依赖它们在当前 Block 中的位置。

### 16.3 读取和回填

```text
组 1：Resource 1，row IDs [101, 103]
      101 的内容 -> 输出 0、3
      103 的内容 -> 输出 2

组 2：Resource 2，row IDs [9]
        9 的内容 -> 输出 1
```

同一个数字的 row ID 若属于不同 source，也在不同分组中读取，不会混淆两个 dataset。

### 16.4 SQL 观察入口

```sql
-- 观察 FILE 和公开元数据。
SELECT id, image,
       element_at(image, 'uri') AS blob_uri,
       element_at(image, 'size') AS blob_size
FROM lance_catalog.demo.assets
LIMIT 10;

-- 显式读取内容。
SELECT id, fl_get_binary(image) AS image_bytes
FROM lance_catalog.demo.assets
WHERE id IN (1, 2);
```

SQL 例子用于说明入口。读取行数应结合实际物理计划验证，不以 SQL 的书写顺序替代执行顺序。

## 17 资源成本与当前实现边界

| 内容 | 从代码可确定的成本或边界 |
| --- | --- |
| Resource 配置 | 每个来源列复制 URI、版本和选项，并发送给所有参与 BE |
| ColumnFile ID 数组 | 分配后约 12N 字节数据；不包括容量和列对象 |
| 新格式无引用列 | 引用部分只增加一个标志字节 |
| groups | 每次调用创建分组和输出位置容器 |
| dataset cache | 每个 QueryContext、每个不同 DatasetKey 一份结果 |
| payload | 单次调用内相同 source、row 只读取一次；不提供跨调用内容缓存 |
| 同步调用 | SDK 读取期间当前调用等待完成 |
| 存储选项 | 本次查询使用固定配置；当前没有凭证更新通道 |

同 dataset 的多个来源列可以共享 dataset 句柄，但 Resource 定义本身没有按 dataset 抽出一层共享配置。

当前 `LanceBlobReader` 没有专用的跨批次 payload cache、异步读取状态机、预计长度匹配或独立的读取并发控制。描述实现时不应把这些能力算作已具备。

## 18 spill 和其他序列化调用方

[spill_file_writer.cpp](../be/src/exec/spill/spill_file_writer.cpp) 使用 Block::serialize，版本取 `BeExecVersionManager::get_newest_version()`；[spill_file_reader.cpp](../be/src/exec/spill/spill_file_reader.cpp) 从 PBlock 恢复 Block。

因此 spill 保存的是公开列值和引用 ID，没有在文件内复制整个 Resource 表。恢复后的内容读取仍依赖当前查询的 QueryContext。

Exchange 使用查询 RuntimeState 中的执行版本，spill writer 使用本 BE 最新版本。这两个选版入口不能合并理解成所有写出都取 FE 当前配置。

arena、PValues、哈希及聚合中间状态需要按各自实际调用链检查。当前数据类型中存在版本分支，不代表整个系统每个 FILE 使用路径都已经通过混部验证。

## 19 评审与验收

### 19.1 需要保持的代码不变量

1. 所有参与 BE 使用同一份 source ID 定义，且在 fragment 执行前安装。
2. Resource 和扫描使用相同 URI、版本、列及存储选项。
3. ColumnFile 公开值与两组 ID 始终逐行对齐。
4. 复制 source ID 不改变其在本次查询中的含义。
5. Block 的大小计算、写入和读取使用相同版本条件及标志解释。
6. 错误和取消路径释放已经取得的 Blob 句柄。
7. 消费端不依赖扫描器仍存活，也不解析展示 URI 恢复访问配置。

### 19.2 当前实现需要的运行验收

| 类别 | 应覆盖的场景 |
| --- | --- |
| 正确性 | 四种支持的 kind、SQL NULL、空 Blob、原生 row ID 0、乱序和重复 ID |
| 多来源 | 多 dataset、不同版本、同 dataset 不同列、相同 row ID 的不同来源 |
| 列操作 | Field 往返、复制、过滤、重排、切片、COW、普通 FILE 与 Blob 混合 |
| 跨 BE | 接收 BE 没有 Lance Scanner、扫描先结束、结果仍可读取 |
| 传输 | 常量、Nullable、FILE 后面紧跟其他列、压缩、PValues 六/八个 children |
| 版本 | 旧查询中新 BE 收发旧格式、新格式标志、低版本拒绝引用、arena 和 hash 边界 |
| Lance 参数 | 旧 FE 的 schema 获取、普通扫描、搜索、COUNT 和非 Blob TVF |
| spill | 同一查询内恢复 ID 并继续读取、选版及资源释放正确 |
| 错误 | 来源缺失、Reader 缺失、对象删除、权限、超长、非法 descriptor |
| 性能 | dataset cache 命中、并发首次打开、批次内去重、真实存储请求和内存 |

这些项目是验收清单，不是已经通过的测试结果。本文按工作区源码描述机制，不宣称已经完成编译、跨 BE 或升级联调。

### 19.3 讨论项

- 是否需要按 dataset 共享 Resource 中的重复打开配置。
- 本地 base 检查是否需要从整个注册表缩小到实际引用来源。
- 是否值得在 SDK 增加 descriptor 与文件来源的直接读取接口，以替换 row-ID 定位。
- 是否需要对函数的计划位置、CASE/IF 选择行提供更明确的优化和执行机制。
- 是否需要跨批次内容缓存、分块读取、并发控制和异步调度。
- External 对象可变性、凭证有效期和公开 URI 展示需要怎样的业务约束。
- 当前 Block 以外的序列化和哈希使用面，应如何形成完整升级验收。

## 20 关键源码索引

| 模块 | 主要文件与函数 |
| --- | --- |
| 类型映射 | [LanceTypeConverter.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/metadata/LanceTypeConverter.java)：toDorisType、extensionType |
| FE 来源规划 | [LanceBlobResourcePlanner.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceBlobResourcePlanner.java)：plan、assign |
| Catalog 扫描 | [LanceScanNode.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceScanNode.java)：blobReadColumns、assignBlobSourceIds、setScanParams |
| TVF 扫描 | [TVFScanNode.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/tvf/source/TVFScanNode.java)：createScanRangeLocations、assignBlobSourceIds |
| split | [LanceSplit.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceSplit.java)、[LanceSplitBuilder.java](../fe/fe-core/src/main/java/org/apache/doris/datasource/lance/source/LanceSplitBuilder.java) |
| 执行参数 | [ThriftPlansBuilder.java](../fe/fe-core/src/main/java/org/apache/doris/qe/runtime/ThriftPlansBuilder.java)：plansToThrift |
| Coordinator | [Coordinator.java](../fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java)：processFragmentAssignmentAndParams、unsetFields |
| Thrift 类型 | [PlanNodes.thrift](../gensrc/thrift/PlanNodes.thrift)：TLanceBlobResource、TLanceScanParams、TLanceFileDesc |
| Thrift 执行字段 | [PaloInternalService.thrift](../gensrc/thrift/PaloInternalService.thrift)：TPipelineFragmentParams.lance_blob_resources |
| BE 查询初始化 | [fragment_mgr.cpp](../be/src/runtime/fragment_mgr.cpp)、[query_context.cpp](../be/src/runtime/query_context.cpp) |
| 扫描到引用 | [lance_reader.cpp](../be/src/format_v2/table/lance_reader.cpp)：_open_scanner、_convert_record_batch |
| descriptor 构造 | [lance_blob_column_reader.cpp](../be/src/format_v2/lance/lance_blob_column_reader.cpp)：build_lance_blob_descriptors |
| 单值 File 定义 | [field.h](../be/src/core/field.h)：doris::File |
| 列表示 | [column_file.h](../be/src/core/column/column_file.h)、[column_file.cpp](../be/src/core/column/column_file.cpp) |
| 内容入口 | [function_fl_get_binary.cpp](../be/src/exprs/function/function_fl_get_binary.cpp)：execute_impl |
| 内容读取与缓存 | [lance_blob_reader.h](../be/src/runtime/lance_blob_reader.h)、[lance_blob_reader.cpp](../be/src/runtime/lance_blob_reader.cpp)：read_blob、_get_or_open_dataset |
| Block FILE 编码 | [data_type_file.cpp](../be/src/core/data_type/data_type_file.cpp)：get_uncompressed_serialized_bytes、serialize、deserialize |
| PValues 和 JSON | [data_type_file_serde.cpp](../be/src/core/data_type_serde/data_type_file_serde.cpp)：write_column_to_pb、read_column_from_pb 及 JSON 方法 |
| Block 框架 | [block.cpp](../be/src/core/block/block.cpp)：serialize、deserialize |
| BE 版本 | [be_exec_version_manager.h](../be/src/agent/be_exec_version_manager.h)、[be_exec_version_manager.cpp](../be/src/agent/be_exec_version_manager.cpp) |
| FE 版本 | [Config.java](../fe/fe-common/src/main/java/org/apache/doris/common/Config.java) |
| SDK session | [lance_session_manager.cpp](../be/src/format_v2/lance/lance_session_manager.cpp) |
