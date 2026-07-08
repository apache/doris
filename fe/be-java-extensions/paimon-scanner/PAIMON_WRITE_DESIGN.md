# Doris Paimon 写入功能架构设计

> **设计原则**：从第一性原理出发，基于 Paimon Java SDK（功能完备）和 Paimon Rust（性能优先）的差异，设计一个统一的写入框架。
> 
> **核心策略**：Rust writer 和 Java writer 是**互相替代**的关系，Rust 是 Java 的功能子集。优先使用 Rust（性能），功能缺失时回退 Java。首个版本仅引入 Java JNI writer，但在架构上必须提前为 Rust FFI writer 预留接口。

## 目录

1. [背景与目标](#1-背景与目标)
2. [Paimon 写入模型分析](#2-paimon-写入模型分析)
3. [Java SDK vs Rust 差异矩阵](#3-java-sdk-vs-rust-差异矩阵)
4. [Doris 现有写入框架分析](#4-doris-现有写入框架分析)
5. [架构设计](#5-架构设计)
6. [接口定义](#6-接口定义)
7. [数据流与交互时序](#7-数据流与交互时序)
8. [FE 端设计](#8-fe-端设计)
9. [BE 端设计](#9-be-端设计)
10. [功能路由矩阵](#10-功能路由矩阵)
11. [实施路线图](#11-实施路线图)

---

## 1. 背景与目标

### 1.1 背景

Doris 需要支持向 Paimon 表写入数据。目前有两个可用的 Paimon 客户端库：

- **Paimon Java SDK** (`paimon-core`)：功能最完整的参考实现，支持全量的写入、提交、Compaction、Schema Evolution、Lookup Changelog Producer、Deletion Vector 等功能。
- **Paimon Rust** (`paimon-rust`)：高性能实现，使用 Arrow 原生格式，零拷贝，异步 I/O。但目前功能较 Java 版本有差距（缺少 Compaction、Lookup/Full-compaction Changelog Producer、部分 MergeEngine 与 Deletion Vector 的组合等）。

### 1.2 目标

1. **v1（当前）**：基于 Java SDK 通过 JNI 实现功能完备的 Paimon 写入。同时建立 `IPaimonWriteBackend` 抽象接口，为 Rust 后端预留设计空间。
2. **v2（中期）**：引入 Paimon Rust 通过 FFI 加速写入。Rust 覆盖 Append-only + 固定桶等高吞吐场景，功能缺失时自动回退 Java。
3. **长期**：随着 Paimon Rust 功能完善，逐步扩大 Rust 后端的覆盖范围，最终在大多数场景下优先使用 Rust。
4. **架构目标**：`IPaimonWriteBackend` 是核心抽象——Java 和 Rust 是平等的后端实现，可以互相替代，上层（Doris Sink）不感知差异。

### 1.3 关键约束

- Paimon C++ native 版本（paimon-cpp）未来会被删除，不考虑
- Java SDK 通过 JNI 调用，Rust 通过 C FFI 调用
- Doris BE 是 C++ 代码，需要通过 JNI（调用 Java）或 FFI（调用 Rust）来桥接
- 写入流程包括：数据写入 → CommitMessage 生成 → Snapshot 提交
- FE 端需要感知表类型（Paimon）来做查询规划和写入协调

---

## 2. Paimon 写入模型分析

### 2.1 核心写入模型（两阶段提交）

Paimon 的写入模型是一个**两阶段提交**过程，无论 Java 还是 Rust 都遵循相同的抽象：

```
阶段1：Write（分布式）
  WriteBuilder → newWrite() → TableWrite
    → write(row/batch) × N       // 每个 worker 写自己的数据
    → prepareCommit()            // 关闭文件，生成 CommitMessage[]
    → CommitMessage[]             // 序列化传递给 Coordinator

阶段2：Commit（集中式）
  WriteBuilder → newCommit() → TableCommit
    → commit(CommitMessage[])    // 原子提交：写 Manifest → 创建 Snapshot
    → abort(CommitMessage[])     // 失败时清理数据文件
```

### 2.2 Java SDK 核心接口

#### WriteBuilder（工厂）

```java
// 批写入构建器
public interface BatchWriteBuilder extends WriteBuilder {
    BatchWriteBuilder withOverwrite();
    BatchWriteBuilder withOverwrite(Map<String, String> staticPartition);
    BatchTableWrite newWrite();
    BatchTableCommit newCommit();
}

// 流写入构建器
public interface StreamWriteBuilder extends WriteBuilder {
    StreamWriteBuilder withCommitUser(String commitUser);
    StreamTableWrite newWrite();
    StreamTableCommit newCommit();
}
```

**关键点**：`WriteBuilder` 同时创建 `TableWrite` 和 `TableCommit`。同一个 `WriteBuilder` 实例产出的 write 和 commit 共享 `commitUser`（用于幂等提交）。

#### TableWrite（数据写入）

```java
public interface TableWrite extends AutoCloseable {
    TableWrite withIOManager(IOManager ioManager);
    TableWrite withWriteType(RowType writeType);
    TableWrite withMemoryPoolFactory(MemoryPoolFactory factory);

    // 分区/桶计算
    BinaryRow getPartition(InternalRow row);
    int getBucket(InternalRow row);

    // 逐行写入
    void write(InternalRow row) throws Exception;
    void write(InternalRow row, int bucket) throws Exception;

    // 批量写入（高效路径）
    void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception;

    // 压缩
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    // 生成提交消息（在 TableWriteImpl 上，非接口方法）
    // List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
}
```

**关键设计点**：
- `writeBundle()` 是高性能批量写入路径：调用者预先计算好 partition 和 bucket，将一批行打包为 `BundleRecords` 直接写入
- `compact()` 是显式的 bucket 级压缩触发
- `prepareCommit()` 返回 `List<CommitMessage>`，每个 `CommitMessage` 对应一个 (partition, bucket) 的新文件列表
- `CommitMessage` 是 `Serializable` 的，可以跨网络传输

#### CommitMessage

```java
public interface CommitMessage extends Serializable {
    BinaryRow partition();     // 分区键（二进制编码）
    int bucket();              // 桶编号
    Integer totalBuckets();    // 该分区的总桶数
}
```

> **注意**：Java 的 `CommitMessage` 接口只暴露了 partition 和 bucket。实际的文件列表（newFiles, changelogFiles, indexFiles 等）在实现类 `CommitMessageImpl` 中，通过 `CommitMessageSerializer` 序列化。这是一种封装设计——调用者不需要知道内部文件细节。

#### BatchTableCommit

```java
public interface BatchTableCommit extends TableCommit {
    void commit(List<CommitMessage> commitMessages);
    void truncateTable();
    void truncatePartitions(List<Map<String, String>> partitionSpecs);
    void updateStatistics(Statistics statistics);
    void compactManifests();
}
```

### 2.3 Rust 核心接口

Rust 采用更直接的 API 设计：

```rust
// 工厂
pub struct WriteBuilder<'a> { table, commit_user, overwrite }
impl WriteBuilder {
    pub fn new_write(&self) -> Result<TableWrite>;
    pub fn new_commit(&self) -> TableCommit;
    pub fn with_overwrite(self) -> Self;
    pub fn with_commit_user(self, user) -> Result<Self>;
}

// 写入
pub struct TableWrite { ... }
impl TableWrite {
    pub fn with_overwrite(self) -> Self;
    pub async fn write_arrow_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    pub async fn write_arrow(&mut self, batches: &[RecordBatch]) -> Result<()>;
    pub async fn prepare_commit(&mut self) -> Result<Vec<CommitMessage>>;
}

// 提交
pub struct TableCommit { ... }
impl TableCommit {
    pub async fn commit(&self, messages: Vec<CommitMessage>) -> Result<()>;
    pub async fn overwrite(&self, messages: Vec<CommitMessage>,
                           static_partitions: Option<HashMap<String, Option<Datum>>>) -> Result<()>;
    pub async fn truncate_table(&self) -> Result<()>;
    pub async fn truncate_partitions(&self, partitions: Vec<HashMap<String, Option<Datum>>>) -> Result<()>;
    pub async fn abort(&self, messages: &[CommitMessage]) -> Result<()>;
}

// 提交消息（具体结构体，所有字段公开）
pub struct CommitMessage {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub new_files: Vec<DataFileMeta>,
    pub new_changelog_files: Vec<DataFileMeta>,
    pub new_index_files: Vec<IndexFileMeta>,
    pub deleted_index_files: Vec<IndexFileMeta>,
    pub deleted_files: Vec<DataFileMeta>,
    pub check_from_snapshot: Option<i64>,
}
```

**关键差异**：
- Rust 的 `CommitMessage` 是**具体结构体**，所有字段直接暴露，而 Java 通过序列化器隐藏内部细节
- Rust 使用 Arrow `RecordBatch` 作为数据载体，Java 使用 `InternalRow`
- Rust 的写入是 async 的，Java 是同步的（但在 Flink 等框架中被异步调用）

### 2.4 写入功能全景

| 功能 | Java SDK | Rust | 说明 |
|------|----------|------|------|
| **Append-only 写入** | ✅ | ✅ | 无主键表的顺序写入 |
| **主键表写入** | ✅ | ✅ | Sorted Merge Tree，按主键排序去重 |
| **固定桶 (Fixed Bucket)** | ✅ | ✅ | hash(bucket_key) % N |
| **动态桶 (Dynamic Bucket)** | ✅ | ✅ | bucket=-1，自动分配桶 |
| **跨分区桶 (Cross Partition)** | ✅ | ✅ | 动态桶 + 跨分区主键 |
| **Postpone 桶 (bucket=-2)** | ✅ | ✅ | 延迟桶分配，用于高吞吐写入 |
| **MergeEngine: Normal** | ✅ | ✅ | 最后写入胜出 |
| **MergeEngine: PartialUpdate** | ✅ | ✅ | 部分列更新（Rust 不支持 +DV） |
| **MergeEngine: Aggregation** | ✅ | ✅ | 聚合函数（Rust 不支持 +DV） |
| **MergeEngine: FirstRow** | ✅ | ✅ | 保留第一行 |
| **MergeEngine: Deduplicate** | ✅ | ✅ | 按主键去重 |
| **ChangelogProducer: None** | ✅ | ✅ | 不产生 changelog |
| **ChangelogProducer: Input** | ✅ | ✅ | 透传 changelog |
| **ChangelogProducer: Lookup** | ✅ | ❌ | 提交时查找旧值生成 changelog |
| **ChangelogProducer: FullCompaction** | ✅ | ❌ | 全量压缩时生成 changelog |
| **Compaction** | ✅ | ❌ | 自动/手动合并，层级压缩 |
| **Deletion Vector** | ✅ | 部分 | Rust: 不支持 PartialUpdate+DV、Aggregation+DV |
| **Schema Evolution** | ✅ | ✅ | 列添加/删除/重命名/类型变更 |
| **Row Tracking** | ✅ | ✅ | DataEvolution 的行 ID 追踪 |
| **DataEvolution (MERGE INTO)** | ✅ | ✅ | 基于行 ID 的部分列更新 |
| **CoW MERGE INTO** | ✅ | ✅ | 无 PK 表的 Copy-on-Write 合并 |
| **分区覆盖 (Overwrite)** | ✅ | ✅ | 静态/动态分区覆盖 |
| **Truncate** | ✅ | ✅ | 清空表/分区 |
| **Blob 类型** | ✅ | ✅ | 大对象列分离存储 |
| **全局索引 (Global Index)** | ✅ | 部分 | Rust 仅支持 DropPartitionIndex |
| **二次索引 (BTree Index)** | ✅ | ✅ | |
| **文件格式: Parquet** | ✅ | ✅ | 默认 |
| **文件格式: ORC** | ✅ | ✅ | |
| **文件格式: Avro** | ✅ | ✅ | |
| **文件格式: Row** | ✅ | ✅ | Arrow IPC 格式 |
| **文件格式: Vortex** | ❌ | ✅ (feature) | Rust 特有 |
| **Stream Writer (持续写入)** | ✅ | ❌ | Flink 流式写入模式 |
| **Tag/Branch** | ✅ | 部分 | Rust 有 Tag 读取器但无创建 API |
| **提交重试与幂等** | ✅ | ✅ | commit_user + commitIdentifier |

---

## 3. Java SDK vs Rust 差异矩阵

### 3.1 架构差异

| 维度 | Java SDK | Rust |
|------|----------|------|
| **数据载体** | `InternalRow` (自定义行格式) | Arrow `RecordBatch` (列式) |
| **API 粒度** | 逐行 `write(InternalRow)` + 批量 `writeBundle()` | 批量 `write_arrow_batch(RecordBatch)` |
| **CommitMessage** | 接口 + 序列化器（封装实现） | 具体结构体（所有字段公开） |
| **I/O 模型** | 同步（阻塞 I/O） | 异步（tokio + opendal） |
| **内存管理** | Java GC + 直接内存（spillable） | Rust 所有权系统 |
| **压缩** | 内置 `CompactManager` | 未实现 |
| **可扩展性** | 通过 SPI 插件 | 通过 feature flags 和 traits |

### 3.2 性能差异

| 维度 | Java SDK | Rust |
|------|----------|------|
| **写入吞吐** | 良好 | 优秀（零拷贝 Arrow，异步 I/O） |
| **内存效率** | 依赖 GC，可能 FGC | 确定性的所有权管理 |
| **启动时间** | JVM 预热开销 | 原生性能，无预热 |
| **CPU 效率** | JIT 编译优化 | 编译期优化 |
| **跨语言调用** | JNI 开销 | FFI 开销（更低） |

### 3.3 功能完备度

**Java SDK 功能完备度**：⭐⭐⭐⭐⭐（参考实现，100% 功能）
**Rust 功能完备度**：⭐⭐⭐⭐（约 75%，核心缺失：Compaction, Lookup/FullCompaction ChangelogProducer）

---

## 4. Doris 现有写入框架分析

### 4.1 BE 端写入框架

Doris BE 端的 Table Writer 继承体系：

```
ResultWriter (result_writer.h)
  └── AsyncResultWriter (async_result_writer.h)
        ├── VTabletWriter (vtablet_writer.h)        // 内部表写入
        ├── VHiveTableWriter (vhive_table_writer.h)  // Hive 外部表
        ├── VJdbcTableWriter (vjdbc_table_writer.h)  // JDBC 外部表
        ├── VTVFTableWriter (vtvf_table_writer.h)    // TVF 表函数
        ├── VIcebergTableWriter                       // Iceberg 外部表
        └── VMaxComputeTableWriter                    // MaxCompute 外部表
```

**AsyncResultWriter** 的核心接口：
```cpp
class AsyncResultWriter : public ResultWriter {
public:
    virtual Status open(RuntimeState* state, RuntimeProfile* profile) = 0;
    virtual Status write(RuntimeState* state, Block& input_block) = 0;  // 实际 I/O
    Status sink(Block* block, bool eos);  // 异步入队
    Status start_writer(RuntimeState* state, RuntimeProfile* profile);  // 启动 I/O 线程
};
```

**关键设计**：
- `sink()` → 异步队列 → `write()`：解耦计算和 I/O
- `Block` 是 Doris 的列式数据批次
- 每个 writer 实例对应一个 BE worker 线程

### 4.2 Iceberg Table Writer 参考

`VIcebergTableWriter` 是最接近 Paimon 写入需求的外部表写入器：

```
写入流程：
  write(RuntimeState, Block)
    → 分区计算（PartitionTransform）
    → 确定目标 partition path
    → 获取/创建 VIcebergPartitionWriter（每个 partition 一个）
    → 写入 Parquet/ORC 文件
    → 文件滚动（按 target_file_size）
  close()
    → 关闭所有 partition writers
    → 收集产生的 DataFile 列表
    → 通过 Iceberg Catalog API 提交（AddFiles）
```

**关键差异（vs Paimon）**：

Iceberg 和 Paimon 的写入模型存在根本性差异，这直接影响 Doris 的写入架构设计。

**Iceberg：Commit-on-Close 模型**

```
BE Worker 1                    BE Worker 2                    FE Coordinator
  │                               │                              │
  │ write(block) × N              │ write(block) × N             │
  │  → 写入 Parquet 文件           │  → 写入 Parquet 文件          │
  │  → 文件滚动                    │  → 文件滚动                   │
  │                               │                              │
  │ close()                       │ close()                      │
  │  → 收集 DataFile 列表          │  → 收集 DataFile 列表         │
  │  → 生成 TIcebergCommitData    │  → 生成 TIcebergCommitData   │
  │     {file_path, row_count,    │     {file_path, row_count,    │
  │      file_size, partition}    │      file_size, partition}    │
  │                               │                              │
  │──── TIcebergCommitData ──────│──── TIcebergCommitData ──────→│
  │                               │                              │
  │                               │    聚合所有 DataFile 列表      │
  │                               │    table.newAppend()          │
  │                               │      .appendFile(files...)    │
  │                               │      .commit()   ← 原子提交   │
```

Iceberg writer 的输出就是**最终产物**：一堆已经写好的数据文件（Parquet/ORC）+ 文件的元信息（路径、行数、大小、分区值）。FE Coordinator 收集这些文件列表后，直接调用 Iceberg 的 `appendFile()` + `commit()` 即可完成提交。文件列表是**透明的、可直接使用的**。

**Paimon：Prepare-then-Commit 模型**

```
BE Worker 1                    BE Worker 2                    FE Coordinator
  │                               │                              │
  │ write(row) × N                │ write(row) × N               │
  │  → Paimon 内部：               │  → Paimon 内部：              │
  │    分区/桶路由                  │    分区/桶路由                 │
  │    KeyValueFileWriter         │    AppendOnlyWriter          │
  │    排序、去重、序列号分配        │    文件滚动                   │
  │    写入数据文件 + changelog     │                              │
  │                               │                              │
  │ prepareCommit()               │ prepareCommit()              │
  │  → CommitMessage[]            │  → CommitMessage[]           │
  │     {                         │     {                        │
  │       partition: bytes,       │       partition: bytes,      │
  │       bucket: 3,              │       bucket: 0,             │
  │       newFiles: [DataFileMeta │       newFiles: [DataFileMeta │
  │         {fileName, level,     │         {fileName, level,     │
  │          minSeq, maxSeq,      │          minSeq, maxSeq,      │
  │          minKey, maxKey,      │          minKey, maxKey,      │
  │          rowCount, ...}],     │          rowCount, ...}],     │
  │       changelogFiles: [...],  │       changelogFiles: [...],  │
  │       indexFiles: [...]       │       indexFiles: [...]       │
  │     }                         │     }                        │
  │                               │                              │
  │── CommitMessage[] (序列化) ──│── CommitMessage[] (序列化) ──→│
  │                               │                              │
  │                               │    聚合所有 CommitMessage     │
  │                               │    TableCommit.commit(        │
  │                               │      allMessages)             │
  │                               │    ├─ 合并 Manifest           │
  │                               │    ├─ 冲突检测                │
  │                               │    ├─ 写入新 Manifest         │
  │                               │    └─ 原子创建 Snapshot       │
```

Paimon writer 的输出是**中间产物**：`CommitMessage` 不是简单的"文件路径列表"，而是一个包含内部元数据的结构化消息：

- **`newFiles` vs `deletedFiles`**：同一个 CommitMessage 可能同时包含新增文件和要删除的文件（例如 Compaction 后：新文件是合并结果，旧文件标记删除）
- **`minSequenceNumber` / `maxSequenceNumber`**：每个文件的序列号范围，Commit 引擎用它来检测冲突和保证 LSM-tree 的一致性
- **`changelogFiles`**：如果启用了 changelog producer，会产生单独的 changelog 文件，需要在 Commit 时写入独立的 changelog manifest
- **`indexFiles` / `deletedIndexFiles`**：动态桶模式下的哈希索引文件，需要合并到 IndexManifest
- **`minKey` / `maxKey`**：用于 Manifest 的分区裁剪优化

这些元数据**必须由 Paimon 的 Commit 引擎来消费和处理**，外部不能简单地"收集文件列表然后提交"。`TableCommit.commit()` 做的事情远比 Iceberg 的 `appendFile()` 复杂——它需要合并 manifest、检测冲突、分配 row ID、维护索引、写 changelog manifest 等。

**对 Doris 架构的影响**：

| 维度 | Iceberg 模式 | Paimon 模式 |
|------|-------------|-------------|
| Writer 输出 | DataFile 列表（透明） | CommitMessage（不透明内部结构） |
| Commit 执行者 | FE 直接调 Iceberg API | 需要通过 Paimon `TableCommit` 执行 |
| Commit 的关键输入 | 文件路径 + 行数 + 分区 | 完整 CommitMessage（含序列号、键范围、索引文件等） |
| 提交失败处理 | 重试即可（文件已存在） | 需 abort() 清理数据文件，然后重试 |
| 跨 BE 聚合 | 简单合并文件列表 | 相同 (partition,bucket) 的 messages 需保持原样传递 |
| FE 需要理解的内容 | 文件元信息 | 无需理解（透传），但不能丢失任何字段 |

**为什么 Paimon 不能像 Iceberg 一样 Commit-on-Close？**

根本原因是 Paimon 的 LSM-tree 架构。Paimon 的表不是简单的"文件集合"，而是一个分层的有序结构：

1. **序列号（Sequence Number）**：LSM-tree 依赖全局递增的序列号来决定记录的可见性和冲突解决。每个 writer 写入时分配序列号，但 Commit 时 Commit 引擎需要全局地验证序列号的连续性，合并增量到 base manifest。
2. **Compaction**：Writer 在 `prepareCommit()` 时可能已经执行了 Compaction，产生了新旧文件的替换关系（`compactBefore` / `compactAfter`），这些必须在 Commit 时原子地反映到 manifest 中。如果多个 writer 各自独立 commit，会导致 manifest 冲突。
3. **Manifest 的版本一致性**：Paimon 的 Snapshot 是一个完整的 manifest 树（base manifest list + delta manifest list + changelog manifest list + index manifest）。这棵树必须在**一个原子操作**中创建，不能由多个 writer 各自添加叶子节点。

因此，Paimon 的 Commit **必须是一个集中的、原子的操作**——这就是 "Prepare-then-Commit" 的含义：Writer 负责准备（产生 CommitMessage），Coordinator 负责提交（消费 CommitMessage 创建 Snapshot）。

### 4.3 FE 端规划

```
BaseExternalTableDataSink
  └── IcebergTableSink    // 生成 TIcebergTableSink (Thrift)
      // 包含：table location, schema, partition spec, file format, output expressions

DataSink (Thrift)
  ├── TIcebergTableSink
  ├── THiveTableSink
  ├── TMaxComputeTableSink
  └── TPaimonTableSink  ← 需要新增
```

### 4.4 现有 JNI 框架

Doris 已有 JNI 调用框架用于**读取**外部表：

```
be/src/format_v2/jni/
  ├── jni_table_reader.{h,cpp}    // JNI 读取器框架
  ├── paimon_jni_reader.{h,cpp}   // Paimon JNI 读取器
  ├── hudi_jni_reader.{h,cpp}     // Hudi JNI 读取器
  └── ...

be/src/format/jni/
  ├── jni_data_bridge.{h,cpp}     // C++ ↔ Java 数据桥接
  └── jni_reader.{h,cpp}
```

**JNI 模式的写路径参考**：目前没有 JNI 写路径（只有读取），需要新建。

---

## 5. 架构设计

### 5.1 总体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Doris FE                                   │
│  ┌──────────────┐   ┌──────────────────┐   ┌─────────────────────┐ │
│  │ Nereids      │   │ PaimonTableSink  │   │ Thrift:             │ │
│  │ Planner      │──▶│ (FE Planner)     │──▶│ TPaimonTableSink    │ │
│  └──────────────┘   └──────────────────┘   └─────────┬───────────┘ │
└───────────────────────────────────────────────────────┼─────────────┘
                                                        │ TDataSink
                                                        ▼
┌───────────────────────────────────────────────────────┴─────────────┐
│                          Doris BE                                   │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              VPaimonTableWriter (AsyncResultWriter)           │  │
│  │                                                              │  │
│  │  write(Block) → partition/bucket routing → dispatch to       │  │
│  │       VPaimonPartitionWriter × N (per partition+bucket)      │  │
│  │                                                              │  │
│  │  close() → collect CommitMessage[] from all writers         │  │
│  └──────────────────────┬───────────────────────────────────────┘  │
│                         │                                          │
│                         ▼                                          │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                 IPaimonWriteBackend (抽象层)                  │  │
│  │                                                              │  │
│  │  + open(TPaimonTableSink) → Status                          │  │
│  │  + createWriter(partition, bucket) → IPaimonWriter           │  │
│  │  + createCommit() → IPaimonCommitter                        │  │
│  │  + getCommiter() → IPaimonCommitter (singleton per sink)     │  │
│  └──────────┬───────────────────────────────┬───────────────────┘  │
│             │                               │                      │
│             ▼                               ▼                      │
│  ┌──────────────────┐             ┌──────────────────┐            │
│  │ JniWriteBackend  │             │ FfiWriteBackend  │            │
│  │ (Java SDK)       │             │ (Rust lib)       │            │
│  │                  │             │                  │            │
│  │ via JNI:         │             │ via C FFI:       │            │
│  │ BatchTableWrite  │             │ TableWrite       │            │
│  │ BatchTableCommit │             │ TableCommit      │            │
│  │ CommitMessage    │             │ CommitMessage    │            │
│  │ Serializer       │             │ (direct struct)  │            │
│  └──────────────────┘             └──────────────────┘            │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              CommitMessage (统一格式，跨后端)                  │  │
│  │                                                              │  │
│  │  BE 本地聚合 → FE Coordinator 全局聚合 → 统一提交              │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 核心设计决策

#### 决策 1：统一 CommitMessage 格式

**设计**：在 BE 层定义一个 C++ 版本的 `CommitMessage` 结构体，同时能被 JNI 后端（Java 序列化反序列化）和 FFI 后端（Rust 结构体）填充。

```cpp
// be/src/exec/sink/writer/paimon/paimon_commit_message.h
struct PaimonDataFileMeta {
    std::string file_name;
    int64_t file_size;
    int64_t row_count;
    int64_t delete_row_count;
    int64_t min_sequence_number;
    int64_t max_sequence_number;
    int32_t level;
    int32_t schema_id;
    std::string min_key;    // serialized BinaryRow
    std::string max_key;
    // ... 更多元数据字段
};

struct PaimonCommitMessage {
    std::string partition;   // serialized BinaryRow bytes
    int32_t bucket;
    std::optional<int32_t> total_buckets;

    std::vector<PaimonDataFileMeta> new_files;
    std::vector<PaimonDataFileMeta> new_changelog_files;
    std::vector<PaimonIndexFileMeta> new_index_files;
    std::vector<PaimonIndexFileMeta> deleted_index_files;
    std::vector<PaimonDataFileMeta> deleted_files;
    std::optional<int64_t> check_from_snapshot;

    // 序列化为 Thrift 格式，传递给 FE Coordinator
    void to_thrift(TPaimonCommitMessage* t_msg) const;
};
```

**理由**：
- 解耦写入后端和提交逻辑
- FE Coordinator 需要聚合所有 BE 的 CommitMessage，因此需要一个统一的序列化格式
- 使用 Thrift（Doris 的标准 RPC 序列化）来传输 CommitMessage 到 FE

#### 决策 2：IPaimonWriteBackend 抽象

**设计**：定义一个 C++ 抽象接口，屏蔽 JNI 和 FFI 的差异。

```cpp
// be/src/exec/sink/writer/paimon/paimon_write_backend.h
class IPaimonWriter {
public:
    virtual ~IPaimonWriter() = default;
    // 写入一个 Block（列式批次）
    virtual Status write(RuntimeState* state, Block& block) = 0;
    // 关闭当前 writer，产生 CommitMessage
    virtual Status prepareCommit(std::vector<PaimonCommitMessage>& messages) = 0;
    // 强制 compaction（可选，仅 Java 后端支持）
    virtual Status compact(bool fullCompaction) = 0;
    // 丢弃当前写入
    virtual Status abort() = 0;
};

class IPaimonCommitter {
public:
    virtual ~IPaimonCommitter() = default;
    // 提交所有 messages
    virtual Status commit(const std::vector<PaimonCommitMessage>& messages) = 0;
    // 分区覆盖提交
    virtual Status overwrite(const std::vector<PaimonCommitMessage>& messages,
                            const std::optional<std::map<std::string, std::string>>& staticPartition) = 0;
    // 清空表
    virtual Status truncateTable() = 0;
    // 清空分区
    virtual Status truncatePartitions(const std::vector<std::map<std::string, std::string>>& partitions) = 0;
    // 中止（删除数据文件）
    virtual Status abort(const std::vector<PaimonCommitMessage>& messages) = 0;
};

class IPaimonWriteBackend {
public:
    virtual ~IPaimonWriteBackend() = default;

    // 初始化
    virtual Status open(const TPaimonTableSink& sink) = 0;

    // 为特定 partition+bucket 创建 writer
    virtual Status createWriter(const std::string& partition,
                                int32_t bucket,
                                std::unique_ptr<IPaimonWriter>* writer) = 0;

    // 获取 committer（整个 sink 共享一个 committer）
    virtual Status createCommitter(std::unique_ptr<IPaimonCommitter>* committer) = 0;

    // 后端类型标识
    virtual PaimonBackendType type() const = 0;

    // 功能查询
    virtual bool supportsCompaction() const = 0;
    virtual bool supportsLookupChangelog() const = 0;
    virtual bool supportsFullCompactionChangelog() const = 0;
};
```

#### 决策 3：双后端，可互相替代

Java writer 和 Rust writer 是**互相替代**的关系，不是互补关系。Rust 是 Java 的功能子集：

```
功能全集 (Java SDK)
┌─────────────────────────────────────────┐
│  ┌─────────────────────────────────┐    │
│  │  功能子集 (Rust)                │    │
│  │  · Append-only 写入             │    │
│  │  · 固定桶 PK 写入               │    │
│  │  · 动态桶 PK 写入               │    │
│  │  · MergeEngine: Normal/         │    │
│  │    PartialUpdate/Aggregation/   │    │
│  │    FirstRow/Deduplicate         │    │
│  │  · ChangelogProducer: None/Input│    │
│  │  · Overwrite/Truncate           │    │
│  └─────────────────────────────────┘    │
│  Rust 缺失（需回退 Java）：             │
│  · Compaction                          │
│  · Lookup/FullCompaction Changelog      │
│  · DeletionVector + PartialUpdate/      │
│    Aggregation 组合                     │
│  · MERGE INTO / DataEvolution           │
└─────────────────────────────────────────┘
```

**选择策略：优先 Rust，功能不足时回退 Java**

```
IF table options 包含 Rust 不支持的功能:
  → Java JNI Backend  ← 功能完备保证
ELSE:
  → Rust FFI Backend   ← 优先选择，性能更优
```

**v1 实现策略**：

```
v1: 仅实现 Java JNI Backend，但：
  - IPaimonWriteBackend 接口从第一天就存在
  - VPaimonTableWriter 通过 IPaimonWriteBackend 调用
  - Java 后端是实现细节，不是架构主体
  - 新增 Rust 后端 = 新增一个 IPaimonWriteBackend 实现类，不碰上层代码

v2: 实现 Rust FFI Backend
  - 扩展 paimon-rust C binding
  - 新增 FfiPaimonWriteBackend（<1000行新代码）
  - 上层 VPaimonTableWriter 无需修改
```

| 场景 | v1 (仅 Java) | v2 (Java + Rust) |
|------|-------------|-------------------|
| Append-only 表 | JNI Backend | **FFI Backend** (优先) |
| PK 表，固定桶，No Compaction | JNI Backend | **FFI Backend** (优先) |
| PK 表，需要 Compaction | JNI Backend | JNI Backend (Rust 不支持) |
| Lookup Changelog Producer | JNI Backend | JNI Backend (Rust 不支持) |
| MERGE INTO | JNI Backend | JNI Backend (Rust 不支持) |

### 5.3 提交流程

Paimon 的两阶段提交对 Doris 的分布式架构提出了特殊挑战：

```
时间线：分布式写入 + 集中式提交

BE1: write() → prepareCommit() → CommitMessage[]
BE2: write() → prepareCommit() → CommitMessage[]
BE3: write() → prepareCommit() → CommitMessage[]
  │               │                    │
  └───────────────┼────────────────────┘
                  │  (通过 Thrift RPC 发送到 FE Coordinator)
                  ▼
FE Coordinator: 聚合所有 CommitMessage[]
                  │
                  ▼
         TableCommit.commit(allMessages)
                  │
                  ▼
         新的 Snapshot 创建
```

**关键问题**：Commit 应该在哪发生？

**选择 1：FE Coordinator 执行 Commit**
- 优点：自然的两阶段提交，FE 已有 Iceberg 的集中式提交模式
- 缺点：FE 需要持有 Java SDK 依赖（或通过 BE 代理）

**选择 2：选出一个 BE 执行 Commit（Leader BE）**
- 优点：BE 端直接调用 JNI/FFI
- 缺点：需要选举逻辑，增加复杂度

**选择 3：混合方案** ✅ **推荐**

```
BE 端：
  - 数据写入（全并行）
  - prepareCommit() → CommitMessage[]（序列化为 Thrift）

FE Coordinator 端：
  - 聚合所有 BE 的 CommitMessage[]
  - 通过专用的 "Commit BE" 执行 TableCommit.commit()

Commit BE（可以是任意 BE 或专用 BE）：
  - 接收 FE 发来的聚合后的 CommitMessage[]
  - 调用 JNI Backend 的 TableCommit.commit()
  - 返回结果给 FE
```

对于 Append-only 表（简单场景），可以直接在 FE 端通过 Java SDK 执行提交。对于需要 Compaction 的表（复杂场景），通过 Commit BE 执行。

### 5.4 架构图总览（分场景）

#### 场景 1：Append-only 表写入

```
Doris Sink
  │
  ▼
VPaimonTableWriter (BE)
  │  Block[col1, col2, ...]
  ▼
Partition/Bucket Router
  │  hash(bucket_key) % N → bucket
  │  partition columns → partition path
  ▼
VPaimonPartitionWriter × N
  │  每个 (partition, bucket) 一个 writer
  │  写入数据批次
  ▼
IPaimonWriter (FFI Backend preferred)
  │  Arrow RecordBatch → Rust TableWrite::write_arrow_batch()
  │  高效：零拷贝，异步 I/O
  ▼
prepareCommit() → PaimonCommitMessage[]

  ▼ (Thrift RPC)
FE Coordinator
  │  聚合所有 BE 的 messages
  ▼
Commit BE: IPaimonCommitter::commit(messages)
  │  调用 TableCommit::commit()
  ▼
New Snapshot ✓
```

#### 场景 2：主键表写入（需要 Compaction）

```
Doris Sink
  │
  ▼
VPaimonTableWriter (BE)
  │  Block[...]
  ▼
Partition/Bucket Router → IPaimonWriter (JNI Backend required)
  │  使用 JNI Backend 因为：
  │  1. 需要 KeyValueFileWriter
  │  2. 需要 sequence number 管理
  │  3. 需要 MergeEngine 逻辑
  │  4. 需要 Compaction
  ▼
write() → Java BatchTableWrite.write(row) × N
  │
  ▼
compact() → Java BatchTableWrite.compact(partition, bucket, fullCompaction)
  │  (定期触发或在 commit 前触发)
  ▼
prepareCommit() → CommitMessage[]
  │
  ▼ (Thrift RPC)
FE Coordinator → Commit BE → commit()
  ▼
New Snapshot + Compaction result ✓
```

---

## 6. 接口定义

### 6.1 Thrift 定义

```thrift
// gensrc/thrift/DataSinks.thrift

enum TPaimonWriteBackendType {
    JNI = 0,       // Java SDK via JNI
    FFI = 1,       // Rust via C FFI
}

enum TPaimonWriteMode {
    APPEND = 0,           // INSERT INTO
    OVERWRITE = 1,        // INSERT OVERWRITE
    DYNAMIC_OVERWRITE = 2, // 动态分区覆盖
    MERGE_INTO = 3,       // DataEvolution MERGE INTO
}

struct TPaimonTableSink {
    // 表标识
    1: required string table_location;     // Paimon 表路径
    2: required string paimon_catalog_type; // filesystem / hive / rest
    3: optional map<string, string> catalog_options;
    4: optional map<string, string> table_options;

    // Schema
    5: required string schema_json;         // Paimon TableSchema JSON
    6: required list<string> partition_keys;
    7: required list<string> primary_keys;
    8: required list<i32> primary_key_indices;

    // 写入配置
    9: required TPaimonWriteMode write_mode;
    10: optional map<string, string> static_partition; // overwrite 分区 spec
    11: required TPaimonWriteBackendType backend_type;

    // 输出表达式（Doris Block → Paimon Row 映射）
    12: required list<TExpr> output_exprs;

    // 文件配置
    13: optional string file_format;       // parquet / orc / avro
    14: optional string file_compression;  // zstd / snappy / lz4 / none
    15: optional i64 target_file_size;    // bytes

    // 提交配置
    16: optional string commit_user;
    17: optional i32 commit_max_retries;
    18: optional i64 commit_timeout_ms;

    // Hadoop 配置（对于 Hive Metastore catalog）
    19: optional map<string, string> hadoop_config;
}

struct TPaimonCommitMessage {
    1: required binary partition;          // serialized BinaryRow
    2: required i32 bucket;
    3: optional i32 total_buckets;
    4: required list<TPaimonDataFileMeta> new_files;
    5: optional list<TPaimonDataFileMeta> new_changelog_files;
    6: optional list<TPaimonIndexFileMeta> new_index_files;
    7: optional list<TPaimonIndexFileMeta> deleted_index_files;
    8: optional list<TPaimonDataFileMeta> deleted_files;
    9: optional i64 check_from_snapshot;
}

struct TPaimonDataFileMeta {
    1: required string file_name;
    2: required i64 file_size;
    3: required i64 row_count;
    4: optional i64 delete_row_count;
    5: required i64 min_sequence_number;
    6: required i64 max_sequence_number;
    7: required i32 level;
    8: required i32 schema_id;
    9: required binary min_key;
    10: required binary max_key;
    11: optional binary min_partition_key;
    12: optional binary max_partition_key;
    13: optional list<string> write_cols;
    14: optional i64 first_row_id;
    15: optional i32 file_source;
    // ... 其他元数据
}

struct TPaimonIndexFileMeta {
    1: required string file_name;
    2: required i64 file_size;
    3: required i64 row_count;
    4: required string index_type;   // "HASH" / "BTREE" / "GLOBAL"
    5: optional TPaimonGlobalIndexMeta global_index_meta;
}

struct TPaimonCommitResult {
    1: required bool success;
    2: optional i64 new_snapshot_id;
    3: optional string error_message;
}

// 添加到 TDataSink
struct TDataSink {
    // ... existing fields ...
    16: optional TPaimonTableSink paimon_table_sink
}
```

### 6.2 C++ 抽象接口（完整版）

```cpp
// be/src/exec/sink/writer/paimon/paimon_write_backend.h

namespace doris {

// ──── 枚举 ────────────────────────────────────────────────

enum class PaimonBackendType {
    JNI,  // Java SDK
    FFI   // Rust C FFI
};

enum class PaimonWriteMode {
    APPEND,
    OVERWRITE,
    DYNAMIC_OVERWRITE,
    MERGE_INTO
};

// ──── 数据结构 ────────────────────────────────────────────

struct PaimonWriteOptions {
    std::string table_location;
    std::string commit_user;
    PaimonWriteMode write_mode;
    std::optional<std::unordered_map<std::string, std::string>> static_partition;
    std::string file_format;       // "parquet", "orc", "avro"
    std::string file_compression;  // "zstd", "snappy", "lz4", "none"
    int64_t target_file_size_bytes = 128 * 1024 * 1024; // 128MB default
    int32_t commit_max_retries = 5;
    int64_t commit_timeout_ms = 300000; // 5min
};

// ──── IPaimonWriter ───────────────────────────────────────

class IPaimonWriter {
public:
    virtual ~IPaimonWriter() = default;

    /// 写入一个 Doris Block
    /// @param state  运行时状态
    /// @param block  输入数据（Doris 列式格式）
    virtual Status write(RuntimeState* state, Block& block) = 0;

    /// 准备提交：关闭文件，生成 CommitMessage
    /// @param messages  输出：本次写入产生的 CommitMessage 列表
    virtual Status prepareCommit(
        std::vector<PaimonCommitMessage>& messages) = 0;

    /// 强制 compaction（仅部分后端支持）
    /// @param fullCompaction true=全量压缩, false=增量压缩
    virtual Status compact(bool fullCompaction) {
        return Status::NotSupported("compact not supported by this backend");
    }

    /// 丢弃本次写入的所有数据
    virtual Status abort() = 0;
};

// ──── IPaimonCommitter ────────────────────────────────────

class IPaimonCommitter {
public:
    virtual ~IPaimonCommitter() = default;

    /// 追加提交：将所有 CommitMessage 中的数据文件原子提交
    virtual Status commit(
        const std::vector<PaimonCommitMessage>& messages) = 0;

    /// 覆盖提交：删除旧数据，写入新数据
    virtual Status overwrite(
        const std::vector<PaimonCommitMessage>& messages,
        const std::optional<std::unordered_map<std::string, std::string>>&
            static_partition) = 0;

    /// 清空整张表
    virtual Status truncateTable() = 0;

    /// 清空指定分区
    virtual Status truncatePartitions(
        const std::vector<std::unordered_map<std::string, std::string>>&
            partitions) = 0;

    /// 中止：删除已写入但未提交的数据文件
    virtual Status abort(
        const std::vector<PaimonCommitMessage>& messages) = 0;
};

// ──── IPaimonWriteBackend ─────────────────────────────────

class IPaimonWriteBackend {
public:
    virtual ~IPaimonWriteBackend() = default;

    /// 初始化后端
    virtual Status open(const TPaimonTableSink& sink,
                        RuntimeState* state) = 0;

    /// 为特定的 (partition, bucket) 创建一个新的 writer
    /// 同一个 (partition, bucket) 可以被多次调用（每次创建一个新的文件 writer）
    virtual Status createWriter(
        const std::string& partition_bytes,
        int32_t bucket,
        std::unique_ptr<IPaimonWriter>* writer) = 0;

    /// 创建一个 committer（每个 sink 只需要一个）
    virtual Status createCommitter(
        std::unique_ptr<IPaimonCommitter>* committer) = 0;

    /// 获取后端类型
    virtual PaimonBackendType type() const = 0;

    // ──── 功能查询 ────────────────────────────────────────

    virtual bool supportsCompaction() const = 0;
    virtual bool supportsLookupChangelogProducer() const = 0;
    virtual bool supportsFullCompactionChangelogProducer() const = 0;
    virtual bool supportsPartialUpdateWithDV() const = 0;
    virtual bool supportsAggregationWithDV() const = 0;
};

// ──── 后端工厂 ────────────────────────────────────────────

class PaimonWriteBackendFactory {
public:
    /// 根据配置创建合适的后端
    static Status create(
        const TPaimonTableSink& sink,
        std::unique_ptr<IPaimonWriteBackend>* backend);

    /// 根据表属性和用户偏好选择最佳后端类型
    static PaimonBackendType selectBackendType(
        const TPaimonTableSink& sink);
};

} // namespace doris
```

---

## 7. 数据流与交互时序

### 7.1 完整写入流程（时序图）

```
 FE Coordinator          BE Worker 1           BE Worker 2         Commit BE
      │                      │                     │                   │
      │  TDataSink           │                     │                   │
      │  (TPaimonTableSink)  │                     │                   │
      ├──────────────────────┤                     │                   │
      │                      │                     │                   │
      │                   open()                 open()                │
      │                   IPaimonWriteBackend    IPaimonWriteBackend   │
      │                      │                     │                   │
      │  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐                   │
      │  │ 写入阶段（并行）                        │                   │
      │  │                                         │                   │
      │  │ Block1[rows]                            │                   │
      │  ├─────────────────────────────────────────┤                   │
      │  │  partition/bucket 路由                  │                   │
      │  │  write() → IPaimonWriter                │                   │
      │  │                                         │                   │
      │  │              Block2[rows]               │                   │
      │  ├─────────────────────────────────────────┤                   │
      │  │              路由 → write()             │                   │
      │  │                                         │                   │
      │  │ ... (更多数据) ...                      │                   │
      │  │                                         │                   │
      │  │ EOS (End of Stream)                     │                   │
      │  ├─────────────────────────────────────────┤                   │
      │  │  prepareCommit()                        │                   │
      │  │  → CommitMessage[]                      │                   │
      │  │                                         │                   │
      │  │              prepareCommit()            │                   │
      │  ├─────────────────────────────────────────┤                   │
      │  │              → CommitMessage[]          │                   │
      │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘                   │
      │                      │                     │                   │
      │  CommitMessage[]     │   CommitMessage[]   │                   │
      │  (Thrift RPC)        │   (Thrift RPC)      │                   │
      │◄─────────────────────┼─────────────────────┤                   │
      │                      │                     │                   │
      │  聚合 & 去重         │                     │                   │
      │  allMessages[]       │                     │                   │
      │                      │                     │                   │
      │                      │   commit(allMessages)                    │
      ├──────────────────────────────────────────────────────────────────┤
      │                      │                     │   TableCommit      │
      │                      │                     │   .commit()        │
      │                      │                     │   ├─ write manifests│
      │                      │                     │   ├─ write snapshot │
      │                      │                     │   └─ atomic rename  │
      │                      │                     │                   │
      │  CommitResult        │                     │                   │
      │◄──────────────────────────────────────────────────────────────────┤
      │                      │                     │                   │
      │  (on success)        │                     │                   │
      │  close() writers     │                     │                   │
      ├──────────────────────┤                     │                   │
      │                      │                     │                   │
      │  (on failure)        │                     │                   │
      │  abort(messages)     │                     │                   │
      ├──────────────────────┼─────────────────────┤                   │
```

### 7.2 错误处理流程

```
写入阶段可能的错误：
  1. 网络/存储 I/O 错误
     → 重试（with exponential backoff）
     → 超过重试次数 → 标记 task 失败 → FE 重新调度

  2. 内存不足
     → spill to disk (if spillable enabled)
     → 触发 memory limiter → 减少并发度

  3. Schema mismatch
     → 立即失败，不回滚已写入的文件
     → 依赖 Snapshot 的隔离性，未提交的数据不会被读到

提交阶段可能的错误：
  1. Commit conflict（并发提交）
     → 自动重试（commit_max_retries times）
     → 检查 idempotency (commit_user + commit_identifier)

  2. 提交超时
     → 检查是否已经提交成功（idempotency check）
     → 如果未成功 → 根据 Snapshot 状态决定重试还是放弃

  3. 写入成功但提交失败 = 孤儿文件
     → 调用 abort(messages) 清理数据文件
     → 下次 Snapshot expiration 时也会清理未引用的文件
```

---

## 8. FE 端设计

### 8.1 PaimonTableSink（FE Planner）

```java
// fe/fe-core/src/main/java/org/apache/doris/planner/PaimonTableSink.java

public class PaimonTableSink extends BaseExternalTableDataSink {

    private final PaimonExternalTable targetTable;
    private List<Expr> outputExprs;
    private PaimonWriteMode writeMode;
    private Map<String, String> staticPartition;

    public PaimonTableSink(PaimonExternalTable targetTable, InsertCommandContext ctx) {
        this.targetTable = targetTable;
        // 从 ctx 确定写入模式
        if (ctx.isOverwrite()) {
            this.writeMode = PaimonWriteMode.OVERWRITE;
            this.staticPartition = ctx.getStaticPartition();
        } else {
            this.writeMode = PaimonWriteMode.APPEND;
        }
    }

    @Override
    public TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);

        TPaimonTableSink paimonSink = new TPaimonTableSink();
        paimonSink.setTableLocation(targetTable.getTableLocation());
        paimonSink.setPaimonCatalogType(targetTable.getCatalogType());
        paimonSink.setSchemaJson(targetTable.getPaimonSchemaJson());
        paimonSink.setPartitionKeys(targetTable.getPartitionKeys());
        paimonSink.setPrimaryKeys(targetTable.getPrimaryKeys());
        paimonSink.setPrimaryKeyIndices(targetTable.getPrimaryKeyIndices());
        paimonSink.setWriteMode(writeMode.toThrift());
        paimonSink.setOutputExprs(Expr.treesToThrift(outputExprs));
        paimonSink.setFileFormat(targetTable.getFileFormat());
        paimonSink.setFileCompression(targetTable.getFileCompression());
        paimonSink.setTargetFileSize(targetTable.getTargetFileSize());

        // 选择后端类型
        paimonSink.setBackendType(selectBackendType().toThrift());

        // 生成 commit_user
        paimonSink.setCommitUser(UUID.randomUUID().toString());

        result.setPaimonTableSink(paimonSink);
        return result;
    }

    /**
     * 智能选择写入后端：
     * - 如果需要 Compaction 或 Lookup/FullCompaction Changelog，必须用 JNI
     * - 如果用户指定了 preferred backend，遵循用户选择
     * - 默认：JNI（功能优先）
     */
    private TPaimonWriteBackendType selectBackendType() {
        // 检查表是否配置了需要 Java SDK 才支持的功能
        if (targetTable.requiresCompaction() ||
            targetTable.getChangelogProducer().requiresJava()) {
            return TPaimonWriteBackendType.JNI;
        }

        // 检查用户偏好
        String configuredBackend = targetTable.getTableProperties()
            .get("doris.paimon.write.backend");
        if ("ffi".equalsIgnoreCase(configuredBACKend) ||
            "rust".equalsIgnoreCase(configuredBACKend)) {
            return TPaimonWriteBackendType.FFI;
        }

        // 默认使用 JNI（功能最完整）
        return TPaimonWriteBackendType.JNI;
    }
}
```

### 8.2 PaimonWriteCommitCoordinator（FE Coordinator）

```java
// fe/fe-core/src/main/java/org/apache/doris/transaction/PaimonWriteCommitCoordinator.java

public class PaimonWriteCommitCoordinator {

    /**
     * 协调分布式写入的提交：
     * 1. 收集所有 BE 的 CommitMessage[]
     * 2. 聚合去重
     * 3. 选择一个 BE 执行提交（或 FE 本地执行）
     * 4. 处理提交结果
     */
    public TPaimonCommitResult commit(
            Database db,
            PaimonExternalTable table,
            List<List<TPaimonCommitMessage>> allBackendMessages) {

        // Step 1: 扁平化所有 BE 的 messages
        List<TPaimonCommitMessage> allMessages = allBackendMessages.stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());

        if (allMessages.isEmpty()) {
            return new TPaimonCommitResult(true); // nothing to commit
        }

        // Step 2: 聚合相同 (partition, bucket) 的 messages
        Map<Pair<byte[], Integer>, List<TPaimonCommitMessage>> grouped =
            groupByPartitionBucket(allMessages);

        // Step 3: 选择 Commit BE
        TNetworkAddress commitBE = selectCommitBE(allBackendMessages);

        // Step 4: 通过 RPC 调用 Commit BE 执行提交
        TPaimonCommitRequest request = new TPaimonCommitRequest();
        request.setTableLocation(table.getTableLocation());
        request.setCommitMessages(allMessages);
        request.setCommitUser(generateCommitUser());

        try {
            TPaimonCommitResult result = BackendServiceProxy.getInstance()
                .paimonCommit(commitBE, request);
            return result;
        } catch (Exception e) {
            // 提交失败处理
            return handleCommitFailure(allMessages, e);
        }
    }
}
```

---

## 9. BE 端设计

### 9.1 VPaimonTableWriter

```cpp
// be/src/exec/sink/writer/paimon/vpaimon_table_writer.h

class VPaimonTableWriter final : public AsyncResultWriter {
public:
    VPaimonTableWriter(const TDataSink& t_sink,
                       const VExprContextSPtrs& output_exprs,
                       std::shared_ptr<Dependency> dep,
                       std::shared_ptr<Dependency> fin_dep);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;
    Status write(RuntimeState* state, Block& block) override;
    Status close(Status status) override;

private:
    // 分区 & 桶路由
    Status _routeBlock(Block& block,
                       std::unordered_map<PartitionBucketKey, Block>& routed);

    // 获取或创建 partition writer
    Status _getOrCreateWriter(const std::string& partition, int32_t bucket,
                             std::shared_ptr<VPaimonPartitionWriter>* writer);

    // 生成 CommitMessage 列表
    Status _prepareCommit(std::vector<PaimonCommitMessage>& messages);

    TDataSink _t_sink;
    RuntimeState* _state = nullptr;

    // 抽象后端
    std::unique_ptr<IPaimonWriteBackend> _backend;
    std::unique_ptr<IPaimonCommitter> _committer; // 预留，实际提交不在这里

    // 活跃的 partition writers
    std::unordered_map<PartitionBucketKey,
                       std::shared_ptr<VPaimonPartitionWriter>> _writers;

    // 路由信息
    std::vector<int> _partition_col_indices;
    std::vector<int> _bucket_key_indices;
    int _total_buckets = 0;

    // 统计
    int64_t _rows_written = 0;
    int64_t _bytes_written = 0;
};
```

### 9.2 JNI Backend 实现

```cpp
// be/src/exec/sink/writer/paimon/jni_paimon_write_backend.h

class JniPaimonWriteBackend : public IPaimonWriteBackend {
public:
    Status open(const TPaimonTableSink& sink, RuntimeState* state) override;

    Status createWriter(const std::string& partition, int32_t bucket,
                        std::unique_ptr<IPaimonWriter>* writer) override;

    Status createCommitter(
        std::unique_ptr<IPaimonCommitter>* committer) override;

    PaimonBackendType type() const override { return PaimonBackendType::JNI; }

    bool supportsCompaction() const override { return true; }
    bool supportsLookupChangelogProducer() const override { return true; }
    bool supportsFullCompactionChangelogProducer() const override { return true; }
    bool supportsPartialUpdateWithDV() const override { return true; }
    bool supportsAggregationWithDV() const override { return true; }

private:
    // JNI 全局引用
    jobject _write_builder;   // org.apache.paimon.table.sink.BatchWriteBuilder
    jobject _jni_env_context; // JNI 执行环境

    // 桥接方法
    Status _callJavaWrite(jobject writer, Block& block);
    Status _callJavaPrepareCommit(jobject writer,
                                   std::vector<PaimonCommitMessage>& messages);
    Status _serializeCommitMessages(jobject javaMessages,
                                     std::vector<PaimonCommitMessage>& cppMessages);
};
```

### 9.3 FFI (Rust) Backend 实现

```cpp
// be/src/exec/sink/writer/paimon/ffi_paimon_write_backend.h

class FfiPaimonWriteBackend : public IPaimonWriteBackend {
public:
    Status open(const TPaimonTableSink& sink, RuntimeState* state) override;

    Status createWriter(const std::string& partition, int32_t bucket,
                        std::unique_ptr<IPaimonWriter>* writer) override;

    Status createCommitter(
        std::unique_ptr<IPaimonCommitter>* committer) override;

    PaimonBackendType type() const override { return PaimonBackendType::FFI; }

    // 功能查询 — Rust 目前的能力边界
    bool supportsCompaction() const override { return false; }
    bool supportsLookupChangelogProducer() const override { return false; }
    bool supportsFullCompactionChangelogProducer() const override { return false; }
    bool supportsPartialUpdateWithDV() const override { return false; }
    bool supportsAggregationWithDV() const override { return false; }

private:
    // 动态加载的 Rust 共享库
    void* _lib_handle;

    // C FFI 函数指针
    // 通过 paimon-rust bindings/c 提供
    using PaimonWriteBuilderNew = void* (*)(/* args */);
    using PaimonTableWriteNew = void* (*)(void* builder);
    using PaimonTableWriteArrowBatch = int32_t (*)(void* writer,
        FFI_ArrowArray* array, FFI_ArrowSchema* schema);
    using PaimonTableWritePrepareCommit = void* (*)(void* writer); // returns CommitMessage[]
    using PaimonTableCommitNew = void* (*)(void* builder);
    using PaimonTableCommitCommit = int32_t (*)(void* commit, void* messages);
    // ... 等等

    PaimonWriteBuilderNew _ffi_write_builder_new;
    PaimonTableWriteNew _ffi_table_write_new;
    PaimonTableWriteArrowBatch _ffi_write_arrow_batch;
    PaimonTableWritePrepareCommit _ffi_prepare_commit;
    PaimonTableCommitNew _ffi_table_commit_new;
    PaimonTableCommitCommit _ffi_commit;
};
```

**注意**：`paimon-rust/bindings/c/` 目前只暴露了**读取** API。写入 API 需要新增。这是实施路线图中的第一步。

---

## 10. 功能路由矩阵

### 10.1 写入模式路由

原则：**优先 Rust，功能不足回退 Java**。Rust 是 Java 的功能子集。

| 表类型 | v1 (仅Java) | v2 首选 | v2 回退 | 回退原因 |
|--------|------------|---------|---------|----------|
| Append-only（任意分区） | JNI | **FFI** | JNI | Rust 覆盖最充分 |
| Append-only + Blob 列 | JNI | **FFI** | JNI | Rust 支持 |
| PK 表，固定桶 | JNI | **FFI** | JNI | KeyValueFileWriter |
| PK 表，动态桶 | JNI | **FFI** | JNI | DynamicBucketAssigner |
| PK 表，Postpone 桶 | JNI | **FFI** | JNI | PostponeFileWriter |
| ChangelogProducer=Input | JNI | **FFI** | JNI | Rust 支持 |
| INSERT OVERWRITE | JNI | **FFI** | JNI | Rust 支持 |
| Truncate table/partition | JNI | **FFI** | JNI | Rust 支持 |
| ChangelogProducer=Lookup | JNI | JNI | - | Rust **不支持** |
| ChangelogProducer=FullCompaction | JNI | JNI | - | Rust **不支持** |
| PartialUpdate + DeletionVector | JNI | JNI | - | Rust **不支持** |
| Aggregation + DeletionVector | JNI | JNI | - | Rust **不支持** |
| MERGE INTO (有 PK) | JNI | JNI | - | Rust **不支持** |
| MERGE INTO (无 PK) | JNI | JNI | - | Rust **不支持** |
| 需要 Compaction | JNI | JNI | - | Rust **不支持** |

**v2 回退规则**：一旦表的 options 中包含任何 Rust 不支持的功能，整个写入退回到 Java Backend。

### 10.2 功能对齐路线

```
Phase 1 (v1) — Java JNI Backend + 抽象层：
  ✅ 建立 IPaimonWriteBackend / IPaimonWriter / IPaimonCommitter 接口
  ✅ JniPaimonWriteBackend（完整实现）
  ✅ FfiPaimonWriteBackend（stub，所有方法返回 NotSupported）
  ✅ 结构化 Thrift CommitMessage
  ✅ BE 端 partition/bucket 路由
  ✅ Append-only + 固定桶 PK 写入
  ✅ INSERT OVERWRITE / Truncate
  ✅ ChangelogProducer: None / Input
  ✅ 所有 MergeEngine
  ⬜ Compaction（Java 后端）
  ⬜ ChangelogProducer: Lookup / FullCompaction（Java 后端）

Phase 2 (v2) — Rust FFI Backend：
  ⬜ 扩展 paimon-rust C binding（写入 API）
  ⬜ FfiPaimonWriteBackend 从 stub 变为完整实现
  ⬜ 零拷贝 Arrow C Data Interface
  ⬜ Rust tokio runtime 集成
  ⬜ PaimonWriteBackendFactory 自动后端选择
  ⬜ 上层代码零改动（VPaimonTableWriter 不变）

Phase 3（高级功能补齐）：
  ⬜ Compaction 触发与执行（Java 后端）
  ⬜ Lookup / FullCompaction Changelog Producer（Java 后端）
  ⬜ MERGE INTO / DataEvolution（Java 后端）
  ⬜ 动态桶 + 跨分区桶

Phase 4（Rust 功能追赶）：
  ⬜ Rust Compaction 实现
  ⬜ Rust Lookup Changelog Producer
  ⬜ Rust DV + PartialUpdate/Aggregation
```

---

## 11. 实施路线图

### Phase 1：v1 — Java JNI Backend + 预设计 Rust 接口（约 6-8 周）

**目标**：基于 Java SDK 实现功能完备的 Paimon 写入，同时建立 `IPaimonWriteBackend` 抽象为 Rust 后端预留空间。

**核心原则**：虽然只实现 Java 后端，但**架构代码从第一天起就通过抽象接口调用**。新增 Rust 后端 = 新增一个 `IPaimonWriteBackend` 的实现类（<1000行），不动上层代码。

1. **Thrift 定义**
   - 新增 `TPaimonTableSink`（含 `backend_type` 字段，v1 固定为 JNI）
   - 新增结构化 `TPaimonCommitMessage`（含完整 DataFileMeta、IndexFileMeta）
   - 新增 `TPaimonCommitResult`
   - 在 `TDataSink` 中添加 `paimon_table_sink` 字段

2. **BE 端 — 抽象层（关键）**
   - **`IPaimonWriteBackend`** 接口：`open()` / `createWriter()` / `createCommitter()` / 能力查询
   - **`IPaimonWriter`** 接口：`write()` / `prepareCommit()` / `compact()` / `abort()`
   - **`IPaimonCommitter`** 接口：`commit()` / `overwrite()` / `truncateTable()` / `truncatePartitions()` / `abort()`
   - **`PaimonWriteBackendFactory`**：`selectBackendType()` + `create()`
   - 这些接口从第一天起就存在，Java 后端只是第一个实现

3. **BE 端 — JNI 后端实现**
   - **`JniPaimonWriteBackend`**：实现 `IPaimonWriteBackend`
   - **`VPaimonTableWriter`**：继承 `AsyncResultWriter`，通过 `IPaimonWriteBackend` 接口调用（不直接依赖 JNI）
   - Block → Arrow → Java 的数据桥接
   - partition/bucket 路由逻辑

4. **BE 端 — FFI 后端预留**
   - **`FfiPaimonWriteBackend`**：stub 实现，所有方法返回 `Status::NotSupported("FFI backend not yet implemented")`
   - 后端选择逻辑：`PaimonWriteBackendFactory` 根据 `TPaimonTableSink.backend_type` 创建对应后端
   - v1 中 `backend_type` 固定为 JNI，但代码路径已支持切换

5. **Java 端 (be-java-extensions)**
   - `PaimonJniWriteBackend` — JNI 入口，接收调用并转发给 Paimon SDK
   - `BatchTableWrite` / `StreamTableWrite` 的 JNI wrapper
   - `CommitMessage` 的序列化（Paimon 原生格式），同时转换为结构化 Thrift 格式

6. **FE 端**
   - `PaimonTableSink` — 生成 Thrift sink 描述，v1 固定 `backend_type = JNI`
   - `PaimonWriteCommitCoordinator` / `PaimonTransaction` — 聚合 CommitMessages，协调提交
   - Nereids planner 集成（INSERT INTO paimon_table SELECT ...）

7. **测试**
   - 单元测试：`IPaimonWriteBackend` 接口的 mock 测试
   - 集成测试：Doris → Paimon → Doris 读写闭环
   - 性能测试：与直接使用 Paimon Flink 写入的对比

### Phase 2：v2 — Rust FFI Backend（约 4-6 周）

**目标**：引入 Rust 后端作为优先选择，加速覆盖大部分写入场景。

**关键**：由于 Phase 1 已经建立了 `IPaimonWriteBackend` 抽象，此阶段**上层代码（VPaimonTableWriter、FE Planner）无需修改**。仅需：

1. **Rust 端（核心工作）**
   - 扩展 `paimon-rust/bindings/c/`：新增写入相关的 C FFI 函数
     - `paimon_write_builder_new()` / `paimon_table_write_new()` / `paimon_write_arrow_batch()`
     - `paimon_prepare_commit()` / `paimon_table_commit_new()` / `paimon_commit()` / `paimon_commit_abort()`
   - Arrow C Data Interface 支持（ArrowArray/ArrowSchema FFI，零拷贝）

2. **BE 端（替换 stub）**
   - 将 `FfiPaimonWriteBackend` 从 stub 替换为真正的实现（<1000行新代码）
   - 动态加载 Rust 共享库（`dlopen`）
   - Doris Block → Arrow FFI 的零拷贝转换
   - Tokio runtime 集成（异步 I/O）

3. **功能路由（自动切换）**
   - `PaimonWriteBackendFactory::selectBackendType()` 实现自动判断：
     - 检查表 options 是否包含 Rust 不支持的功能
     - 是 → JNI Backend；否 → FFI Backend
   - FE 端 `TPaimonTableSink.backend_type` 从固定 JNI 变为**自动选择**

### Phase 3：高级写入功能（约 6-8 周）

**目标**：对齐 Paimon 的全部写入能力

1. **Compaction 支持**
   - 触发策略配置
   - 显式 `compact()` 调用
   - Compaction 结果的 CommitMessage 处理

2. **Changelog Producer 全支持**
   - Lookup Changelog Producer
   - FullCompaction Changelog Producer

3. **MERGE INTO 支持**
   - DataEvolution 表的 RowTracking 集成
   - Copy-on-Write 合并

4. **Schema Evolution**
   - 写入时的 Schema 兼容性检查
   - 自动 Schema Update

### Phase 4：优化与 Rust 长期演进（持续）

1. **性能优化**
   - BE 端数据批处理优化
   - 零拷贝数据路径
   - 自适应后端选择

2. **Rust 功能跟进**
   - 与 Paimon Rust 社区协作，补齐 Compaction 等功能
   - 逐步将更多功能从 JNI 迁移到 FFI

3. **稳定性**
   - 混沌测试
   - 大规模数据写入测试
   - 多版本兼容性测试


## 附录 A：关键文件路径

### Paimon Java SDK
| 文件 | 说明 |
|------|------|
| `paimon-core/.../table/sink/TableWrite.java` | 写入接口 |
| `paimon-core/.../table/sink/BatchTableWrite.java` | 批写入接口 |
| `paimon-core/.../table/sink/BatchWriteBuilder.java` | 写入构建器 |
| `paimon-core/.../table/sink/BatchTableCommit.java` | 批提交接口 |
| `paimon-core/.../table/sink/StreamTableWrite.java` | 流写入接口 |
| `paimon-core/.../table/sink/StreamWriteBuilder.java` | 流写入构建器 |
| `paimon-core/.../table/sink/CommitMessage.java` | 提交消息接口 |
| `paimon-core/.../table/sink/CommitMessageSerializer.java` | 提交消息序列化器 |

### Paimon Rust
| 文件 | 说明 |
|------|------|
| `crates/paimon/src/table/write_builder.rs` | WriteBuilder 实现 |
| `crates/paimon/src/table/table_write.rs` | TableWrite 实现（~3000行） |
| `crates/paimon/src/table/table_commit.rs` | TableCommit 实现（~4800行） |
| `crates/paimon/src/table/commit_message.rs` | CommitMessage 结构体 |
| `crates/paimon/src/table/data_file_writer.rs` | Append-only 文件写入器 |
| `crates/paimon/src/table/kv_file_writer.rs` | PK 表 KeyValue 写入器 |
| `crates/paimon/src/table/bucket_assigner*.rs` | 桶分配策略 |
| `bindings/c/src/table.rs` | C FFI 绑定（当前只有读取） |
| `bindings/c/Cargo.toml` | C 绑定 crate 配置 |

### Doris
| 文件 | 说明 |
|------|------|
| `be/src/exec/sink/writer/async_result_writer.h` | 异步写入器基类 |
| `be/src/exec/sink/writer/iceberg/viceberg_table_writer.{h,cpp}` | Iceberg 写入参考 |
| `be/src/format_v2/jni/paimon_jni_reader.{h,cpp}` | Paimon JNI 读取器（参考） |
| `be/src/format/jni/jni_data_bridge.{h,cpp}` | JNI 数据桥接 |
| `gensrc/thrift/DataSinks.thrift` | Thrift sink 定义 |
| `fe/fe-core/.../planner/IcebergTableSink.java` | Iceberg Sink FE 参考 |
| `fe/fe-core/.../datasource/paimon/` | Paimon 数据源相关 |

---

## 附录 B：设计模式与交互规则

### 关键抽象对应关系

```
Doris 概念          Paimon 概念             说明
─────────────────────────────────────────────────────
VPaimonTableWriter   BatchWriteBuilder      整个表的写入器
VPaimonPartitionWriter  BatchTableWrite     单个 (partition, bucket) 的写入器
Block (batch)        InternalRow / RecordBatch  数据批次
prepareCommit()      prepareCommit()        生成 CommitMessage
close() + RPC        TableCommit.commit()   提交
```

### CommitMessage 生命周期

```
创建：IPaimonWriter::prepareCommit()
  ↓
序列化（Thrift）：BE → FE RPC
  ↓
聚合：FE Coordinator
  ↓
去重（idempotency）：对比 commit_user + commit_identifier
  ↓
提交：Commit BE → IPaimonCommitter::commit()
  ↓
清理（success）：释放 writer
清理（failure）：IPaimonCommitter::abort() 删除数据文件
```

---

> **文档版本**：v4.0
> **作者**：Doris Paimon Write Team
> **日期**：2026-07-08
> **状态**：v1 实现已完成（feature/paimon-jni-write-v1），30+ 文件，~4000 行代码
