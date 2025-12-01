# Doris Iceberg 表查询规划阶段元数据读取流程与 IO 分析

## 一、概述

本文档详细分析 Doris 中 Iceberg 表查询在**规划阶段（FE）**读取元数据的完整流程，以及产生的 IO 操作。

## 二、规划阶段元数据读取流程

### 2.1 整体流程概览

```
SQL 查询
  ↓
IcebergScanNode 构造
  ↓
doInitialize() - 初始化阶段
  ├── 获取 Iceberg Table 对象（可能触发缓存加载）
  ├── 获取分区信息
  └── 初始化存储属性
  ↓
getSplits() / startSplit() - 获取文件分片
  ├── createTableScan() - 创建 TableScan
  │   ├── planWith() - 规划扫描（可能触发 manifest 读取）
  │   └── 设置 snapshot 和 filter
  └── planFileScanTask() - 规划文件扫描任务
      └── scan.planFiles() - 读取 manifest 文件列表
          └── 遍历所有 manifest 文件（产生大量 IO）
```

### 2.2 详细流程分析

#### 阶段 1: IcebergScanNode 构造

**代码位置**: `IcebergScanNode.java:130-153`

```java
public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv) {
    // 根据表类型创建不同的 Source
    if (table instanceof HMSExternalTable) {
        source = new IcebergHMSSource((HMSExternalTable) table, desc);
    } else if (table instanceof IcebergExternalTable) {
        source = new IcebergApiSource((IcebergExternalTable) table, desc, columnNameToRange);
    }
}
```

**IO 操作**: 
- ❌ **无直接 IO**，仅创建 Source 对象

#### 阶段 2: doInitialize() - 初始化

**代码位置**: `IcebergScanNode.java:156-171`

```java
@Override
protected void doInitialize() throws UserException {
    // 1. 获取 Iceberg Table 对象（可能触发缓存加载）
    icebergTable = source.getIcebergTable();
    
    // 2. 获取分区信息
    isPartitionedTable = icebergTable.spec().isPartitioned();
    
    // 3. 获取格式版本
    formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
    
    // 4. 初始化存储属性
    storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(...);
}
```

**IO 操作分析**:

##### 2.2.1 获取 Iceberg Table 对象

**调用链**:
```
source.getIcebergTable()
  ↓
IcebergApiSource.getIcebergTable() / IcebergHMSSource.getIcebergTable()
  ↓
IcebergMetadataCache.getIcebergTable(dorisTable)
  ↓
tableCache.get(key)  [如果缓存未命中，触发 loadTable]
  ↓
IcebergMetadataOps.loadTable(dbName, tblName)
  ↓
Iceberg Catalog.loadTable(identifier)
```

**IO 操作**:

1. **从元数据存储读取表元数据**（缓存未命中时）
   - **HMS Catalog**: 从 Hive Metastore 读取表信息（1 次 RPC）
   - **REST Catalog**: 从 REST API 读取表信息（1 次 HTTP 请求）
   - **Glue Catalog**: 从 AWS Glue 读取表信息（1 次 API 调用）
   - **DLF Catalog**: 从阿里云 DLF 读取表信息（1 次 API 调用）

2. **读取 Iceberg 元数据文件**（缓存未命中时）
   - 读取 `metadata.json` 文件（表的最新元数据）
   - 位置: `{table_location}/metadata/metadata.json`
   - **IO 类型**: 远程文件系统读取（S3/HDFS/OSS）
   - **IO 大小**: 通常几 KB 到几十 KB

3. **读取 Snapshot 元数据**（如果 metadata.json 中引用了 snapshot）
   - 读取 `{table_location}/metadata/snap-{snapshot-id}.avro`
   - **IO 类型**: 远程文件系统读取
   - **IO 大小**: 通常几 KB 到几百 KB

**IO 特点**:
- ✅ **有缓存机制**: `tableCache` 使用 Caffeine LoadingCache
- ✅ **缓存命中时无 IO**: 如果表元数据已缓存，此阶段无 IO
- ⚠️ **缓存未命中时**: 需要 1-2 次远程文件读取

##### 2.2.2 获取分区信息和格式版本

**IO 操作**:
- ❌ **无额外 IO**: 这些信息已包含在 Table 对象中（从缓存或刚加载的元数据）

#### 阶段 3: createTableScan() - 创建表扫描

**代码位置**: `IcebergScanNode.java:325-358`

```java
@VisibleForTesting
public TableScan createTableScan() throws UserException {
    if (icebergTableScan != null) {
        return icebergTableScan;  // 已创建，直接返回
    }

    TableScan scan = icebergTable.newScan();

    // 设置 snapshot
    IcebergTableQueryInfo info = getSpecifiedSnapshot();
    if (info != null) {
        if (info.getRef() != null) {
            scan = scan.useRef(info.getRef());
        } else {
            scan = scan.useSnapshot(info.getSnapshotId());
        }
    }

    // 设置 filter
    List<Expression> expressions = new ArrayList<>();
    for (Expr conjunct : conjuncts) {
        Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema());
        if (expression != null) {
            expressions.add(expression);
        }
    }
    for (Expression predicate : expressions) {
        scan = scan.filter(predicate);
    }

    // ⚠️ 关键：planWith() 可能触发 manifest 读取
    icebergTableScan = scan.planWith(source.getCatalog().getThreadPoolWithPreAuth());

    return icebergTableScan;
}
```

**IO 操作分析**:

##### 2.3.1 planWith() 方法

**IO 操作**:
- ⚠️ **可能触发 manifest 列表读取**（取决于 Iceberg 实现）
- 如果 snapshot 信息未完全加载，可能需要读取 snapshot 元数据文件
- **IO 类型**: 远程文件系统读取
- **IO 大小**: 通常几 KB 到几十 KB

**注意**: `planWith()` 是 Iceberg API 的方法，具体实现取决于 Iceberg 版本和配置。在某些情况下，它可能只是准备扫描计划，而不立即读取 manifest。

#### 阶段 4: planFileScanTask() - 规划文件扫描任务

**代码位置**: `IcebergScanNode.java:360-363` 和 `doGetSplits()`

```java
private CloseableIterable<FileScanTask> planFileScanTask(TableScan scan) {
    long targetSplitSize = getRealFileSplitSize(0);
    return TableScanUtil.splitFiles(scan.planFiles(), targetSplitSize);
}

private List<Split> doGetSplits(int numBackends) throws UserException {
    TableScan scan = createTableScan();
    
    try (CloseableIterable<FileScanTask> fileScanTasks = planFileScanTask(scan)) {
        // 遍历所有 FileScanTask，创建 Split
        fileScanTasks.forEach(taskGrp -> splits.add(createIcebergSplit(taskGrp)));
    }
}
```

**IO 操作分析**:

##### 2.4.1 scan.planFiles() - 核心 IO 操作

这是**规划阶段产生最多 IO 的操作**。

**调用链**:
```
scan.planFiles()
  ↓
Iceberg DataTableScan.doPlanFiles()
  ↓
ManifestGroup.planFiles()
  ↓
ManifestGroup.plan()
  ↓
读取所有 manifest 文件
```

**IO 操作详情**:

1. **读取 Manifest List 文件**
   - 从 snapshot 元数据中获取 manifest list 路径
   - 读取 `{table_location}/metadata/snap-{snapshot-id}-m{manifest-list-id}.avro`
   - **IO 类型**: 远程文件系统读取
   - **IO 大小**: 通常几 KB 到几百 KB
   - **IO 次数**: 1 次（每个 snapshot 一个 manifest list）

2. **读取所有 Manifest 文件**（串行或并行）
   - 从 manifest list 中获取所有 manifest 文件路径
   - 对于每个 manifest 文件：
     - 读取 `{table_location}/metadata/{manifest-file-id}.avro`
     - **IO 类型**: 远程文件系统读取
     - **IO 大小**: 通常几十 KB 到几 MB（取决于表的数据文件数量）
   - **IO 次数**: **N 次**（N = manifest 文件数量）
   - **IO 模式**: 
     - ⚠️ **当前实现**: **串行读取**（在 FE 单线程执行）
     - ⚠️ **性能瓶颈**: 对于大表，可能有数百甚至数千个 manifest 文件

3. **Manifest 文件内容解析**
   - 每个 manifest 文件包含多个数据文件的元数据
   - 解析 manifest 文件内容，提取数据文件路径、分区信息、统计信息等
   - **IO 类型**: 内存操作（已读取的文件内容）
   - **IO 大小**: 无额外 IO

**IO 特点**:
- ❌ **无缓存机制**: manifest 文件读取**没有缓存**
- ⚠️ **串行执行**: 所有 manifest 文件在 FE 单线程中串行读取
- ⚠️ **阻塞操作**: 必须等待所有 manifest 文件读取完成才能继续
- ⚠️ **网络延迟敏感**: 每个 manifest 文件读取都有网络往返时间（RTT）

##### 2.4.2 分区过滤优化

**代码位置**: `IcebergScanNode.java:470-486` (isBatchMode 方法)

```java
try (CloseableIterator<ManifestFile> matchingManifest =
        IcebergUtils.getMatchingManifest(
                createTableScan().snapshot().dataManifests(icebergTable.io()),
                icebergTable.specs(),
                createTableScan().filter()).iterator()) {
    // 遍历匹配的 manifest 文件
    while (matchingManifest.hasNext()) {
        ManifestFile next = matchingManifest.next();
        // ...
    }
}
```

**IO 操作**:
- ⚠️ **仍然需要读取所有 manifest 文件**: `getMatchingManifest()` 需要先获取所有 manifest 文件列表
- ✅ **有优化**: 使用 `ManifestEvaluator` 缓存（`IcebergUtils.getMatchingManifest()`）
- ⚠️ **但 manifest 文件内容仍需读取**: 即使过滤了 manifest，仍需要读取 manifest 文件内容来获取数据文件信息

#### 阶段 5: createIcebergSplit() - 创建分片

**代码位置**: `IcebergScanNode.java:365-407`

```java
private Split createIcebergSplit(FileScanTask fileScanTask) {
    // 从 FileScanTask 中提取信息创建 Split
    // 无额外 IO 操作
}
```

**IO 操作**:
- ❌ **无 IO**: 仅从已读取的 FileScanTask 中提取信息

## 三、IO 操作总结

### 3.1 IO 操作清单

| 阶段 | 操作 | IO 类型 | IO 次数 | IO 大小 | 是否缓存 | 执行位置 |
|------|------|---------|---------|---------|---------|---------|
| **初始化** | 加载 Table 元数据 | 远程文件读取 | 0-1 | 几 KB - 几十 KB | ✅ 有缓存 | FE |
| **初始化** | 读取 metadata.json | 远程文件读取 | 0-1 | 几 KB - 几十 KB | ✅ 有缓存 | FE |
| **初始化** | 读取 snapshot 元数据 | 远程文件读取 | 0-1 | 几 KB - 几百 KB | ✅ 有缓存 | FE |
| **规划扫描** | planWith() | 远程文件读取 | 0-1 | 几 KB - 几十 KB | ❌ 无缓存 | FE |
| **规划文件** | 读取 manifest list | 远程文件读取 | 1 | 几 KB - 几百 KB | ❌ 无缓存 | FE |
| **规划文件** | 读取 manifest 文件 | 远程文件读取 | **N** | 几十 KB - 几 MB | ❌ 无缓存 | FE |
| **规划文件** | 解析 manifest 内容 | 内存操作 | N | 无额外 IO | - | FE |

**说明**:
- **N**: manifest 文件数量，可能从几个到数千个
- **缓存命中时**: Table 相关 IO 为 0
- **缓存未命中时**: 需要 1-2 次额外 IO

### 3.2 IO 性能瓶颈分析

#### 瓶颈 1: Manifest 文件串行读取

**问题**:
- 所有 manifest 文件在 FE 单线程中**串行读取**
- 每个 manifest 文件读取都有网络 RTT（通常 10-100ms）
- 对于大表（数百个 manifest 文件），总耗时可能达到**数秒甚至数十秒**

**示例计算**:
- 假设有 100 个 manifest 文件
- 每个文件读取耗时 50ms（包括网络 RTT + 文件读取）
- 总耗时 = 100 × 50ms = **5 秒**

#### 瓶颈 2: 无 Manifest 缓存

**问题**:
- Manifest 文件读取**没有缓存机制**
- 每次查询都需要重新读取所有 manifest 文件
- 对于频繁查询的表，重复读取相同的 manifest 文件

#### 瓶颈 3: FE 单点执行

**问题**:
- 所有元数据读取都在 FE 执行
- FE 需要与对象存储（S3/HDFS/OSS）建立连接
- FE 的网络带宽和 IO 能力成为瓶颈

### 3.3 IO 模式分析

#### 当前实现（串行模式）

```
FE 线程
  ↓
读取 manifest list (1 次 IO)
  ↓
读取 manifest 1 (1 次 IO, 等待完成)
  ↓
读取 manifest 2 (1 次 IO, 等待完成)
  ↓
读取 manifest 3 (1 次 IO, 等待完成)
  ↓
...
  ↓
读取 manifest N (1 次 IO, 等待完成)
  ↓
总耗时 = N × (RTT + 文件读取时间)
```

#### 理想实现（并行模式，参考 StarRocks）

```
FE 线程
  ↓
读取 manifest list (1 次 IO)
  ↓
分发 manifest 读取任务到多个 BE
  ↓
BE 1: 读取 manifest 1, 2, 3... (并行)
BE 2: 读取 manifest 4, 5, 6... (并行)
BE 3: 读取 manifest 7, 8, 9... (并行)
...
  ↓
总耗时 ≈ (RTT + 文件读取时间) × (N / BE数量)
```

## 四、优化建议

### 4.1 短期优化（相对容易实现）

1. **添加 Manifest 文件缓存**
   - 在 FE 缓存已读取的 manifest 文件内容
   - 使用 manifest 文件路径 + snapshot ID 作为缓存键
   - 缓存失效策略：当 snapshot 变化时失效

2. **并行读取 Manifest 文件**
   - 在 FE 使用线程池并行读取多个 manifest 文件
   - 限制并发数，避免过多连接

3. **延迟加载 Manifest 内容**
   - 先读取 manifest list，获取文件列表
   - 只在需要时（创建 Split 时）才读取 manifest 内容

### 4.2 长期优化（参考 StarRocks）

1. **分布式 Manifest 读取**
   - 将 manifest 文件读取任务分发到多个 BE 节点
   - BE 节点直接访问对象存储，减少 FE 负担
   - 性能提升：n ~ 2n 倍（n 为 BE 节点数）

2. **Manifest 元数据缓存**
   - 在 BE 节点缓存 manifest 文件内容
   - 支持增量更新（只读取新增的 manifest）

3. **智能预取**
   - 根据查询模式预测需要读取的 manifest
   - 提前并行读取

## 五、代码关键位置

### 5.1 元数据读取入口

- `IcebergScanNode.doInitialize()` - 初始化阶段
- `IcebergScanNode.createTableScan()` - 创建扫描
- `IcebergScanNode.doGetSplits()` - 获取分片

### 5.2 缓存相关

- `IcebergMetadataCache.getIcebergTable()` - Table 缓存
- `IcebergMetadataCache.loadTable()` - Table 加载
- `IcebergUtils.getMatchingManifest()` - Manifest 过滤（有 Evaluator 缓存）

### 5.3 IO 操作触发点

- `TableScan.planWith()` - 可能触发 manifest 读取
- `TableScan.planFiles()` - **主要 IO 操作点**
- `ManifestGroup.plan()` - Iceberg 内部 manifest 读取

## 六、性能监控建议

### 6.1 关键指标

1. **规划阶段耗时**
   - `doInitialize()` 耗时
   - `createTableScan()` 耗时
   - `planFiles()` 耗时

2. **IO 指标**
   - Manifest 文件读取次数
   - Manifest 文件读取总大小
   - Manifest 文件读取平均耗时

3. **缓存命中率**
   - Table 缓存命中率
   - Manifest 缓存命中率（如果实现）

### 6.2 日志增强

在关键 IO 操作点添加日志：
- Manifest 文件读取开始/结束
- Manifest 文件读取耗时
- Manifest 文件数量统计

## 七、总结

### 7.1 当前状态

- ✅ **Table 元数据有缓存**: 减少重复读取
- ❌ **Manifest 文件无缓存**: 每次查询都重新读取
- ❌ **串行读取**: 所有 manifest 文件串行读取，性能瓶颈明显
- ❌ **FE 单点执行**: 所有 IO 在 FE 执行，无法利用 BE 集群资源

### 7.2 优化潜力

- **短期优化**: 添加 manifest 缓存 + 并行读取，预计提升 **2-5 倍**
- **长期优化**: 分布式 manifest 读取，预计提升 **n ~ 2n 倍**（n 为 BE 节点数）

### 7.3 参考实现

- **StarRocks Issue #43460**: Iceberg 元数据分布式计划优化
- **StarRocks PR #45479**: 主版本实现
- **StarRocks PR #45640**: Backport 版本

