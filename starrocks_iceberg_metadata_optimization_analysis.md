# StarRocks Iceberg 元数据优化分析

基于 [StarRocks Issue #43460](https://github.com/StarRocks/starrocks/issues/43460) 的优化分析

## 一、优化概览

StarRocks 对 Iceberg 元数据进行了四个重要的优化，显著提升了查询性能和元数据访问效率：

1. **Iceberg 元数据分布式计划** - 性能提升 n ~ 2n 倍（n 为 BE 节点数）
2. **Iceberg Manifest 缓存** - 作业规划延迟降低到约 100ms
3. **基于元数据文件的外部表分布式计划框架** - 统一的分布式执行框架
4. **重构 Iceberg 元数据部分** - 优化缓存架构和代码结构

## 二、详细优化分析

### 2.1 Iceberg 元数据分布式计划优化

#### 优化目标
将原本在 FE 单点执行的元数据读取任务，分布到多个 BE 节点并行执行，充分利用集群资源。

#### 技术实现

**核心组件：**
- `DistributePlanner` - 分布式计划生成器
- `BackendDistributedPlanWorkerManager` - BE 节点管理器
- `LoadBalanceScanWorkerSelector` - 负载均衡的 Worker 选择器
- `UnassignedJobBuilder` - 任务构建器

**实现原理：**

```java
// 1. 创建 BE Worker 管理器
BackendDistributedPlanWorkerManager workerManager = 
    new BackendDistributedPlanWorkerManager(context, notNeedBackend, isLoadJob);

// 2. 负载均衡选择 Worker
LoadBalanceScanWorkerSelector workerSelector = 
    new LoadBalanceScanWorkerSelector(workerManager);

// 3. 构建未分配的任务
FragmentIdMapping<UnassignedJob> fragmentJobs = 
    UnassignedJobBuilder.buildJobs(workerSelector, statementContext, idToFragments);

// 4. 将任务分配到具体的 BE 实例
ListMultimap<PlanFragmentId, AssignedJob> instanceJobs = 
    AssignedJobBuilder.buildJobs(fragmentJobs, workerManager, isLoadJob);
```

**性能提升机制：**
- **并行读取 Manifest 文件**：多个 BE 节点同时读取不同的 Manifest 文件，而不是在 FE 串行读取
- **负载均衡**：根据 BE 节点的 CPU、IO、内存负载智能分配任务
- **减少网络传输**：BE 节点直接访问对象存储（S3/HDFS），避免 FE 作为中间层

**性能数据：**
- 性能提升：**n ~ 2n 倍**（n 为 BE 节点数）
- 例如：7 个 BE 节点，性能提升约 7-14 倍

### 2.2 Iceberg Manifest 缓存优化

#### 优化目标
减少 Manifest 文件过滤和评估的重复计算，将作业规划延迟从秒级降低到毫秒级。

#### 技术实现

**核心代码位置：** `IcebergUtils.getMatchingManifest()`

```java
// 使用 Caffeine 缓存 ManifestEvaluator
LoadingCache<Integer, ManifestEvaluator> evalCache = Caffeine.newBuilder()
    .build(
        specId -> {
            PartitionSpec spec = specsById.get(specId);
            return ManifestEvaluator.forPartitionFilter(
                Expressions.and(
                    Expressions.alwaysTrue(),
                    Projections.inclusive(spec, true).project(dataFilter)),
                spec,
                true);
        });

// 使用缓存的 Evaluator 过滤 Manifest
CloseableIterable<ManifestFile> matchingManifests = CloseableIterable.filter(
    CloseableIterable.withNoopClose(dataManifests),
    manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));
```

**缓存策略：**
- **缓存键**：`PartitionSpecId` - 每个分区规范对应一个 Evaluator
- **缓存值**：`ManifestEvaluator` - 用于评估 Manifest 文件是否匹配查询条件
- **缓存效果**：相同 `specId` 的 Manifest 文件共享同一个 Evaluator，避免重复创建

**性能提升：**
- **规划延迟**：从秒级降低到 **约 100ms**
- **适用场景**：大分区表、多分区规范（分区演进场景）

**缓存失效：**
在 `IcebergMetadataCache` 中，当表元数据失效时，会调用 `ManifestFiles.dropCache()` 清理缓存：
```java
ManifestFiles.dropCache(entry.getValue().io());
```

### 2.3 基于元数据文件的外部表分布式计划框架

#### 优化目标
为所有基于元数据文件的外部表（Iceberg、Delta Lake、Hudi 等）提供统一的分布式计划框架。

#### 框架设计

**核心组件：**
- `DistributedPlanWorker` - Worker 抽象接口
- `BackendWorker` - BE 节点 Worker 实现
- `UnassignedScanMetadataJob` - 未分配的扫描元数据任务
- `AssignedJob` - 已分配的任务

**框架优势：**
1. **统一接口**：不同格式的外部表使用相同的分布式计划接口
2. **可扩展性**：易于添加新的外部表格式支持
3. **代码复用**：避免为每种格式重复实现分布式逻辑

**工作流程：**
```
查询请求
  ↓
构建未分配任务 (UnassignedJob)
  ↓
选择 Worker (LoadBalanceScanWorkerSelector)
  ↓
分配任务到 BE (AssignedJob)
  ↓
构建分布式计划 (DistributedPlan)
  ↓
执行计划
```

### 2.4 Iceberg 元数据重构

#### 优化目标
优化缓存架构，提高代码可维护性和性能。

#### 当前缓存架构

**四层缓存结构：**

1. **Table 缓存** (`tableCache`)
   - 缓存 Iceberg `Table` 对象
   - 最核心的缓存，包含表的完整元数据

2. **Snapshot 列表缓存** (`snapshotListCache`)
   - 缓存表的所有 Snapshot 列表
   - 用于时间旅行查询

3. **Snapshot 缓存** (`snapshotCache`)
   - 缓存 Snapshot 信息和分区信息
   - 用于 MTMV（物化视图）

4. **View 缓存** (`viewCache`)
   - 缓存 Iceberg View 对象

**缓存配置：**
```java
- expireAfterAccess: Config.external_cache_expire_time_seconds_after_access
- refreshAfterWrite: Config.external_cache_refresh_time_minutes * 60
- maximumSize: Config.max_external_table_cache_num
- executor: 共享的 commonRefreshExecutor
```

**重构方向（未来优化）：**

根据文档 `iceberg_tiered_cache_implementation.md`，计划实现分层缓存架构：

1. **Level 1: TableMetadata 缓存**
   - 缓存表的基本元数据（currentSnapshotId 等）
   - 最轻量级，访问最频繁

2. **Level 2: Snapshot 缓存**
   - 缓存 Snapshot 信息
   - 中等重量级

3. **Level 3: Schema 缓存**
   - 缓存 Schema 信息
   - 已部分实现（`ExternalSchemaCache`）

4. **Level 4: Partition 缓存**
   - 缓存分区级别的元数据（Manifest 级别）
   - 最重量级，延迟加载

**优势：**
- 按需加载，只加载需要的层级
- 细粒度失效，只失效变化的层级
- 支持增量更新，利用 Iceberg 的 transaction log

## 三、未来优化方向

根据 Issue 描述，还有以下优化计划：

### 3.1 自动分布式策略增强
- 基于 BE 负载（CPU/IO/内存）的智能任务分配
- 动态调整任务分配策略

### 3.2 时间旅行语句增强
- 增强 `AS OF` 语句支持
- 优化历史版本查询性能

### 3.3 分片结构重构
- 重构 `RemoteFileInfo` 结构
- 优化文件分片信息存储

### 3.4 性能基准测试
- 提供默认最优分布式计划阈值
- 通过大量测试确定最佳参数

### 3.5 分区演进适配
- 适配分区数据解析器
- 支持分区演进场景

### 3.6 缓存监控
- 添加缓存指标（命中率等）
- 提供性能监控和调优工具

## 四、Doris 中的相关实现

### 4.1 当前实现状态

在 Doris 代码库中，可以看到类似的优化思路：

1. **Manifest Evaluator 缓存**：
   - 位置：`IcebergUtils.getMatchingManifest()`
   - 使用 Caffeine 缓存 `ManifestEvaluator`

2. **元数据缓存架构**：
   - `IcebergMetadataCache` - 核心缓存实现
   - `ExternalSchemaCache` - Schema 缓存
   - 支持 Table、Snapshot、View 等多种缓存

3. **分布式计划框架**：
   - `DistributePlanner` - 分布式计划生成
   - `BackendDistributedPlanWorkerManager` - BE Worker 管理

### 4.2 可借鉴的优化点

1. **分布式 Manifest 读取**：
   - 将 Manifest 文件读取任务分布到多个 BE 节点
   - 实现并行读取，提升性能

2. **更细粒度的缓存**：
   - 实现分层缓存架构
   - 支持增量更新

3. **负载感知的任务分配**：
   - 基于 BE 节点负载智能分配任务
   - 动态调整分配策略

## 五、性能影响总结

| 优化项 | 性能提升 | 适用场景 |
|--------|---------|---------|
| 元数据分布式计划 | n ~ 2n 倍 | 大表、多 BE 节点 |
| Manifest 缓存 | 规划延迟降至 100ms | 大分区表、多分区规范 |
| 分布式计划框架 | 统一优化 | 所有外部表格式 |
| 元数据重构 | 提升可维护性 | 长期优化 |

## 六、参考资料

- [StarRocks Issue #43460](https://github.com/StarRocks/starrocks/issues/43460)
- [StarRocks PR #43459](https://github.com/StarRocks/starrocks/pull/43459) - 第一个版本补丁
- [StarRocks PR #45479](https://github.com/StarRocks/starrocks/pull/45479) - 主版本补丁
- [StarRocks PR #45640](https://github.com/StarRocks/starrocks/pull/45640) - Backport 版本

