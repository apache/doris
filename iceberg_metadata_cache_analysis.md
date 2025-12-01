# Doris FE Iceberg 元数据缓存架构与代码逻辑梳理

## 一、总体架构

### 1.1 核心组件层次结构

```
ExternalMetaCacheMgr (外部元数据缓存管理器 - 统一入口)
    ├── IcebergMetadataCacheMgr (Iceberg 元数据缓存管理器 - 包装层)
    │   └── IcebergMetadataCache (Iceberg 元数据缓存实现 - 核心缓存层)
    │       ├── tableCache (Table 缓存)
    │       ├── snapshotListCache (Snapshot 列表缓存)
    │       ├── snapshotCache (Snapshot 缓存值)
    │       └── viewCache (View 缓存)
    └── ExternalSchemaCache (Schema 缓存 - 独立的 Schema 缓存层)
        └── schemaCache (Schema 缓存)
```

### 1.2 缓存类型与职责

| 缓存名称 | 类型 | Key | Value | 职责 |
|---------|------|-----|-------|------|
| **tableCache** | LoadingCache | IcebergMetadataCacheKey | org.apache.iceberg.Table | 缓存原生 Iceberg Table 对象，是最核心的缓存 |
| **snapshotListCache** | LoadingCache | IcebergMetadataCacheKey | List&lt;Snapshot&gt; | 缓存表的所有 snapshot 列表 |
| **snapshotCache** | LoadingCache | IcebergMetadataCacheKey | IcebergSnapshotCacheValue | 缓存 snapshot 信息和分区信息（用于 MTMV） |
| **viewCache** | LoadingCache | IcebergMetadataCacheKey | org.apache.iceberg.view.View | 缓存 Iceberg View 对象 |
| **schemaCache** | LoadingCache | IcebergSchemaCacheKey | IcebergSchemaCacheValue | 缓存表的 Schema 和分区列信息 |

## 二、核心类详解

### 2.1 IcebergMetadataCache（核心缓存实现）

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergMetadataCache.java`

#### 2.1.1 缓存配置

所有缓存使用相同的配置策略：
```java
- expireAfterAccess: Config.external_cache_expire_time_seconds_after_access
- refreshAfterWrite: Config.external_cache_refresh_time_minutes * 60
- maximumSize: Config.max_external_table_cache_num
- executor: 共享的 commonRefreshExecutor
```

#### 2.1.2 核心方法

##### getIcebergTable()
```java
public Table getIcebergTable(ExternalTable dorisTable)
```
- **作用**: 获取 Iceberg Table 对象（最常用的接口）
- **流程**: 
  1. 根据 dorisTable 构建 IcebergMetadataCacheKey
  2. 从 tableCache 获取（如果不存在会触发 loadTable）
- **调用场景**: 几乎所有需要访问 Iceberg 元数据的地方

##### loadTable() （私有方法，缓存加载器）
```java
private Table loadTable(IcebergMetadataCacheKey key)
```
- **作用**: 从外部存储加载 Iceberg Table
- **流程**:
  1. 根据 catalogId 获取 Catalog 对象
  2. 判断 Catalog 类型（HMSExternalCatalog 或 IcebergExternalCatalog）
  3. 获取对应的 IcebergMetadataOps
  4. 调用 ops.loadTable() 从远程加载（支持鉴权）
  
##### getSnapshotCache()
```java
public IcebergSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable)
```
- **作用**: 获取 snapshot 和分区信息（主要用于 MTMV）
- **流程**: 通过 snapshotCache 获取，触发 loadSnapshot

##### loadSnapshot() （私有方法）
```java
private IcebergSnapshotCacheValue loadSnapshot(IcebergMetadataCacheKey key)
```
- **作用**: 加载最新的 snapshot 和分区信息
- **流程**:
  1. 获取 dorisTable
  2. 调用 IcebergUtils.getLastedIcebergSnapshot() 获取最新 snapshot
  3. 如果表有效，调用 IcebergUtils.loadPartitionInfo() 加载分区信息
  4. 返回 IcebergSnapshotCacheValue（包含 snapshot 和 partition 信息）

#### 2.1.3 缓存失效方法

##### invalidateCatalogCache()
```java
public void invalidateCatalogCache(long catalogId)
```
- 失效指定 catalog 下的所有缓存
- 对 tableCache 额外调用 `ManifestFiles.dropCache(entry.getValue().io())` 清理 Iceberg 内部缓存

##### invalidateTableCache()
```java
public void invalidateTableCache(ExternalTable dorisTable)
```
- 失效指定表的所有缓存
- 根据 catalogId + dbName + tblName 过滤

##### invalidateDbCache()
```java
public void invalidateDbCache(long catalogId, String dbName)
```
- 失效指定数据库下的所有表缓存

### 2.2 IcebergMetadataCacheKey（缓存键）

**位置**: `IcebergMetadataCache.java` 内部类

```java
static class IcebergMetadataCacheKey {
    NameMapping nameMapping;  // 包含 catalogId, localDbName, localTblName, remoteDbName, remoteTblName
}
```

- **核心字段**: NameMapping（表的命名映射）
- **equals/hashCode**: 基于 nameMapping

### 2.3 IcebergMetadataCacheMgr（管理器包装层）

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergMetadataCacheMgr.java`

- **作用**: 简单的包装层，提供统一的管理接口
- **持有**: 一个 IcebergMetadataCache 实例
- **方法**: 代理到 IcebergMetadataCache 的失效方法

### 2.4 ExternalSchemaCache（Schema 缓存）

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalSchemaCache.java`

#### 2.4.1 特点

- **独立于 IcebergMetadataCache**: 不在 IcebergMetadataCache 中
- **统一的 Schema 缓存**: 所有外部表类型共享（Hive、Iceberg、Paimon 等）
- **Key 类型**: SchemaCacheKey（基类），Iceberg 使用 IcebergSchemaCacheKey 扩展

#### 2.4.2 IcebergSchemaCacheKey

```java
public class IcebergSchemaCacheKey extends SchemaCacheKey {
    private final long schemaId;  // Iceberg schema 的版本 ID
}
```

- **扩展字段**: schemaId（Iceberg 表的 schema 版本）
- **原因**: Iceberg 支持 schema evolution，不同 snapshot 可能有不同 schema

#### 2.4.3 IcebergSchemaCacheValue

```java
public class IcebergSchemaCacheValue extends SchemaCacheValue {
    private final List<Column> partitionColumns;  // 分区列
}
```

- **继承**: SchemaCacheValue（包含 schema）
- **扩展字段**: partitionColumns（Iceberg 特有的分区列信息）

### 2.5 IcebergSnapshotCacheValue（Snapshot 缓存值）

**位置**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergSnapshotCacheValue.java`

```java
public class IcebergSnapshotCacheValue {
    private final IcebergPartitionInfo partitionInfo;  // 分区信息
    private final IcebergSnapshot snapshot;            // snapshot 信息
}
```

- **用途**: 主要用于 MTMV（物化视图）的 snapshot 跟踪和分区管理
- **包含**: snapshot ID、schema ID 和分区信息

## 三、数据加载流程

### 3.1 Table 加载流程

```
用户查询
  ↓
IcebergUtils.getIcebergTable(dorisTable)
  ↓
Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache()
  ↓
IcebergMetadataCache.getIcebergTable(dorisTable)
  ↓
tableCache.get(key)
  ↓
[缓存未命中] loadTable(key)
  ↓
获取 Catalog 和 IcebergMetadataOps
  ↓
ops.loadTable(dbName, tblName)  [带鉴权]
  ↓
从远程元数据存储加载（HMS/REST/Glue/DLF...）
  ↓
返回 org.apache.iceberg.Table
```

### 3.2 Schema 加载流程

```
获取表 Schema
  ↓
IcebergUtils.loadSchemaCacheValue(dorisTable, schemaId, isView)
  ↓
ExternalSchemaCache.getSchemaValue(IcebergSchemaCacheKey)
  ↓
schemaCache.get(key)
  ↓
[缓存未命中] loadSchema(key)
  ↓
catalog.getSchema(key)
  ↓
IcebergExternalTable.initSchema(key)
  ↓
IcebergUtils.loadSchemaCacheValue()
  ↓
获取 Table/View → 解析 Schema → 生成 IcebergSchemaCacheValue
```

### 3.3 Snapshot 加载流程（MTMV 场景）

```
MTMV 刷新/查询
  ↓
IcebergExternalTable.getAndCopyPartitionItems()
  ↓
IcebergUtils.getOrFetchSnapshotCacheValue()
  ↓
IcebergMetadataCache.getSnapshotCache()
  ↓
snapshotCache.get(key)
  ↓
[缓存未命中] loadSnapshot(key)
  ↓
IcebergUtils.getLastedIcebergSnapshot()  [获取当前 snapshot]
  ↓
IcebergUtils.loadPartitionInfo()  [加载分区信息]
  ↓
生成 IcebergSnapshotCacheValue
```

## 四、缓存使用场景

### 4.1 查询执行路径

#### HMS 类型的 Iceberg 表
```
IcebergScanNode
  ↓
IcebergHMSSource 初始化
  ↓
Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache()
    .getIcebergTable(hmsTable)
  ↓
使用 icebergTable 进行 scan planning
```

#### 原生 Iceberg Catalog
```
IcebergScanNode
  ↓
IcebergApiSource 初始化
  ↓
Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache()
    .getIcebergTable(icebergExtTable)
  ↓
使用 icebergTable 进行 scan planning
```

### 4.2 统计信息收集

```
IcebergUtils.getIcebergRowCount(tbl)
  ↓
获取 icebergTable
  ↓
读取 snapshot.summary() 中的统计信息
  ↓
计算行数: total-records - total-position-deletes
```

### 4.3 Schema 获取

```
IcebergExternalTable.getFullSchema()
  ↓
IcebergUtils.getIcebergSchema(dorisTable)
  ↓
获取 snapshotCacheValue (包含 schemaId)
  ↓
IcebergUtils.getSchemaCacheValue(dorisTable, schemaId)
  ↓
返回 List<Column>
```

### 4.4 MTMV 分区信息

```
MTMV 刷新
  ↓
IcebergExternalTable.getAndCopyPartitionItems()
  ↓
IcebergUtils.getOrFetchSnapshotCacheValue()
  ↓
getSnapshotCache() 获取缓存
  ↓
返回 partitionInfo.getNameToPartitionItem()
```

## 五、缓存失效机制

### 5.1 触发失效的场景

1. **用户手动刷新**: `REFRESH CATALOG` / `REFRESH TABLE`
2. **DDL 操作**: 创建/删除/修改表
3. **Catalog 删除**: 删除整个 catalog
4. **定时刷新**: 基于配置的自动刷新（refreshAfterWrite）

### 5.2 失效调用链

```
ExternalMetaCacheMgr.invalidateTableCache(dorisTable)
  ↓
icebergMetadataCacheMgr.invalidateTableCache(dorisTable)
  ↓
IcebergMetadataCache.invalidateTableCache(dorisTable)
  ↓
+ invalidate snapshotListCache
+ invalidate tableCache (+ ManifestFiles.dropCache)
+ invalidate snapshotCache
+ invalidate viewCache
  
同时：
ExternalSchemaCache.invalidateTableCache(dorisTable)
  ↓
invalidate schemaCache
```

### 5.3 特殊处理：ManifestFiles.dropCache

```java
ManifestFiles.dropCache(entry.getValue().io())
```

- **位置**: invalidateTableCache 和 invalidateCatalogCache 中
- **原因**: Iceberg SDK 内部也有 manifest 文件缓存，需要一并清理
- **重要性**: 避免读取到过期的 manifest 数据

## 六、关键配置参数

### 6.1 缓存配置

| 配置项 | 默认值 | 说明 |
|-------|--------|------|
| `external_cache_expire_time_seconds_after_access` | 86400 (24h) | 缓存访问后过期时间 |
| `external_cache_refresh_time_minutes` | 60 | 缓存刷新间隔（分钟）|
| `max_external_table_cache_num` | 50000 | 最大缓存条目数 |
| `max_external_cache_loader_thread_pool_size` | 10 | 缓存加载线程池大小 |

### 6.2 线程池

```java
commonRefreshExecutor: 
  - 用于所有缓存的异步刷新
  - 固定线程池，大小由 max_external_cache_loader_thread_pool_size 控制
  - 队列大小: pool_size * 10000
```

## 七、依赖关系

### 7.1 核心依赖

```
IcebergMetadataCache
  ├── 依赖 IcebergMetadataOps (加载 Table/View)
  ├── 依赖 IcebergUtils (工具方法)
  ├── 依赖 CatalogMgr (获取 Catalog)
  └── 依赖 ExecutionAuthenticator (鉴权)

IcebergMetadataOps
  ├── 依赖 org.apache.iceberg.catalog.Catalog (Iceberg SDK)
  └── 依赖 ExternalCatalog (Doris Catalog)

ExternalSchemaCache
  ├── 依赖 ExternalCatalog.getSchema() (Schema 加载接口)
  └── 被所有外部表类型共享
```

### 7.2 与 Iceberg SDK 的集成

```
Doris 缓存层
  ↓
IcebergMetadataOps.loadTable()
  ↓
org.apache.iceberg.catalog.Catalog.loadTable()
  ↓
[Iceberg SDK 内部]
  ↓
读取元数据存储 (HMS/REST/Glue/DLF...)
  ↓
返回 org.apache.iceberg.Table
```

## 八、潜在的优化点

### 8.1 当前架构的问题

1. **缓存粒度不够精细**
   - 整个 Table 对象被缓存，即使只需要部分信息
   - Snapshot 列表缓存很少被使用

2. **Schema 缓存与 Table 缓存分离**
   - 需要两次缓存查询才能获取完整信息
   - 增加了复杂度和失效管理难度

3. **分区信息加载低效**
   - 每次都扫描 Partitions 元数据表
   - 对于大表分区数很多时性能差

4. **Snapshot 缓存仅用于 MTMV**
   - 使用场景单一
   - 可以考虑合并到 Table 缓存中

5. **缓存失效策略粗暴**
   - Catalog/DB 级别失效会清空所有相关缓存
   - 可以考虑增量更新

### 8.2 重构建议方向

1. **分层缓存架构**
   ```
   Level 1: Metadata Cache (Table Location, Properties)
   Level 2: Snapshot Cache (Current/Historical Snapshots)
   Level 3: Schema Cache (按 SchemaId 缓存)
   Level 4: Partition Cache (Manifest 级别的分区缓存)
   ```

2. **增量更新机制**
   - 利用 Iceberg 的 transaction log
   - 只更新变化的部分，而不是全部失效

3. **按需加载策略**
   - 区分元数据级别（轻量级）和数据级别（重量级）
   - 支持部分字段的懒加载

4. **统一 Schema 和 Table 缓存**
   - 考虑将 Schema 信息直接放入 Table 缓存值中
   - 减少缓存查询次数

5. **分区信息缓存优化**
   - 考虑缓存 Manifest 级别的信息
   - 支持分区裁剪后再加载详细信息

6. **监控和指标增强**
   - 添加更详细的缓存命中率统计
   - 区分不同类型缓存的性能指标

## 九、调试与监控

### 9.1 查看缓存统计

```sql
SHOW PROC '/catalog_meta_cache/{catalog_name}';
```

返回信息包括：
- iceberg_snapshot_list_cache: hit_ratio, hit_count, read_count, eviction_count
- iceberg_table_cache: 同上
- iceberg_snapshot_cache: 同上

### 9.2 日志调试

关键日志点：
```java
// loadTable 被触发
LOG.debug("load iceberg table {}", nameMapping, new Exception());

// 缓存失效
LOG.info("invalidate iceberg table cache {}", nameMapping, new Exception());
```

启用 DEBUG 日志：
```
fe.conf: sys_log_level = DEBUG
```

### 9.3 缓存刷新命令

```sql
-- 刷新整个 catalog
REFRESH CATALOG catalog_name;

-- 刷新指定表
REFRESH TABLE catalog_name.db_name.table_name;
```

## 十、总结

### 10.1 核心设计特点

1. **多级缓存**: Table、Snapshot、Schema、View 分层缓存
2. **统一管理**: ExternalMetaCacheMgr 作为统一入口
3. **懒加载**: 基于 Caffeine LoadingCache 的按需加载
4. **自动刷新**: 支持定时自动刷新过期缓存
5. **鉴权集成**: 通过 ExecutionAuthenticator 支持 Kerberos 等认证

### 10.2 适用场景

- ✅ 读多写少的查询场景
- ✅ 元数据相对稳定的表
- ✅ 需要高并发查询的场景
- ⚠️ 元数据频繁变化的场景（需要频繁刷新）
- ⚠️ 大量分区的表（分区信息加载慢）

### 10.3 关键代码文件清单

```
核心缓存实现:
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergMetadataCache.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergMetadataCacheMgr.java

缓存值类型:
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergSnapshotCacheValue.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergSchemaCacheValue.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergSchemaCacheKey.java

Schema 缓存:
- fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalSchemaCache.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/SchemaCacheValue.java

统一管理:
- fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalMetaCacheMgr.java

元数据操作:
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergMetadataOps.java

工具类:
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergUtils.java

使用场景:
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergExternalTable.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergHMSSource.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergApiSource.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergScanNode.java
```

---

**文档生成时间**: 2025-11-05
**适用版本**: Doris 主分支
**作者**: AI 代码分析

