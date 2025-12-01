# Iceberg 元数据缓存快速参考

## 核心类速查

### 1. 缓存管理入口
```java
// 获取 Iceberg 元数据缓存
Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache()

// 获取 Schema 缓存
Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(catalog)
```

### 2. 核心缓存接口

#### 获取 Table
```java
// IcebergMetadataCache
public Table getIcebergTable(ExternalTable dorisTable)

// 使用示例
Table icebergTable = Env.getCurrentEnv()
    .getExtMetaCacheMgr()
    .getIcebergMetadataCache()
    .getIcebergTable(dorisTable);
```

#### 获取 Schema
```java
// IcebergUtils
public static IcebergSchemaCacheValue getSchemaCacheValue(ExternalTable dorisTable, long schemaId)

// 使用示例
IcebergSchemaCacheValue schemaValue = IcebergUtils.getSchemaCacheValue(dorisTable, schemaId);
List<Column> schema = schemaValue.getSchema();
List<Column> partitionColumns = schemaValue.getPartitionColumns();
```

#### 获取 Snapshot（MTMV）
```java
// IcebergMetadataCache
public IcebergSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable)

// 使用示例
IcebergSnapshotCacheValue snapshotValue = 
    Env.getCurrentEnv().getExtMetaCacheMgr()
        .getIcebergMetadataCache()
        .getSnapshotCache(dorisTable);
```

### 3. 缓存失效接口

```java
// 失效表缓存
ExternalMetaCacheMgr.invalidateTableCache(ExternalTable dorisTable)

// 失效数据库缓存
ExternalMetaCacheMgr.invalidateDbCache(long catalogId, String dbName)

// 失效 Catalog 缓存
ExternalMetaCacheMgr.invalidateCatalogCache(long catalogId)
```

## 关键配置参数

| 参数名 | 默认值 | 说明 | 调优建议 |
|--------|--------|------|----------|
| `external_cache_expire_time_seconds_after_access` | 86400 (24h) | 缓存访问后多久过期 | 元数据稳定可调大，频繁变化可调小 |
| `external_cache_refresh_time_minutes` | 60 | 缓存自动刷新间隔（分钟） | 根据元数据更新频率调整 |
| `max_external_table_cache_num` | 50000 | 最大缓存条目数 | 表数量多可调大 |
| `max_external_cache_loader_thread_pool_size` | 10 | 缓存加载线程池大小 | 并发高可调大 |

## 常用调试命令

### 查看缓存统计
```sql
-- 查看所有元数据缓存统计
SHOW PROC '/catalog_meta_cache';

-- 查看指定 catalog 的缓存
SHOW PROC '/catalog_meta_cache/{catalog_name}';
```

输出字段：
- `hit_ratio`: 缓存命中率
- `hit_count`: 命中次数
- `read_count`: 总访问次数
- `eviction_count`: 驱逐次数
- `average_load_penalty`: 平均加载耗时
- `estimated_size`: 缓存条目数

### 刷新缓存
```sql
-- 刷新整个 catalog
REFRESH CATALOG catalog_name;

-- 刷新单个表
REFRESH TABLE catalog_name.db_name.table_name;
```

### 启用 DEBUG 日志
```properties
# fe.conf
sys_log_level = DEBUG
```

关键日志搜索：
```bash
# loadTable 调用
grep "load iceberg table" fe.log

# 缓存失效
grep "invalidate iceberg table cache" fe.log
```

## 核心数据结构

### IcebergMetadataCacheKey
```java
class IcebergMetadataCacheKey {
    NameMapping nameMapping;  // catalogId + dbName + tblName
}
```

### IcebergSchemaCacheKey
```java
class IcebergSchemaCacheKey extends SchemaCacheKey {
    NameMapping nameMapping;
    long schemaId;  // Iceberg schema 版本 ID
}
```

### IcebergSnapshotCacheValue
```java
class IcebergSnapshotCacheValue {
    IcebergPartitionInfo partitionInfo;  // 分区信息
    IcebergSnapshot snapshot;            // snapshotId + schemaId
}
```

### IcebergSchemaCacheValue
```java
class IcebergSchemaCacheValue extends SchemaCacheValue {
    List<Column> schema;              // 表 schema
    List<Column> partitionColumns;    // 分区列
}
```

## 典型代码模式

### 模式 1: 查询时获取 Table
```java
// IcebergScanNode 或 IcebergSource
public void initialize() {
    // 获取缓存的 Table 对象
    Table icebergTable = Env.getCurrentEnv()
        .getExtMetaCacheMgr()
        .getIcebergMetadataCache()
        .getIcebergTable(dorisTable);
    
    // 使用 Table 进行扫描规划
    Snapshot snapshot = icebergTable.currentSnapshot();
    PartitionSpec spec = icebergTable.spec();
    // ...
}
```

### 模式 2: 获取表的 Schema
```java
// IcebergExternalTable.getFullSchema()
public List<Column> getFullSchema() {
    // 1. 获取当前 snapshot 的 schemaId
    Optional<MvccSnapshot> snapshotFromContext = 
        MvccUtil.getSnapshotFromContext(this);
    IcebergSnapshotCacheValue cacheValue = 
        IcebergUtils.getOrFetchSnapshotCacheValue(snapshotFromContext, this);
    long schemaId = cacheValue.getSnapshot().getSchemaId();
    
    // 2. 从 Schema 缓存获取
    IcebergSchemaCacheValue schemaValue = 
        IcebergUtils.getSchemaCacheValue(this, schemaId);
    
    return schemaValue.getSchema();
}
```

### 模式 3: MTMV 获取分区信息
```java
// IcebergExternalTable (MTMVRelatedTableIf)
public Map<String, PartitionItem> getAndCopyPartitionItems(
        Optional<MvccSnapshot> snapshot) {
    // 从 snapshot 缓存获取分区信息
    IcebergSnapshotCacheValue cacheValue = 
        IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, this);
    
    return Maps.newHashMap(
        cacheValue.getPartitionInfo().getNameToPartitionItem());
}
```

### 模式 4: 手动失效缓存
```java
// DDL 操作后失效缓存
public void afterTableModified(ExternalTable table) {
    Env.getCurrentEnv()
        .getExtMetaCacheMgr()
        .invalidateTableCache(table);
}
```

## 性能优化建议

### 1. 缓存命中率优化

**问题**: 缓存命中率低
```
原因分析:
1. 过期时间太短
2. 缓存条目数限制太小
3. 频繁失效操作

优化方案:
1. 增大 external_cache_expire_time_seconds_after_access
2. 增大 max_external_table_cache_num
3. 避免全局刷新，使用精确刷新
```

### 2. 元数据加载慢

**问题**: 首次访问表很慢
```
原因分析:
1. 元数据存储网络延迟
2. 加载线程池太小
3. Iceberg 元数据文件太大

优化方案:
1. 提前预热缓存（在低峰期访问表）
2. 增大 max_external_cache_loader_thread_pool_size
3. 考虑使用本地元数据缓存（如果支持）
```

### 3. 分区信息加载慢

**问题**: 获取分区信息很慢（MTMV 场景）
```
原因分析:
1. 分区数量非常多
2. 每次扫描 Partitions 元数据表

优化方案:
1. 考虑不使用自动分区管理
2. 或重构为增量加载分区信息
3. 增加分区信息的专门缓存
```

### 4. 内存占用高

**问题**: FE 内存占用过高
```
原因分析:
1. 缓存的 Table 对象很大
2. 缓存条目数太多

优化方案:
1. 减小 max_external_table_cache_num
2. 减小 external_cache_expire_time_seconds_after_access
3. 考虑实现更细粒度的缓存（只缓存必要信息）
```

## 注意事项

### 1. 缓存一致性
- Doris 缓存是被动失效，不会自动感知外部变化
- 外部修改元数据后必须手动 REFRESH
- 多 FE 环境下，REFRESH 命令会广播到所有 FE

### 2. Iceberg 内部缓存
- `ManifestFiles.dropCache()` 清理 Iceberg SDK 的内部缓存
- 失效 Doris 缓存时必须同时清理，否则可能读到过期数据

### 3. Schema Evolution
- Iceberg 支持 schema evolution，不同 snapshot 可能有不同 schema
- Schema 缓存的 key 包含 schemaId，确保版本对应
- 查询时要使用正确的 schemaId 获取对应版本的 schema

### 4. 并发安全
- 所有缓存操作都是线程安全的（Caffeine LoadingCache）
- 缓存加载使用独立线程池，避免阻塞查询线程
- 缓存刷新是异步的，不影响当前查询

### 5. 鉴权处理
- 加载元数据时通过 ExecutionAuthenticator 处理鉴权
- 支持 Kerberos、IAM 等认证方式
- 鉴权信息缓存在 Catalog 层

## 常见问题排查

### Q1: 查询返回旧数据

**症状**: 外部修改表后，查询仍然返回旧数据

**排查**:
1. 检查是否执行了 REFRESH
2. 查看缓存统计，确认缓存已失效
3. 检查日志，看是否有失效相关日志

**解决**:
```sql
REFRESH TABLE catalog_name.db_name.table_name;
```

### Q2: 首次查询很慢

**症状**: 第一次查询表需要很长时间

**排查**:
1. 这是正常的，因为需要从远程加载元数据
2. 查看日志中的 "load iceberg table" 耗时
3. 检查网络到元数据存储的延迟

**解决**:
1. 预热缓存（提前访问常用表）
2. 优化网络配置
3. 考虑使用更快的元数据存储

### Q3: 内存占用持续增长

**症状**: FE 内存使用持续增长

**排查**:
1. 查看缓存统计的 estimated_size
2. 检查是否有大量不同的表被访问
3. 查看 eviction_count，看是否有驱逐

**解决**:
1. 减小 max_external_table_cache_num
2. 减小 external_cache_expire_time_seconds_after_access
3. 定期 REFRESH 清理不用的表

### Q4: 缓存命中率低

**症状**: hit_ratio 很低，频繁加载元数据

**排查**:
1. 查看 eviction_count 是否很高
2. 检查是否有频繁的 REFRESH 操作
3. 查看缓存过期配置

**解决**:
1. 增大缓存过期时间
2. 增大缓存条目数
3. 避免不必要的 REFRESH

## 相关代码文件

### 核心实现
- `IcebergMetadataCache.java` - 核心缓存实现
- `IcebergMetadataCacheMgr.java` - 缓存管理器
- `ExternalSchemaCache.java` - Schema 缓存
- `ExternalMetaCacheMgr.java` - 统一缓存管理

### 工具类
- `IcebergUtils.java` - 工具方法（大部分缓存访问通过这里）
- `IcebergMetadataOps.java` - 元数据操作（实际加载）

### 使用场景
- `IcebergExternalTable.java` - 表定义（Schema、分区）
- `IcebergScanNode.java` - 查询扫描
- `IcebergHMSSource.java` / `IcebergApiSource.java` - 数据源

### 数据结构
- `IcebergSnapshotCacheValue.java` - Snapshot 缓存值
- `IcebergSchemaCacheValue.java` - Schema 缓存值
- `IcebergSchemaCacheKey.java` - Schema 缓存键

---

**文档版本**: v1.0
**最后更新**: 2025-11-05

