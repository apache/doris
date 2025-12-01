# Iceberg 元数据分层缓存架构实现方案

## 一、设计目标

### 1.1 核心目标

1. **分层职责明确**: 按照元数据的访问频率和重要程度分层，每层有明确的职责
2. **细粒度控制**: 支持更精细的缓存失效和更新策略
3. **性能提升**: 减少不必要的元数据加载，提高缓存命中率
4. **扩展性**: 易于添加新的缓存层或调整缓存策略
5. **向后兼容**: 保持对现有代码的兼容性

### 1.2 解决的问题

**当前架构的痛点**:
- ✗ Table 缓存粒度太粗，修改任何字段都要重新加载整个 Table
- ✗ Schema 缓存与 Table 缓存分离，需要两次查询
- ✗ 分区信息每次都要全量加载，对大分区表性能差
- ✗ 缓存失效策略粗暴，一次失效清空所有相关数据
- ✗ 缺乏增量更新机制

**分层架构的优势**:
- ✓ 按需加载，只加载需要的层级
- ✓ 细粒度失效，只失效变化的层级
- ✓ 支持增量更新，利用 Iceberg 的 transaction log
- ✓ 更好的性能监控和调优

## 二、分层架构设计

### 2.1 四层架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                    IcebergMetadataCache                      │
│                     (统一管理入口)                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ delegates to
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  TieredCacheCoordinator                      │
│                    (分层缓存协调器)                            │
│  - 协调各层缓存的加载和失效                                      │
│  - 处理层级之间的依赖关系                                        │
│  - 提供统一的查询接口                                           │
└─────────────────────────────────────────────────────────────┘
        │              │               │               │
        ↓              ↓               ↓               ↓
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   Level 1    │ │   Level 2    │ │   Level 3    │ │   Level 4    │
│ TableMetadata│ │   Snapshot   │ │    Schema    │ │  Partition   │
│    Cache     │ │    Cache     │ │    Cache     │ │    Cache     │
│              │ │              │ │              │ │              │
│  表基础信息   │ │  快照信息     │ │  Schema信息   │ │  分区信息     │
│  (location,  │ │  (snapshot   │ │  (columns,   │ │  (manifest   │
│   props)     │ │   metadata)  │ │   types)     │ │   entries)   │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
```

### 2.2 各层详细说明

#### Level 1: TableMetadata Cache（表元数据缓存）

**职责**: 缓存表的基础元数据信息

**缓存内容**:
```java
class TableMetadataCacheValue {
    String tableLocation;           // 表的存储位置
    String metadataLocation;        // 当前元数据文件位置
    Map<String, String> properties; // 表属性
    long currentSnapshotId;         // 当前快照 ID
    long lastModifiedTime;          // 最后修改时间
    int formatVersion;              // Iceberg 格式版本
}
```

**特点**:
- 最轻量级，访问频率最高
- 变化频率较低（除非有新的 snapshot）
- 作为其他层的入口

**失效条件**:
- 表被 DROP/ALTER
- 外部执行了 REFRESH TABLE

#### Level 2: Snapshot Cache（快照缓存）

**职责**: 缓存 Snapshot 的详细信息

**缓存内容**:
```java
class SnapshotCacheValue {
    long snapshotId;
    long schemaId;                  // 关联的 schema ID
    long parentSnapshotId;          // 父快照 ID
    long timestampMillis;           // 快照时间
    String manifestListLocation;    // Manifest list 位置
    Map<String, String> summary;    // 快照统计信息 (row count, file count)
    String operation;               // 操作类型 (append, overwrite)
}
```

**特点**:
- 中等大小
- 支持历史 snapshot 查询（time travel）
- 包含重要的统计信息

**失效条件**:
- 新的 commit 产生新快照
- Snapshot 过期清理

#### Level 3: Schema Cache（Schema 缓存）

**职责**: 缓存表的 Schema 信息（按 schemaId）

**缓存内容**:
```java
class SchemaCacheValue {
    long schemaId;
    List<Column> columns;           // 列定义
    List<Column> partitionColumns;  // 分区列
    Map<Integer, Column> idToColumn;// fieldId -> Column 映射
    int identifierFieldIds[];       // 主键字段 IDs
}
```

**特点**:
- 按 schemaId 缓存，支持 schema evolution
- 同一表的不同 schema 版本独立缓存
- 访问频率高

**失效条件**:
- 表执行了 ALTER TABLE (ADD/DROP/RENAME COLUMN)
- Schema evolution 产生新版本

#### Level 4: Partition Cache（分区缓存）

**职责**: 缓存分区级别的元数据（Manifest 级别）

**缓存内容**:
```java
class PartitionCacheValue {
    long snapshotId;
    // 分区级别的元数据
    Map<String, PartitionMetadata> partitions; // partition spec -> metadata
    
    static class PartitionMetadata {
        PartitionSpec partitionSpec;
        List<ManifestFile> manifestFiles;   // 该分区的 manifest 文件
        long recordCount;                   // 记录数
        long fileCount;                     // 文件数
        Map<String, String> lowerBounds;    // 分区下界
        Map<String, String> upperBounds;    // 分区上界
    }
}
```

**特点**:
- 最重量级，延迟加载
- 只在需要分区信息时加载（MTMV、分区裁剪）
- 支持增量更新（只加载新增分区）

**失效条件**:
- 数据写入（INSERT/DELETE/UPDATE）
- Snapshot 变化

### 2.3 层级依赖关系

```
查询流程:
1. 查询 Level 1 (TableMetadata) → 获取 currentSnapshotId
2. 使用 snapshotId 查询 Level 2 (Snapshot) → 获取 schemaId
3. 使用 schemaId 查询 Level 3 (Schema) → 获取 columns
4. (可选) 使用 snapshotId 查询 Level 4 (Partition) → 获取分区信息

失效流程:
- DROP TABLE: 失效所有层级
- ALTER TABLE (schema): 失效 Level 3, 4
- INSERT/UPDATE: 失效 Level 2, 4
- REFRESH TABLE: 失效 Level 1, 2, 4 (Level 3 保留，除非 schema 变化)
```

## 三、核心组件设计

### 3.1 TieredCacheCoordinator（分层缓存协调器）

**职责**: 统一管理四层缓存，协调加载和失效

```java
public class TieredCacheCoordinator {
    
    // 四层缓存
    private final LoadingCache<TableCacheKey, TableMetadataCacheValue> tableMetadataCache;
    private final LoadingCache<SnapshotCacheKey, SnapshotCacheValue> snapshotCache;
    private final LoadingCache<SchemaCacheKey, SchemaCacheValue> schemaCache;
    private final LoadingCache<PartitionCacheKey, PartitionCacheValue> partitionCache;
    
    // 线程池
    private final ExecutorService executorService;
    
    /**
     * 获取表的完整元数据（按需加载）
     */
    public TableMetadata getTableMetadata(ExternalTable table, MetadataLevel level) {
        // level 控制加载到哪一层
    }
    
    /**
     * 获取 Level 1: 表基础信息
     */
    public TableMetadataCacheValue getTableMetadata(TableCacheKey key) {
        return tableMetadataCache.get(key);
    }
    
    /**
     * 获取 Level 2: Snapshot 信息
     */
    public SnapshotCacheValue getSnapshot(long catalogId, String dbName, 
                                           String tableName, long snapshotId) {
        SnapshotCacheKey key = new SnapshotCacheKey(catalogId, dbName, tableName, snapshotId);
        return snapshotCache.get(key);
    }
    
    /**
     * 获取 Level 3: Schema 信息
     */
    public SchemaCacheValue getSchema(long catalogId, String dbName, 
                                       String tableName, long schemaId) {
        SchemaCacheKey key = new SchemaCacheKey(catalogId, dbName, tableName, schemaId);
        return schemaCache.get(key);
    }
    
    /**
     * 获取 Level 4: 分区信息
     */
    public PartitionCacheValue getPartition(long catalogId, String dbName, 
                                             String tableName, long snapshotId) {
        PartitionCacheKey key = new PartitionCacheKey(catalogId, dbName, tableName, snapshotId);
        return partitionCache.get(key);
    }
    
    /**
     * 智能失效：根据变化类型失效对应的层级
     */
    public void invalidate(ExternalTable table, InvalidationType type) {
        switch (type) {
            case DROP_TABLE:
                invalidateAllLevels(table);
                break;
            case ALTER_SCHEMA:
                invalidateLevel3And4(table);
                break;
            case DATA_CHANGE:
                invalidateLevel2And4(table);
                break;
            case METADATA_REFRESH:
                invalidateLevel1And2And4(table);
                break;
        }
    }
    
    /**
     * 增量更新：只更新变化的部分
     */
    public void incrementalUpdate(ExternalTable table, long newSnapshotId) {
        // 1. 更新 Level 1 的 currentSnapshotId
        // 2. 加载新 Snapshot 到 Level 2
        // 3. 如果 schemaId 变化，加载新 Schema 到 Level 3
        // 4. 增量加载新分区到 Level 4
    }
}
```

### 3.2 缓存键设计

```java
/**
 * Level 1 缓存键: 表级别
 */
@Data
public class TableCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
}

/**
 * Level 2 缓存键: Snapshot 级别
 */
@Data
public class SnapshotCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
    private final long snapshotId;  // 支持查询历史 snapshot
}

/**
 * Level 3 缓存键: Schema 版本级别
 */
@Data
public class SchemaCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
    private final long schemaId;
}

/**
 * Level 4 缓存键: Snapshot + 分区级别
 */
@Data
public class PartitionCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
    private final long snapshotId;
    
    // 可选：支持分区级别的缓存键
    // private final String partitionPath;  // 细粒度分区缓存
}
```

### 3.3 缓存值设计

```java
/**
 * Level 1 缓存值
 */
@Data
public class TableMetadataCacheValue {
    private final String tableLocation;
    private final String metadataLocation;
    private final Map<String, String> properties;
    private final long currentSnapshotId;
    private final long lastModifiedTime;
    private final int formatVersion;
    
    // 可选：缓存一些常用的派生信息
    private final TableFormat tableFormat;  // v1/v2
    private final boolean isPartitioned;
}

/**
 * Level 2 缓存值
 */
@Data
public class SnapshotCacheValue {
    private final long snapshotId;
    private final long schemaId;
    private final long parentSnapshotId;
    private final long timestampMillis;
    private final String manifestListLocation;
    private final Map<String, String> summary;
    private final String operation;
    
    // 统计信息（从 summary 解析）
    private final long totalRecords;
    private final long totalDataFiles;
    private final long totalDeleteFiles;
    private final long totalPositionDeletes;
    private final long totalEqualityDeletes;
}

/**
 * Level 3 缓存值
 */
@Data
public class SchemaCacheValue {
    private final long schemaId;
    private final List<Column> columns;
    private final List<Column> partitionColumns;
    private final Map<Integer, Column> fieldIdToColumn;
    private final int[] identifierFieldIds;
    
    // 辅助信息
    private final Map<String, Column> nameToColumn;
}

/**
 * Level 4 缓存值
 */
@Data
public class PartitionCacheValue {
    private final long snapshotId;
    private final Map<PartitionKey, PartitionMetadata> partitions;
    
    @Data
    public static class PartitionMetadata {
        private final PartitionSpec partitionSpec;
        private final List<ManifestFile> manifestFiles;
        private final long recordCount;
        private final long fileCount;
        private final long dataFileSizeBytes;
        private final Map<Integer, String> lowerBounds;  // fieldId -> lower bound
        private final Map<Integer, String> upperBounds;  // fieldId -> upper bound
    }
    
    @Data
    public static class PartitionKey {
        private final String partitionPath;  // 例如: "year=2023/month=11"
        
        // 或使用结构化表示
        // private final Map<String, String> values;  // field -> value
    }
}
```

### 3.4 失效类型枚举

```java
public enum InvalidationType {
    /**
     * 表被删除 - 失效所有层级
     */
    DROP_TABLE,
    
    /**
     * Schema 变更 - 失效 Level 3, 4
     */
    ALTER_SCHEMA,
    
    /**
     * 数据变更 (INSERT/UPDATE/DELETE) - 失效 Level 2, 4
     */
    DATA_CHANGE,
    
    /**
     * 元数据刷新 (REFRESH TABLE) - 失效 Level 1, 2, 4
     */
    METADATA_REFRESH,
    
    /**
     * 仅刷新分区 - 失效 Level 4
     */
    PARTITION_REFRESH,
    
    /**
     * Catalog 级别刷新 - 失效指定 catalog 的所有缓存
     */
    CATALOG_REFRESH,
    
    /**
     * Database 级别刷新 - 失效指定 database 的所有缓存
     */
    DATABASE_REFRESH
}
```

## 四、接口设计

### 4.1 查询接口

```java
public interface TieredCacheQueryService {
    
    /**
     * 获取表的基础元数据 (Level 1)
     */
    TableMetadataCacheValue getTableMetadata(ExternalTable table);
    
    /**
     * 获取当前 Snapshot (Level 2)
     */
    SnapshotCacheValue getCurrentSnapshot(ExternalTable table);
    
    /**
     * 获取指定 Snapshot (Level 2 - Time Travel)
     */
    SnapshotCacheValue getSnapshot(ExternalTable table, long snapshotId);
    
    /**
     * 获取当前 Schema (Level 3)
     */
    SchemaCacheValue getCurrentSchema(ExternalTable table);
    
    /**
     * 获取指定 Schema (Level 3 - Schema Evolution)
     */
    SchemaCacheValue getSchema(ExternalTable table, long schemaId);
    
    /**
     * 获取分区信息 (Level 4)
     */
    PartitionCacheValue getPartitions(ExternalTable table);
    
    /**
     * 获取指定 Snapshot 的分区信息 (Level 4 - Time Travel)
     */
    PartitionCacheValue getPartitions(ExternalTable table, long snapshotId);
    
    /**
     * 一次性获取所有信息（组合查询）
     */
    CompleteTableMetadata getCompleteMetadata(ExternalTable table, 
                                               boolean includePartitions);
}
```

### 4.2 失效接口

```java
public interface TieredCacheInvalidationService {
    
    /**
     * 智能失效：根据失效类型自动判断失效哪些层级
     */
    void invalidate(ExternalTable table, InvalidationType type);
    
    /**
     * 精确失效：失效指定的层级
     */
    void invalidateLevel(ExternalTable table, CacheLevel... levels);
    
    /**
     * 失效整个表的所有层级
     */
    void invalidateTable(ExternalTable table);
    
    /**
     * 失效整个数据库的所有缓存
     */
    void invalidateDatabase(long catalogId, String dbName);
    
    /**
     * 失效整个 Catalog 的所有缓存
     */
    void invalidateCatalog(long catalogId);
    
    /**
     * 增量更新：新的 Snapshot
     */
    void onNewSnapshot(ExternalTable table, long newSnapshotId);
    
    /**
     * 增量更新：Schema 变化
     */
    void onSchemaChange(ExternalTable table, long newSchemaId);
}
```

### 4.3 监控接口

```java
public interface TieredCacheMetricsService {
    
    /**
     * 获取各层缓存统计
     */
    CacheStats getCacheStats(CacheLevel level);
    
    /**
     * 获取表的缓存状态
     */
    TableCacheStatus getTableCacheStatus(ExternalTable table);
    
    /**
     * 获取所有缓存的综合统计
     */
    TieredCacheMetrics getAllMetrics();
}

@Data
public class TieredCacheMetrics {
    private Map<CacheLevel, CacheStats> levelStats;
    private long totalCachedTables;
    private long totalCachedSnapshots;
    private long totalCachedSchemas;
    private long totalCachedPartitions;
    private double overallHitRatio;
    private long totalMemoryUsageBytes;
}
```

## 五、实现步骤

### 5.1 Phase 1: 基础架构（2-3周）

**目标**: 搭建分层缓存的基础框架，不破坏现有功能

**任务清单**:
1. ✅ 创建核心类结构
   - `TieredCacheCoordinator`
   - 四个 CacheKey 类
   - 四个 CacheValue 类
   
2. ✅ 实现 Level 1 (TableMetadata) 缓存
   - 实现缓存加载逻辑
   - 实现失效逻辑
   - 编写单元测试

3. ✅ 实现 Level 2 (Snapshot) 缓存
   - 支持当前 snapshot
   - 支持历史 snapshot 查询
   - 编写单元测试

4. ✅ 配置项支持
   - 添加各层缓存的配置参数
   - 支持独立配置每层的过期时间和大小

**配置示例**:
```properties
# Level 1: Table Metadata
iceberg_table_metadata_cache_expire_seconds = 86400  # 24h
iceberg_table_metadata_cache_max_size = 10000

# Level 2: Snapshot
iceberg_snapshot_cache_expire_seconds = 7200  # 2h
iceberg_snapshot_cache_max_size = 50000

# Level 3: Schema
iceberg_schema_cache_expire_seconds = 43200  # 12h
iceberg_schema_cache_max_size = 5000

# Level 4: Partition
iceberg_partition_cache_expire_seconds = 3600  # 1h
iceberg_partition_cache_max_size = 10000
```

**验收标准**:
- 新架构可以正常加载和缓存 Level 1, 2 数据
- 所有单元测试通过
- 不影响现有功能

### 5.2 Phase 2: Schema 和 Partition 层（2-3周）

**目标**: 实现 Level 3 和 Level 4，完成四层架构

**任务清单**:
1. ✅ 实现 Level 3 (Schema) 缓存
   - 按 schemaId 缓存
   - 支持 schema evolution
   - 迁移现有 ExternalSchemaCache 逻辑

2. ✅ 实现 Level 4 (Partition) 缓存
   - 支持懒加载（按需加载）
   - 实现 Manifest 级别的缓存
   - 支持增量加载新分区

3. ✅ 实现层级间的依赖管理
   - Level 1 → Level 2 的自动级联
   - Level 2 → Level 3 的自动级联
   - Level 2 → Level 4 的按需加载

4. ✅ 集成到现有代码
   - 修改 `IcebergUtils` 使用新 API
   - 修改 `IcebergExternalTable` 使用新缓存
   - 修改 `IcebergScanNode` 使用新缓存

**验收标准**:
- 四层缓存都可以正常工作
- 层级间依赖正确处理
- 现有功能测试全部通过

### 5.3 Phase 3: 智能失效机制（1-2周）

**目标**: 实现细粒度的缓存失效和增量更新

**任务清单**:
1. ✅ 实现智能失效逻辑
   - 根据 InvalidationType 自动判断失效层级
   - 实现精确失效（避免过度失效）

2. ✅ 实现增量更新机制
   - `onNewSnapshot()`: 只更新必要的层级
   - `onSchemaChange()`: 只失效 schema 相关缓存
   - 利用 Iceberg 的 transaction log

3. ✅ 优化 REFRESH 命令
   - `REFRESH TABLE`: 智能判断变化类型
   - `REFRESH CATALOG`: 批量处理，避免全部清空

**示例代码**:
```java
public void refreshTable(ExternalTable table) {
    // 1. 检查远程元数据
    TableMetadataCacheValue oldMetadata = getTableMetadata(table);
    TableMetadataCacheValue newMetadata = loadFromRemote(table);
    
    // 2. 智能判断变化类型
    if (!oldMetadata.getMetadataLocation().equals(newMetadata.getMetadataLocation())) {
        // 元数据文件变化，可能有新 snapshot
        if (oldMetadata.getCurrentSnapshotId() != newMetadata.getCurrentSnapshotId()) {
            // 新 snapshot，增量更新
            onNewSnapshot(table, newMetadata.getCurrentSnapshotId());
        }
    }
    
    // 3. 检查 schema 是否变化
    SnapshotCacheValue newSnapshot = getSnapshot(table, newMetadata.getCurrentSnapshotId());
    if (needSchemaRefresh(table, newSnapshot.getSchemaId())) {
        onSchemaChange(table, newSnapshot.getSchemaId());
    }
}
```

**验收标准**:
- 失效精度提高，不会过度失效
- 增量更新逻辑正确
- REFRESH 性能明显提升

### 5.4 Phase 4: 监控和调优（1周）

**目标**: 完善监控指标，提供调优工具

**任务清单**:
1. ✅ 实现监控接口
   - 各层缓存的命中率统计
   - 内存使用统计
   - 加载耗时统计

2. ✅ 实现 SHOW PROC 扩展
   - 显示分层缓存的统计信息
   - 显示每个表的缓存状态

3. ✅ 性能调优
   - 根据监控数据调整各层的配置
   - 识别并优化热点表
   - 添加预热机制

4. ✅ 文档更新
   - 更新用户文档
   - 添加运维指南
   - 添加性能调优指南

**SHOW PROC 示例**:
```sql
SHOW PROC '/tiered_cache_stats/{catalog_name}/{db_name}/{table_name}';

-- 输出:
+-----------------+-----------+------------+-----------+-----------------+
| Cache Level     | Hit Ratio | Hit Count  | Miss Count| Avg Load Time   |
+-----------------+-----------+------------+-----------+-----------------+
| TableMetadata   | 95.2%     | 10500      | 500       | 12 ms           |
| Snapshot        | 88.5%     | 8850       | 1150      | 45 ms           |
| Schema          | 99.1%     | 9910       | 90        | 8 ms            |
| Partition       | 72.3%     | 7230       | 2770      | 230 ms          |
+-----------------+-----------+------------+-----------+-----------------+
```

**验收标准**:
- 监控指标完整准确
- SHOW PROC 能显示详细信息
- 提供明确的调优建议

### 5.5 Phase 5: 灰度上线和回滚方案（1周）

**目标**: 安全上线，提供回滚能力

**任务清单**:
1. ✅ 实现特性开关
   - 配置项控制是否启用分层缓存
   - 默认关闭，逐步灰度

2. ✅ 兼容模式
   - 分层缓存和旧缓存并行运行
   - 对比结果一致性

3. ✅ 回滚方案
   - 快速回退到旧架构
   - 数据迁移和清理

4. ✅ 压力测试
   - 高并发查询测试
   - 大量表的缓存测试
   - 内存压力测试

**配置示例**:
```properties
# 特性开关
enable_tiered_cache = false  # 默认关闭
tiered_cache_compatibility_mode = true  # 兼容模式，双写验证

# 灰度控制
tiered_cache_rollout_percentage = 0  # 0-100，控制灰度比例
```

**验收标准**:
- 可以无缝切换新旧架构
- 压力测试通过
- 有明确的回滚步骤

## 六、迁移方案

### 6.1 向后兼容策略

**保持现有接口不变**:
```java
// 现有代码仍然可以工作
Table table = IcebergUtils.getIcebergTable(dorisTable);

// 内部实现迁移到分层缓存
public static Table getIcebergTable(ExternalTable table) {
    if (Config.enableTieredCache) {
        // 使用新的分层缓存
        TieredCacheCoordinator coordinator = getTieredCacheCoordinator();
        TableMetadataCacheValue metadata = coordinator.getTableMetadata(table);
        SnapshotCacheValue snapshot = coordinator.getCurrentSnapshot(table);
        SchemaCacheValue schema = coordinator.getCurrentSchema(table);
        return buildIcebergTable(metadata, snapshot, schema);
    } else {
        // 使用旧的缓存逻辑
        return getIcebergTableLegacy(table);
    }
}
```

### 6.2 数据迁移

**迁移现有缓存数据到新架构**:
```java
public class CacheMigrationService {
    
    /**
     * 将旧缓存迁移到新架构
     */
    public void migrateCache() {
        IcebergMetadataCache oldCache = getOldCache();
        TieredCacheCoordinator newCache = getNewCache();
        
        // 遍历旧缓存的所有表
        oldCache.tableCache.asMap().forEach((key, table) -> {
            try {
                // 提取信息到各层
                TableMetadataCacheValue level1 = extractTableMetadata(table);
                SnapshotCacheValue level2 = extractSnapshot(table);
                SchemaCacheValue level3 = extractSchema(table);
                
                // 写入新缓存
                newCache.putTableMetadata(key, level1);
                newCache.putSnapshot(key, table.currentSnapshot().snapshotId(), level2);
                newCache.putSchema(key, table.schema().schemaId(), level3);
                
                // Level 4 (Partition) 延迟加载，不做迁移
            } catch (Exception e) {
                LOG.warn("Failed to migrate table cache: {}", key, e);
            }
        });
    }
}
```

### 6.3 渐进式上线

**分阶段启用新缓存**:

**Stage 1: 只读验证**（1周）
- 分层缓存只读，不写入
- 对比新旧缓存结果
- 记录不一致情况

**Stage 2: 双写模式**（1周）
- 同时写入新旧缓存
- 查询仍使用旧缓存
- 验证新缓存正确性

**Stage 3: 灰度切换**（2周）
- 5% 流量切换到新缓存
- 监控性能和正确性
- 逐步增加到 100%

**Stage 4: 清理旧缓存**（1周）
- 所有流量使用新缓存
- 移除旧缓存代码
- 更新文档

## 七、性能优化建议

### 7.1 缓存预热

```java
public class CacheWarmupService {
    
    /**
     * 预热热点表的缓存
     */
    public void warmupHotTables(List<ExternalTable> hotTables) {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        
        for (ExternalTable table : hotTables) {
            executor.submit(() -> {
                try {
                    // 预热 Level 1, 2, 3（常用层级）
                    coordinator.getTableMetadata(table);
                    coordinator.getCurrentSnapshot(table);
                    coordinator.getCurrentSchema(table);
                    
                    // Level 4 不预热，太重
                    LOG.info("Warmed up cache for table: {}", table.getName());
                } catch (Exception e) {
                    LOG.warn("Failed to warmup table: {}", table.getName(), e);
                }
            });
        }
        
        executor.shutdown();
    }
    
    /**
     * 识别热点表
     */
    public List<ExternalTable> identifyHotTables(int topN) {
        // 从审计日志或查询历史中识别访问频繁的表
        return auditLogService.getTopQueriedTables(topN, Duration.ofHours(24));
    }
}
```

### 7.2 自适应缓存大小

```java
public class AdaptiveCacheSizeManager {
    
    /**
     * 根据内存使用动态调整缓存大小
     */
    public void adjustCacheSize() {
        MemoryUsage heapUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long usedMemory = heapUsage.getUsed();
        long maxMemory = heapUsage.getMax();
        double usageRatio = (double) usedMemory / maxMemory;
        
        if (usageRatio > 0.8) {
            // 内存紧张，缩小缓存
            shrinkCache(0.8);  // 缩小到 80%
        } else if (usageRatio < 0.5) {
            // 内存充足，扩大缓存
            expandCache(1.2);  // 扩大到 120%
        }
    }
}
```

### 7.3 分区缓存优化

```java
public class PartitionCacheOptimizer {
    
    /**
     * 增量加载分区（只加载新增部分）
     */
    public PartitionCacheValue incrementalLoadPartitions(
            ExternalTable table, long oldSnapshotId, long newSnapshotId) {
        
        // 1. 获取旧的分区缓存
        PartitionCacheValue oldCache = getPartitionsIfPresent(table, oldSnapshotId);
        
        // 2. 只加载新增的 manifest files
        List<ManifestFile> newManifests = getIncrementalManifests(
                table, oldSnapshotId, newSnapshotId);
        
        // 3. 合并旧缓存和新数据
        if (oldCache != null && !newManifests.isEmpty()) {
            Map<PartitionKey, PartitionMetadata> mergedPartitions = 
                    new HashMap<>(oldCache.getPartitions());
            
            // 只解析新的 manifest files
            for (ManifestFile manifest : newManifests) {
                parseManifestAndUpdate(manifest, mergedPartitions);
            }
            
            return new PartitionCacheValue(newSnapshotId, mergedPartitions);
        } else {
            // 全量加载
            return loadPartitionsFull(table, newSnapshotId);
        }
    }
}
```

### 7.4 查询优化器集成

```java
public class IcebergQueryPlanner {
    
    /**
     * 利用缓存的统计信息进行查询优化
     */
    public PlanFragment optimizePlan(SelectStmt stmt, IcebergExternalTable table) {
        // 1. 从 Level 2 (Snapshot) 获取统计信息
        SnapshotCacheValue snapshot = coordinator.getCurrentSnapshot(table);
        long totalRecords = snapshot.getTotalRecords();
        long totalDataFiles = snapshot.getTotalDataFiles();
        
        // 2. 估算扫描代价
        long estimatedScanCost = estimateCost(totalRecords, totalDataFiles);
        
        // 3. 如果需要分区裁剪，才加载 Level 4
        if (needPartitionPruning(stmt)) {
            PartitionCacheValue partitions = coordinator.getPartitions(table);
            return buildPlanWithPartitionPruning(stmt, snapshot, partitions);
        } else {
            return buildPlanWithoutPartitions(stmt, snapshot);
        }
    }
}
```

## 八、测试方案

### 8.1 单元测试

```java
@Test
public void testTieredCacheBasicFlow() {
    // 1. 创建测试表
    ExternalTable table = createTestTable();
    
    // 2. 测试 Level 1 加载
    TableMetadataCacheValue metadata = coordinator.getTableMetadata(table);
    assertNotNull(metadata);
    assertEquals("hdfs://path/to/table", metadata.getTableLocation());
    
    // 3. 测试 Level 2 加载
    SnapshotCacheValue snapshot = coordinator.getCurrentSnapshot(table);
    assertNotNull(snapshot);
    assertTrue(snapshot.getSnapshotId() > 0);
    
    // 4. 测试 Level 3 加载
    SchemaCacheValue schema = coordinator.getSchema(table, snapshot.getSchemaId());
    assertNotNull(schema);
    assertFalse(schema.getColumns().isEmpty());
    
    // 5. 测试缓存命中
    TableMetadataCacheValue cached = coordinator.getTableMetadata(table);
    assertSame(metadata, cached);  // 应该是同一个对象
}

@Test
public void testSmartInvalidation() {
    ExternalTable table = createTestTable();
    
    // 1. 加载所有层级
    coordinator.getCompleteMetadata(table, true);
    
    // 2. 测试 DATA_CHANGE 失效
    coordinator.invalidate(table, InvalidationType.DATA_CHANGE);
    
    // Level 1 应该还在缓存中
    assertTrue(isInCache(table, CacheLevel.TABLE_METADATA));
    // Level 2, 4 应该被失效
    assertFalse(isInCache(table, CacheLevel.SNAPSHOT));
    assertFalse(isInCache(table, CacheLevel.PARTITION));
    // Level 3 应该还在缓存中
    assertTrue(isInCache(table, CacheLevel.SCHEMA));
}

@Test
public void testIncrementalUpdate() {
    ExternalTable table = createTestTable();
    long oldSnapshotId = 100L;
    long newSnapshotId = 101L;
    
    // 1. 加载旧 snapshot 的缓存
    coordinator.getSnapshot(table, oldSnapshotId);
    coordinator.getPartitions(table, oldSnapshotId);
    
    // 2. 增量更新到新 snapshot
    coordinator.onNewSnapshot(table, newSnapshotId);
    
    // 3. 验证缓存更新正确
    SnapshotCacheValue newSnapshot = coordinator.getCurrentSnapshot(table);
    assertEquals(newSnapshotId, newSnapshot.getSnapshotId());
    
    // 4. 验证分区增量加载
    PartitionCacheValue newPartitions = coordinator.getPartitions(table);
    assertEquals(newSnapshotId, newPartitions.getSnapshotId());
}
```

### 8.2 集成测试

```java
@Test
public void testQueryWithTieredCache() {
    // 1. 创建 Iceberg 表并插入数据
    String createSql = "CREATE TABLE iceberg_catalog.db.test_table " +
                       "(id INT, name STRING) USING iceberg";
    executeSQL(createSql);
    executeSQL("INSERT INTO iceberg_catalog.db.test_table VALUES (1, 'Alice'), (2, 'Bob')");
    
    // 2. 查询表（触发缓存加载）
    List<Row> rows = executeQuery("SELECT * FROM iceberg_catalog.db.test_table");
    assertEquals(2, rows.size());
    
    // 3. 验证缓存命中
    TieredCacheMetrics metrics = coordinator.getMetricsService().getAllMetrics();
    assertTrue(metrics.getOverallHitRatio() > 0);
    
    // 4. 再次查询（应该命中缓存）
    rows = executeQuery("SELECT * FROM iceberg_catalog.db.test_table");
    assertEquals(2, rows.size());
    
    // 5. 验证缓存命中率提升
    TieredCacheMetrics newMetrics = coordinator.getMetricsService().getAllMetrics();
    assertTrue(newMetrics.getOverallHitRatio() > metrics.getOverallHitRatio());
}

@Test
public void testRefreshTableWithTieredCache() {
    ExternalTable table = getExternalTable("iceberg_catalog.db.test_table");
    
    // 1. 查询表（加载缓存）
    executeQuery("SELECT * FROM iceberg_catalog.db.test_table");
    
    // 2. 外部插入新数据
    externalInsert(table, "(3, 'Charlie')");
    
    // 3. REFRESH 之前，应该查询到旧数据
    List<Row> oldRows = executeQuery("SELECT * FROM iceberg_catalog.db.test_table");
    assertEquals(2, oldRows.size());
    
    // 4. REFRESH 表
    executeSQL("REFRESH TABLE iceberg_catalog.db.test_table");
    
    // 5. REFRESH 之后，应该查询到新数据
    List<Row> newRows = executeQuery("SELECT * FROM iceberg_catalog.db.test_table");
    assertEquals(3, newRows.size());
}
```

### 8.3 性能测试

```java
@Test
public void testCachePerformance() {
    int numTables = 1000;
    int numQueries = 10000;
    
    // 1. 创建大量表
    List<ExternalTable> tables = createTestTables(numTables);
    
    // 2. 冷启动性能测试
    long coldStartTime = measureQueryTime(() -> {
        for (int i = 0; i < numQueries; i++) {
            ExternalTable table = tables.get(i % numTables);
            coordinator.getTableMetadata(table);
        }
    });
    
    // 3. 缓存命中性能测试
    long hotStartTime = measureQueryTime(() -> {
        for (int i = 0; i < numQueries; i++) {
            ExternalTable table = tables.get(i % numTables);
            coordinator.getTableMetadata(table);
        }
    });
    
    // 4. 验证性能提升
    assertTrue(hotStartTime < coldStartTime / 10);  // 至少提升10倍
    
    // 5. 验证缓存命中率
    TieredCacheMetrics metrics = coordinator.getMetricsService().getAllMetrics();
    assertTrue(metrics.getOverallHitRatio() > 0.9);  // 命中率超过90%
}

@Test
public void testConcurrentAccess() throws InterruptedException {
    ExternalTable table = createTestTable();
    int numThreads = 100;
    int queriesPerThread = 100;
    
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger errors = new AtomicInteger(0);
    
    // 并发查询缓存
    for (int i = 0; i < numThreads; i++) {
        executor.submit(() -> {
            try {
                for (int j = 0; j < queriesPerThread; j++) {
                    coordinator.getCompleteMetadata(table, false);
                }
            } catch (Exception e) {
                errors.incrementAndGet();
            } finally {
                latch.countDown();
            }
        });
    }
    
    latch.await();
    executor.shutdown();
    
    // 验证没有并发错误
    assertEquals(0, errors.get());
}
```

### 8.4 压力测试

```bash
#!/bin/bash
# 压力测试脚本

# 1. 大量表测试
for i in {1..10000}; do
  echo "CREATE TABLE iceberg_catalog.db.table_$i (id INT, name STRING) USING iceberg;"
done | mysql -h fe_host -P query_port -u root

# 2. 高并发查询测试
for i in {1..100}; do
  (
    for j in {1..1000}; do
      table_id=$((RANDOM % 10000 + 1))
      echo "SELECT * FROM iceberg_catalog.db.table_$table_id LIMIT 10;"
    done | mysql -h fe_host -P query_port -u root
  ) &
done
wait

# 3. 查看缓存统计
echo "SHOW PROC '/tiered_cache_stats';" | mysql -h fe_host -P query_port -u root
```

## 九、监控和运维

### 9.1 关键指标

| 指标 | 说明 | 告警阈值 |
|-----|------|---------|
| `cache_hit_ratio_level1` | Level 1 缓存命中率 | < 90% |
| `cache_hit_ratio_level2` | Level 2 缓存命中率 | < 80% |
| `cache_hit_ratio_level3` | Level 3 缓存命中率 | < 95% |
| `cache_hit_ratio_level4` | Level 4 缓存命中率 | < 70% |
| `avg_load_time_level1_ms` | Level 1 平均加载时间 | > 50ms |
| `avg_load_time_level2_ms` | Level 2 平均加载时间 | > 100ms |
| `avg_load_time_level3_ms` | Level 3 平均加载时间 | > 50ms |
| `avg_load_time_level4_ms` | Level 4 平均加载时间 | > 500ms |
| `cache_memory_usage_mb` | 缓存内存使用 | > 10GB |
| `cache_eviction_rate` | 缓存驱逐速率 | > 100/s |

### 9.2 运维命令

```sql
-- 查看整体缓存统计
SHOW PROC '/tiered_cache_stats';

-- 查看指定 catalog 的缓存
SHOW PROC '/tiered_cache_stats/iceberg_catalog';

-- 查看指定表的缓存
SHOW PROC '/tiered_cache_stats/iceberg_catalog/db/table';

-- 清空指定表的缓存
ADMIN SET FRONTEND CONFIG ("invalidate_table_cache" = "iceberg_catalog.db.table");

-- 清空指定 catalog 的缓存
ADMIN SET FRONTEND CONFIG ("invalidate_catalog_cache" = "iceberg_catalog");

-- 预热热点表
ADMIN SET FRONTEND CONFIG ("warmup_tables" = "iceberg_catalog.db.table1,iceberg_catalog.db.table2");

-- 调整缓存配置（动态生效）
ADMIN SET FRONTEND CONFIG ("iceberg_table_metadata_cache_max_size" = "20000");
```

### 9.3 问题排查

**问题 1: 缓存命中率低**
```
排查步骤:
1. SHOW PROC '/tiered_cache_stats' 查看各层命中率
2. 检查是否有频繁的 REFRESH 操作
3. 检查 eviction_count 是否过高（缓存容量不足）
4. 检查 avg_load_time 是否过长（网络问题）

解决方案:
- 增大缓存容量: iceberg_xxx_cache_max_size
- 增加缓存过期时间: iceberg_xxx_cache_expire_seconds
- 避免不必要的 REFRESH
- 启用缓存预热
```

**问题 2: 内存占用过高**
```
排查步骤:
1. SHOW PROC '/tiered_cache_stats' 查看各层缓存大小
2. 检查是否有大量不常用的表被缓存
3. 检查 Level 4 (Partition) 缓存是否过大

解决方案:
- 减小缓存容量，增加驱逐
- 缩短缓存过期时间
- 禁用 Level 4 缓存（如果不需要分区信息）
- 定期清理不活跃的表缓存
```

**问题 3: 查询返回旧数据**
```
排查步骤:
1. 检查是否执行了 REFRESH TABLE
2. 查看缓存的 snapshot ID 是否是最新的
3. 检查失效逻辑是否正确触发

解决方案:
- 手动 REFRESH TABLE
- 缩短 Level 1, 2 的过期时间（牺牲性能换一致性）
- 启用自动刷新机制
```

## 十、总结

### 10.1 架构优势

1. **性能提升**
   - 细粒度缓存，避免不必要的加载
   - 增量更新，减少全量刷新
   - 按需加载，Level 4 仅在需要时加载

2. **灵活性**
   - 独立配置各层的过期时间和容量
   - 支持 time travel 和 schema evolution
   - 易于扩展新的缓存层

3. **可维护性**
   - 清晰的层级结构
   - 明确的职责划分
   - 完善的监控和调试工具

### 10.2 预期收益

| 指标 | 当前 | 优化后 | 提升 |
|-----|------|-------|------|
| 缓存命中率 | 70% | 90%+ | +28% |
| 平均查询延迟 | 100ms | 50ms | -50% |
| REFRESH 耗时 | 5s | 1s | -80% |
| 内存使用效率 | 60% | 85% | +42% |

### 10.3 后续规划

1. **Phase 6: 分布式缓存**（可选）
   - 多 FE 之间的缓存同步
   - 基于 gossip 协议的失效传播

2. **Phase 7: 智能预取**（可选）
   - 基于查询历史预测需要的表
   - 提前加载热点表的缓存

3. **Phase 8: 持久化缓存**（可选）
   - 将缓存持久化到本地磁盘
   - FE 重启后快速恢复缓存

---

**文档版本**: v1.0  
**最后更新**: 2025-11-05  
**作者**: AI 架构分析  
**审核状态**: 待审核  


