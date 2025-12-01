# 统一数据湖元数据分层缓存架构设计

## 一、背景与目标

### 1.1 当前架构问题

**各数据湖格式的现状**：

```
ExternalMetaCacheMgr (简单聚合，缺乏统一抽象)
    ├── IcebergMetadataCache (Iceberg 独立实现)
    │   ├── tableCache, snapshotCache, viewCache
    │   └── snapshotListCache
    ├── HudiMetadataCacheMgr (Hudi 独立实现)
    │   ├── HudiPartitionProcessor
    │   ├── HudiCachedFsViewProcessor
    │   └── HudiCachedMetaClientProcessor
    ├── PaimonMetadataCache (Paimon 独立实现)
    │   └── snapshotCache
    ├── MaxComputeMetadataCache (MaxCompute 独立实现)
    │   └── partitionValuesCache
    └── ExternalSchemaCache (统一的 Schema 缓存)
```

**存在的问题**：
1. ❌ **缺乏统一抽象**: 每种格式各自实现，没有通用接口
2. ❌ **粒度不一致**: Iceberg 细粒度，MaxCompute 粗粒度
3. ❌ **难以扩展**: 新增数据湖格式需要重复造轮子
4. ❌ **配置分散**: 每种格式独立配置，难以统一管理
5. ❌ **监控困难**: 无法统一监控所有格式的缓存状态
6. ❌ **失效策略不统一**: 各自实现失效逻辑

### 1.2 设计目标

**核心目标**：
1. ✅ **统一抽象**: 提供通用的元数据缓存接口，适配所有数据湖格式
2. ✅ **分层设计**: 按照元数据层级和访问频率分层
3. ✅ **灵活扩展**: 新格式只需实现适配器，复用通用框架
4. ✅ **统一管理**: 统一的配置、监控、失效策略
5. ✅ **向后兼容**: 不破坏现有功能，渐进式迁移
6. ✅ **性能优化**: 细粒度缓存，智能失效，增量更新

## 二、数据湖格式的共性与差异

### 2.1 核心元数据的共性

所有数据湖格式都具备以下元数据层级：

| 元数据层级 | 说明 | Iceberg | Hudi | Paimon | Delta Lake | MaxCompute |
|-----------|------|---------|------|--------|-----------|-----------|
| **表基础信息** | 表位置、属性 | ✓ | ✓ | ✓ | ✓ | ✓ |
| **版本/快照** | 表的版本历史 | Snapshot | Commit | Snapshot | Version | - |
| **Schema** | 列定义、类型 | Schema | Schema | Schema | Schema | Schema |
| **分区信息** | 分区元数据 | Manifest | FileGroup | Manifest | AddFile | Partition |
| **统计信息** | 行数、文件数 | Summary | Statistics | Statistics | Statistics | - |

### 2.2 格式特性差异

| 特性 | Iceberg | Hudi | Paimon | Delta Lake | MaxCompute |
|-----|---------|------|--------|-----------|-----------|
| Time Travel | ✓ | ✓ | ✓ | ✓ | - |
| Schema Evolution | ✓ | ✓ | ✓ | ✓ | 有限 |
| 分区进化 | ✓ (PartitionSpec) | - | ✓ | - | - |
| ACID 事务 | ✓ | ✓ | ✓ | ✓ | ✓ |
| Upsert 支持 | ✓ (v2) | ✓ (核心) | ✓ | ✓ | - |
| 特殊缓存需求 | Manifest, View | Timeline, FSView | - | - | - |

### 2.3 统一抽象的可行性

**结论**: 可以抽象出**四层通用元数据缓存**，同时为每种格式提供**扩展点**。

```
Level 1: TableMetadata (表基础信息) - 所有格式通用
Level 2: Version/Snapshot (版本信息) - 所有格式通用（语义相同）
Level 3: Schema (Schema 信息) - 所有格式通用
Level 4: Partition/Files (分区文件) - 所有格式通用（实现不同）
```

## 三、统一分层缓存架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                  UnifiedMetaCacheFacade                          │
│                   (统一缓存门面)                                   │
│  - 对外提供统一的查询接口                                           │
│  - 隐藏内部实现细节                                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              TieredMetaCacheOrchestrator                         │
│               (分层缓存协调器 - 核心)                              │
│  - 管理四层通用缓存                                                │
│  - 协调缓存加载和失效                                              │
│  - 处理层级依赖关系                                                │
│  - 统一监控和指标                                                  │
└─────────────────────────────────────────────────────────────────┘
        │               │                │               │
        ↓               ↓                ↓               ↓
  ┌──────────┐   ┌──────────┐    ┌──────────┐    ┌──────────┐
  │ Level 1  │   │ Level 2  │    │ Level 3  │    │ Level 4  │
  │TableMeta │   │ Version/ │    │  Schema  │    │Partition/│
  │  Cache   │   │ Snapshot │    │  Cache   │    │  Files   │
  │          │   │  Cache   │    │          │    │  Cache   │
  └──────────┘   └──────────┘    └──────────┘    └──────────┘
        ↑               ↑                ↑               ↑
        └───────────────┴────────────────┴───────────────┘
                              │
                   通过适配器加载具体格式的数据
                              │
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                FormatAdapter (格式适配器层)                       │
│  定义统一的加载接口，各格式实现适配器                               │
└─────────────────────────────────────────────────────────────────┘
        │           │            │            │            │
        ↓           ↓            ↓            ↓            ↓
  ┌──────────┐ ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌──────────┐
  │ Iceberg  │ │  Hudi   │ │ Paimon  │ │DeltaLake │ │MaxCompute│
  │ Adapter  │ │ Adapter │ │ Adapter │ │ Adapter  │ │ Adapter  │
  └──────────┘ └─────────┘ └─────────┘ └──────────┘ └──────────┘
        │           │            │            │            │
        ↓           ↓            ↓            ↓            ↓
  ┌──────────┐ ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌──────────┐
  │ Iceberg  │ │  Hudi   │ │ Paimon  │ │DeltaLake │ │MaxCompute│
  │   SDK    │ │   SDK   │ │   SDK   │ │   SDK    │ │   SDK    │
  └──────────┘ └─────────┘ └─────────┘ └──────────┘ └──────────┘
```

### 3.2 四层缓存详细设计

#### Level 1: TableMetadata Cache（表基础元数据）

**职责**: 缓存表的基础信息，最轻量级，访问频率最高

**通用数据结构**:
```java
public class UnifiedTableMetadata {
    // 通用字段（所有格式必须）
    private final String tableLocation;              // 表存储路径
    private final String metadataLocation;           // 元数据文件位置
    private final Map<String, String> properties;    // 表属性
    private final long currentVersionId;             // 当前版本 ID
    private final long lastModifiedTime;             // 最后修改时间
    private final TableFormat format;                // 格式类型: ICEBERG/HUDI/PAIMON/...
    
    // 格式特定扩展（可选）
    private final Map<String, Object> formatSpecificMetadata;
    
    // 常用派生信息（缓存避免重复计算）
    private final boolean isPartitioned;
    private final int partitionColumnCount;
}

public enum TableFormat {
    ICEBERG, HUDI, PAIMON, DELTA_LAKE, MAX_COMPUTE
}
```

**缓存键**:
```java
public class TableCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
    // 可扩展：支持多租户、namespace 等
}
```

**失效条件**:
- 表被 DROP/RENAME
- 表属性修改
- 外部执行 REFRESH TABLE

---

#### Level 2: Version/Snapshot Cache（版本快照缓存）

**职责**: 缓存表的版本/快照信息，支持 time travel

**通用数据结构**:
```java
public class UnifiedVersionMetadata {
    // 通用字段
    private final long versionId;                    // 版本 ID (Iceberg: snapshotId, Hudi: commitTime)
    private final long schemaId;                     // 关联的 schema ID
    private final long parentVersionId;              // 父版本 ID (支持版本链)
    private final long timestampMillis;              // 版本时间戳
    private final String manifestLocation;           // 文件列表位置
    private final OperationType operation;           // 操作类型
    
    // 统计信息（从各格式的 summary 提取）
    private final long totalRecords;                 // 总记录数
    private final long totalDataFiles;               // 数据文件数
    private final long totalDataSizeBytes;           // 数据大小
    private final long addedRecords;                 // 新增记录
    private final long deletedRecords;               // 删除记录
    
    // 格式特定扩展
    private final Map<String, Object> formatSpecificVersion;
}

public enum OperationType {
    APPEND, OVERWRITE, DELETE, UPDATE, COMPACTION, OPTIMIZE
}
```

**缓存键**:
```java
public class VersionCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
    private final long versionId;  // 支持查询历史版本
}
```

**特殊处理**:
- 支持 "CURRENT" 标记，指向最新版本
- 历史版本可以设置更短的过期时间

---

#### Level 3: Schema Cache（Schema 缓存）

**职责**: 缓存表的 Schema 信息，按 schemaId 版本化

**通用数据结构**:
```java
public class UnifiedSchemaMetadata {
    // 通用字段
    private final long schemaId;                     // Schema 版本 ID
    private final List<Column> columns;              // 列定义
    private final List<Column> partitionColumns;     // 分区列
    private final Map<Integer, Column> fieldIdToColumn;  // fieldId -> Column
    private final List<Integer> primaryKeyFieldIds;  // 主键字段 IDs
    
    // 索引加速
    private final Map<String, Column> nameToColumn;
    private final Map<String, Integer> nameToFieldId;
    
    // Schema 演化信息
    private final long parentSchemaId;               // 父 Schema ID
    private final List<SchemaChange> changes;        // Schema 变更历史
    
    // 格式特定扩展
    private final Map<String, Object> formatSpecificSchema;
}

public class SchemaChange {
    private final SchemaChangeType type;  // ADD, DROP, RENAME, CHANGE_TYPE
    private final String fieldName;
    private final Column column;
}
```

**缓存键**:
```java
public class SchemaCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
    private final long schemaId;  // 按 Schema 版本缓存
}
```

---

#### Level 4: Partition/Files Cache（分区文件缓存）

**职责**: 缓存分区级别的元数据和文件列表，最重量级，延迟加载

**通用数据结构**:
```java
public class UnifiedPartitionMetadata {
    // 通用字段
    private final long versionId;                    // 关联的版本 ID
    private final Map<PartitionKey, PartitionInfo> partitions;
    
    @Data
    public static class PartitionInfo {
        private final PartitionSpec partitionSpec;   // 分区规格
        private final List<FileInfo> dataFiles;      // 数据文件列表
        private final long recordCount;
        private final long fileSizeBytes;
        
        // 分区边界（用于裁剪）
        private final Map<Integer, Object> lowerBounds;  // fieldId -> lower bound
        private final Map<Integer, Object> upperBounds;  // fieldId -> upper bound
    }
    
    @Data
    public static class FileInfo {
        private final String filePath;
        private final String fileFormat;             // PARQUET, ORC, AVRO
        private final long recordCount;
        private final long fileSizeBytes;
        
        // 文件级统计（用于过滤）
        private final Map<Integer, Object> minValues;
        private final Map<Integer, Object> maxValues;
        private final Map<Integer, Long> nullCounts;
    }
    
    @Data
    public static class PartitionKey {
        private final Map<String, String> partitionValues;  // field -> value
        private final String partitionPath;                 // 例如: "year=2023/month=11"
    }
}
```

**缓存键**:
```java
public class PartitionCacheKey {
    private final long catalogId;
    private final String dbName;
    private final String tableName;
    private final long versionId;
    
    // 可选：支持更细粒度的分区级缓存
    // private final PartitionKey partitionKey;
}
```

**特殊优化**:
- 支持增量加载（只加载新增分区）
- 支持按需加载（按分区键过滤）
- 可配置是否缓存文件级详细信息

## 四、核心组件设计

### 4.1 统一缓存门面（UnifiedMetaCacheFacade）

**职责**: 对外提供简单统一的查询接口，隐藏内部复杂性

```java
public class UnifiedMetaCacheFacade {
    
    private final TieredMetaCacheOrchestrator orchestrator;
    
    /**
     * 获取表的基础元数据
     */
    public UnifiedTableMetadata getTableMetadata(ExternalTable table) {
        return orchestrator.getTableMetadata(table);
    }
    
    /**
     * 获取当前版本/快照
     */
    public UnifiedVersionMetadata getCurrentVersion(ExternalTable table) {
        return orchestrator.getCurrentVersion(table);
    }
    
    /**
     * 获取指定版本/快照（Time Travel）
     */
    public UnifiedVersionMetadata getVersion(ExternalTable table, long versionId) {
        return orchestrator.getVersion(table, versionId);
    }
    
    /**
     * 获取当前 Schema
     */
    public UnifiedSchemaMetadata getCurrentSchema(ExternalTable table) {
        UnifiedVersionMetadata version = getCurrentVersion(table);
        return orchestrator.getSchema(table, version.getSchemaId());
    }
    
    /**
     * 获取指定 Schema（Schema Evolution）
     */
    public UnifiedSchemaMetadata getSchema(ExternalTable table, long schemaId) {
        return orchestrator.getSchema(table, schemaId);
    }
    
    /**
     * 获取分区信息（按需加载）
     */
    public UnifiedPartitionMetadata getPartitions(ExternalTable table) {
        return orchestrator.getPartitions(table, getCurrentVersion(table).getVersionId());
    }
    
    /**
     * 获取指定版本的分区信息
     */
    public UnifiedPartitionMetadata getPartitions(ExternalTable table, long versionId) {
        return orchestrator.getPartitions(table, versionId);
    }
    
    /**
     * 一站式获取完整元数据
     */
    public CompleteTableMetadata getCompleteMetadata(ExternalTable table, 
                                                      boolean includePartitions) {
        UnifiedTableMetadata tableMeta = getTableMetadata(table);
        UnifiedVersionMetadata version = getCurrentVersion(table);
        UnifiedSchemaMetadata schema = getSchema(table, version.getSchemaId());
        UnifiedPartitionMetadata partitions = includePartitions 
                ? getPartitions(table, version.getVersionId()) 
                : null;
        
        return new CompleteTableMetadata(tableMeta, version, schema, partitions);
    }
}
```

### 4.2 分层缓存协调器（TieredMetaCacheOrchestrator）

**职责**: 管理四层缓存，协调加载和失效

```java
public class TieredMetaCacheOrchestrator {
    
    // 四层通用缓存
    private final LoadingCache<TableCacheKey, UnifiedTableMetadata> tableMetaCache;
    private final LoadingCache<VersionCacheKey, UnifiedVersionMetadata> versionCache;
    private final LoadingCache<SchemaCacheKey, UnifiedSchemaMetadata> schemaCache;
    private final LoadingCache<PartitionCacheKey, UnifiedPartitionMetadata> partitionCache;
    
    // 格式适配器注册表
    private final Map<TableFormat, FormatAdapter> formatAdapters;
    
    // 线程池
    private final ExecutorService executorService;
    
    // 监控指标
    private final TieredCacheMetrics metrics;
    
    public TieredMetaCacheOrchestrator(ExecutorService executor) {
        this.executorService = executor;
        this.formatAdapters = new HashMap<>();
        this.metrics = new TieredCacheMetrics();
        
        // 初始化四层缓存
        initializeCaches();
        
        // 注册格式适配器
        registerFormatAdapters();
    }
    
    /**
     * 初始化四层缓存
     */
    private void initializeCaches() {
        // Level 1: 表基础元数据（最长过期时间）
        tableMetaCache = buildCache(
                key -> loadTableMetadata(key),
                Config.table_metadata_cache_expire_seconds,
                Config.table_metadata_cache_max_size
        );
        
        // Level 2: 版本/快照（中等过期时间）
        versionCache = buildCache(
                key -> loadVersion(key),
                Config.version_cache_expire_seconds,
                Config.version_cache_max_size
        );
        
        // Level 3: Schema（较长过期时间）
        schemaCache = buildCache(
                key -> loadSchema(key),
                Config.schema_cache_expire_seconds,
                Config.schema_cache_max_size
        );
        
        // Level 4: 分区/文件（最短过期时间）
        partitionCache = buildCache(
                key -> loadPartition(key),
                Config.partition_cache_expire_seconds,
                Config.partition_cache_max_size
        );
    }
    
    /**
     * 注册格式适配器
     */
    private void registerFormatAdapters() {
        formatAdapters.put(TableFormat.ICEBERG, new IcebergFormatAdapter());
        formatAdapters.put(TableFormat.HUDI, new HudiFormatAdapter());
        formatAdapters.put(TableFormat.PAIMON, new PaimonFormatAdapter());
        formatAdapters.put(TableFormat.DELTA_LAKE, new DeltaLakeFormatAdapter());
        formatAdapters.put(TableFormat.MAX_COMPUTE, new MaxComputeFormatAdapter());
    }
    
    /**
     * 加载表基础元数据
     */
    private UnifiedTableMetadata loadTableMetadata(TableCacheKey key) {
        ExternalTable table = getTable(key);
        FormatAdapter adapter = getAdapter(table);
        
        metrics.recordLoadStart(CacheLevel.TABLE_METADATA);
        try {
            UnifiedTableMetadata metadata = adapter.loadTableMetadata(table);
            metrics.recordLoadSuccess(CacheLevel.TABLE_METADATA);
            return metadata;
        } catch (Exception e) {
            metrics.recordLoadFailure(CacheLevel.TABLE_METADATA);
            throw new CacheException("Failed to load table metadata", e);
        }
    }
    
    /**
     * 加载版本/快照信息
     */
    private UnifiedVersionMetadata loadVersion(VersionCacheKey key) {
        ExternalTable table = getTable(key);
        FormatAdapter adapter = getAdapter(table);
        
        metrics.recordLoadStart(CacheLevel.VERSION);
        try {
            UnifiedVersionMetadata version = adapter.loadVersion(table, key.getVersionId());
            metrics.recordLoadSuccess(CacheLevel.VERSION);
            return version;
        } catch (Exception e) {
            metrics.recordLoadFailure(CacheLevel.VERSION);
            throw new CacheException("Failed to load version", e);
        }
    }
    
    /**
     * 加载 Schema 信息
     */
    private UnifiedSchemaMetadata loadSchema(SchemaCacheKey key) {
        ExternalTable table = getTable(key);
        FormatAdapter adapter = getAdapter(table);
        
        metrics.recordLoadStart(CacheLevel.SCHEMA);
        try {
            UnifiedSchemaMetadata schema = adapter.loadSchema(table, key.getSchemaId());
            metrics.recordLoadSuccess(CacheLevel.SCHEMA);
            return schema;
        } catch (Exception e) {
            metrics.recordLoadFailure(CacheLevel.SCHEMA);
            throw new CacheException("Failed to load schema", e);
        }
    }
    
    /**
     * 加载分区/文件信息
     */
    private UnifiedPartitionMetadata loadPartition(PartitionCacheKey key) {
        ExternalTable table = getTable(key);
        FormatAdapter adapter = getAdapter(table);
        
        metrics.recordLoadStart(CacheLevel.PARTITION);
        try {
            UnifiedPartitionMetadata partition = adapter.loadPartition(table, key.getVersionId());
            metrics.recordLoadSuccess(CacheLevel.PARTITION);
            return partition;
        } catch (Exception e) {
            metrics.recordLoadFailure(CacheLevel.PARTITION);
            throw new CacheException("Failed to load partition", e);
        }
    }
    
    /**
     * 获取格式适配器
     */
    private FormatAdapter getAdapter(ExternalTable table) {
        TableFormat format = detectTableFormat(table);
        FormatAdapter adapter = formatAdapters.get(format);
        if (adapter == null) {
            throw new UnsupportedOperationException("Unsupported table format: " + format);
        }
        return adapter;
    }
    
    /**
     * 检测表格式
     */
    private TableFormat detectTableFormat(ExternalTable table) {
        if (table instanceof IcebergExternalTable) {
            return TableFormat.ICEBERG;
        } else if (table instanceof HudiDlaTable) {
            return TableFormat.HUDI;
        } else if (table instanceof PaimonExternalTable) {
            return TableFormat.PAIMON;
        } else if (table instanceof MaxComputeExternalTable) {
            return TableFormat.MAX_COMPUTE;
        }
        throw new UnsupportedOperationException("Unknown table type: " + table.getClass());
    }
    
    /**
     * 智能失效：根据失效类型自动判断失效哪些层级
     */
    public void invalidate(ExternalTable table, InvalidationType type) {
        TableCacheKey tableKey = buildTableKey(table);
        
        switch (type) {
            case DROP_TABLE:
                invalidateAllLevels(tableKey);
                break;
                
            case ALTER_SCHEMA:
                // Schema 变更，失效 Level 3, 4
                invalidateSchemaAndPartition(tableKey);
                break;
                
            case DATA_CHANGE:
                // 数据变更，失效 Level 2, 4
                invalidateVersionAndPartition(tableKey);
                break;
                
            case METADATA_REFRESH:
                // 元数据刷新，失效 Level 1, 2, 4（保留 Schema）
                invalidateTableVersionAndPartition(tableKey);
                break;
                
            case PARTITION_REFRESH:
                // 仅刷新分区，失效 Level 4
                invalidatePartition(tableKey);
                break;
        }
        
        metrics.recordInvalidation(type);
    }
    
    /**
     * 增量更新：新的版本
     */
    public void onNewVersion(ExternalTable table, long newVersionId) {
        TableCacheKey tableKey = buildTableKey(table);
        
        // 1. 更新 Level 1 的 currentVersionId
        UnifiedTableMetadata oldMeta = tableMetaCache.getIfPresent(tableKey);
        if (oldMeta != null) {
            UnifiedTableMetadata newMeta = oldMeta.withNewVersion(newVersionId);
            tableMetaCache.put(tableKey, newMeta);
        }
        
        // 2. 加载新版本到 Level 2
        VersionCacheKey versionKey = new VersionCacheKey(
                tableKey.getCatalogId(), 
                tableKey.getDbName(), 
                tableKey.getTableName(), 
                newVersionId
        );
        versionCache.refresh(versionKey);
        
        // 3. 检查 Schema 是否变化
        UnifiedVersionMetadata newVersion = versionCache.get(versionKey);
        if (oldMeta != null && newVersion.getSchemaId() != oldMeta.getCurrentSchemaId()) {
            // Schema 变化，加载新 Schema
            SchemaCacheKey schemaKey = new SchemaCacheKey(
                    tableKey.getCatalogId(), 
                    tableKey.getDbName(), 
                    tableKey.getTableName(), 
                    newVersion.getSchemaId()
            );
            schemaCache.refresh(schemaKey);
        }
        
        // 4. 失效 Level 4（分区可能变化）
        invalidatePartition(tableKey);
        
        metrics.recordIncrementalUpdate();
    }
}
```

### 4.3 格式适配器接口（FormatAdapter）

**职责**: 定义统一的加载接口，各格式实现适配器

```java
public interface FormatAdapter {
    
    /**
     * 加载表基础元数据
     */
    UnifiedTableMetadata loadTableMetadata(ExternalTable table);
    
    /**
     * 加载版本/快照信息
     * @param versionId 版本 ID，CURRENT_VERSION 表示最新版本
     */
    UnifiedVersionMetadata loadVersion(ExternalTable table, long versionId);
    
    /**
     * 加载 Schema 信息
     */
    UnifiedSchemaMetadata loadSchema(ExternalTable table, long schemaId);
    
    /**
     * 加载分区/文件信息
     */
    UnifiedPartitionMetadata loadPartition(ExternalTable table, long versionId);
    
    /**
     * 获取当前版本 ID
     */
    long getCurrentVersionId(ExternalTable table);
    
    /**
     * 获取版本对应的 Schema ID
     */
    long getSchemaIdForVersion(ExternalTable table, long versionId);
    
    /**
     * 支持增量加载分区（可选）
     */
    default UnifiedPartitionMetadata loadPartitionIncremental(
            ExternalTable table, 
            long oldVersionId, 
            long newVersionId) {
        // 默认实现：全量加载
        return loadPartition(table, newVersionId);
    }
}
```

### 4.4 Iceberg 格式适配器实现示例

```java
public class IcebergFormatAdapter implements FormatAdapter {
    
    @Override
    public UnifiedTableMetadata loadTableMetadata(ExternalTable table) {
        // 1. 调用现有的 IcebergMetadataCache 加载 Table
        Table icebergTable = Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache()
                .getIcebergTable(table);
        
        // 2. 转换为统一格式
        return UnifiedTableMetadata.builder()
                .tableLocation(icebergTable.location())
                .metadataLocation(icebergTable.properties().get(TableProperties.METADATA_LOCATION))
                .properties(icebergTable.properties())
                .currentVersionId(icebergTable.currentSnapshot().snapshotId())
                .lastModifiedTime(icebergTable.currentSnapshot().timestampMillis())
                .format(TableFormat.ICEBERG)
                .isPartitioned(icebergTable.spec().isPartitioned())
                .partitionColumnCount(icebergTable.spec().fields().size())
                .formatSpecificMetadata(buildIcebergSpecific(icebergTable))
                .build();
    }
    
    @Override
    public UnifiedVersionMetadata loadVersion(ExternalTable table, long versionId) {
        Table icebergTable = getIcebergTable(table);
        
        Snapshot snapshot;
        if (versionId == CURRENT_VERSION) {
            snapshot = icebergTable.currentSnapshot();
        } else {
            snapshot = icebergTable.snapshot(versionId);
        }
        
        if (snapshot == null) {
            throw new CacheException("Snapshot not found: " + versionId);
        }
        
        Map<String, String> summary = snapshot.summary();
        
        return UnifiedVersionMetadata.builder()
                .versionId(snapshot.snapshotId())
                .schemaId(snapshot.schemaId())
                .parentVersionId(snapshot.parentId())
                .timestampMillis(snapshot.timestampMillis())
                .manifestLocation(snapshot.manifestListLocation())
                .operation(parseOperation(snapshot.operation()))
                .totalRecords(parseLong(summary.get(SnapshotSummary.TOTAL_RECORDS_PROP)))
                .totalDataFiles(parseLong(summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP)))
                .totalDataSizeBytes(parseLong(summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP)))
                .addedRecords(parseLong(summary.get(SnapshotSummary.ADDED_RECORDS_PROP)))
                .deletedRecords(parseLong(summary.get(SnapshotSummary.DELETED_RECORDS_PROP)))
                .formatSpecificVersion(buildIcebergVersionSpecific(snapshot))
                .build();
    }
    
    @Override
    public UnifiedSchemaMetadata loadSchema(ExternalTable table, long schemaId) {
        Table icebergTable = getIcebergTable(table);
        Schema icebergSchema = icebergTable.schemas().get((int) schemaId);
        
        if (icebergSchema == null) {
            throw new CacheException("Schema not found: " + schemaId);
        }
        
        List<Column> columns = convertIcebergSchema(icebergSchema);
        List<Column> partitionColumns = extractPartitionColumns(icebergTable, icebergSchema);
        
        return UnifiedSchemaMetadata.builder()
                .schemaId(schemaId)
                .columns(columns)
                .partitionColumns(partitionColumns)
                .fieldIdToColumn(buildFieldIdMapping(icebergSchema))
                .primaryKeyFieldIds(icebergSchema.identifierFieldIds())
                .nameToColumn(buildNameMapping(columns))
                .formatSpecificSchema(buildIcebergSchemaSpecific(icebergSchema))
                .build();
    }
    
    @Override
    public UnifiedPartitionMetadata loadPartition(ExternalTable table, long versionId) {
        Table icebergTable = getIcebergTable(table);
        Snapshot snapshot = icebergTable.snapshot(versionId);
        
        if (snapshot == null) {
            throw new CacheException("Snapshot not found: " + versionId);
        }
        
        // 读取 manifest files
        Map<PartitionKey, PartitionInfo> partitions = new HashMap<>();
        
        for (ManifestFile manifest : snapshot.allManifests(icebergTable.io())) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, icebergTable.io())) {
                for (DataFile file : reader) {
                    PartitionKey key = buildPartitionKey(file.partition());
                    PartitionInfo info = partitions.computeIfAbsent(key, 
                            k -> new PartitionInfo());
                    
                    info.addFile(convertDataFile(file));
                }
            }
        }
        
        return new UnifiedPartitionMetadata(versionId, partitions);
    }
    
    @Override
    public UnifiedPartitionMetadata loadPartitionIncremental(
            ExternalTable table, long oldVersionId, long newVersionId) {
        
        Table icebergTable = getIcebergTable(table);
        
        // 1. 获取旧分区缓存
        PartitionCacheKey oldKey = new PartitionCacheKey(
                table.getCatalog().getId(), 
                table.getDbName(), 
                table.getName(), 
                oldVersionId
        );
        UnifiedPartitionMetadata oldPartitions = getPartitionCacheIfPresent(oldKey);
        
        if (oldPartitions == null) {
            // 没有旧缓存，全量加载
            return loadPartition(table, newVersionId);
        }
        
        // 2. 只加载新增的 manifest files
        List<ManifestFile> incrementalManifests = getIncrementalManifests(
                icebergTable, oldVersionId, newVersionId);
        
        if (incrementalManifests.isEmpty()) {
            // 没有新增，直接返回旧缓存
            return oldPartitions.withNewVersionId(newVersionId);
        }
        
        // 3. 合并旧缓存和新数据
        Map<PartitionKey, PartitionInfo> mergedPartitions = 
                new HashMap<>(oldPartitions.getPartitions());
        
        for (ManifestFile manifest : incrementalManifests) {
            // 解析新的 manifest 并合并
            parseAndMerge(manifest, mergedPartitions, icebergTable.io());
        }
        
        return new UnifiedPartitionMetadata(newVersionId, mergedPartitions);
    }
    
    private Table getIcebergTable(ExternalTable table) {
        return Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache()
                .getIcebergTable(table);
    }
}
```

### 4.5 Hudi 格式适配器实现示例

```java
public class HudiFormatAdapter implements FormatAdapter {
    
    @Override
    public UnifiedTableMetadata loadTableMetadata(ExternalTable table) {
        // 获取 Hudi MetaClient
        HoodieTableMetaClient metaClient = Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getMetaClientProcessor(table.getCatalog())
                .getHoodieTableMetaClient(
                        table.getOrBuildNameMapping(),
                        getHudiBasePath(table),
                        getHadoopConf(table)
                );
        
        // 获取最新 commit
        HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline();
        Option<HoodieInstant> latestCommit = timeline.lastInstant();
        
        if (!latestCommit.isPresent()) {
            throw new CacheException("No commits found in Hudi table");
        }
        
        return UnifiedTableMetadata.builder()
                .tableLocation(metaClient.getBasePath().toString())
                .metadataLocation(metaClient.getMetaPath().toString())
                .properties(convertHudiProperties(metaClient))
                .currentVersionId(parseCommitTime(latestCommit.get()))  // Hudi 用 commitTime 作为版本
                .lastModifiedTime(parseCommitTime(latestCommit.get()))
                .format(TableFormat.HUDI)
                .isPartitioned(metaClient.getTableConfig().isTablePartitioned())
                .formatSpecificMetadata(buildHudiSpecific(metaClient))
                .build();
    }
    
    @Override
    public UnifiedVersionMetadata loadVersion(ExternalTable table, long versionId) {
        HoodieTableMetaClient metaClient = getHudiMetaClient(table);
        
        // Hudi 的 versionId 就是 commitTime
        String commitTime;
        if (versionId == CURRENT_VERSION) {
            Option<HoodieInstant> latest = metaClient.getActiveTimeline()
                    .getCommitsTimeline().lastInstant();
            commitTime = latest.get().getTimestamp();
        } else {
            commitTime = String.valueOf(versionId);
        }
        
        HoodieInstant instant = new HoodieInstant(
                HoodieInstant.State.COMPLETED, 
                HoodieTimeline.COMMIT_ACTION, 
                commitTime
        );
        
        // 读取 commit metadata
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                metaClient.getActiveTimeline().getInstantDetails(instant).get(),
                HoodieCommitMetadata.class
        );
        
        return UnifiedVersionMetadata.builder()
                .versionId(Long.parseLong(commitTime))
                .schemaId(extractSchemaVersion(commitMetadata))  // Hudi 的 schema 版本
                .parentVersionId(extractParentCommit(metaClient, commitTime))
                .timestampMillis(Long.parseLong(commitTime))
                .manifestLocation(null)  // Hudi 没有 manifest list
                .operation(parseHudiOperation(commitMetadata.getOperationType()))
                .totalRecords(commitMetadata.fetchTotalRecordsWritten())
                .totalDataFiles(commitMetadata.fetchTotalFilesInsert() + commitMetadata.fetchTotalFilesUpdated())
                .totalDataSizeBytes(commitMetadata.fetchTotalBytesWritten())
                .addedRecords(commitMetadata.fetchTotalRecordsWritten())
                .deletedRecords(commitMetadata.fetchTotalRecordsDeleted())
                .formatSpecificVersion(buildHudiVersionSpecific(commitMetadata))
                .build();
    }
    
    @Override
    public UnifiedSchemaMetadata loadSchema(ExternalTable table, long schemaId) {
        // Hudi 的 schema 从 TableSchema 获取
        HoodieTableMetaClient metaClient = getHudiMetaClient(table);
        TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
        
        Schema avroSchema;
        try {
            if (schemaId == CURRENT_SCHEMA) {
                avroSchema = schemaResolver.getTableAvroSchema();
            } else {
                avroSchema = schemaResolver.getTableAvroSchema((int) schemaId);
            }
        } catch (Exception e) {
            throw new CacheException("Failed to get Hudi schema", e);
        }
        
        List<Column> columns = convertAvroSchema(avroSchema);
        List<Column> partitionColumns = extractHudiPartitionColumns(metaClient);
        
        return UnifiedSchemaMetadata.builder()
                .schemaId(schemaId)
                .columns(columns)
                .partitionColumns(partitionColumns)
                .fieldIdToColumn(buildFieldIdMapping(avroSchema))
                .nameToColumn(buildNameMapping(columns))
                .formatSpecificSchema(buildHudiSchemaSpecific(avroSchema))
                .build();
    }
    
    @Override
    public UnifiedPartitionMetadata loadPartition(ExternalTable table, long versionId) {
        HoodieTableMetaClient metaClient = getHudiMetaClient(table);
        
        // 获取 FileSystemView
        HoodieTableFileSystemView fsView = Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getFsViewProcessor(table.getCatalog())
                .getFsView(table.getDbName(), table.getName(), metaClient);
        
        String commitTime = String.valueOf(versionId);
        Map<PartitionKey, PartitionInfo> partitions = new HashMap<>();
        
        // 遍历所有分区
        List<String> partitionPaths = FSUtils.getAllPartitionPaths(
                metaClient.getEngineContext(),
                metaClient.getStorage(),
                metaClient.getBasePath()
        );
        
        for (String partitionPath : partitionPaths) {
            // 获取该分区在指定 commit 的文件
            Stream<HoodieBaseFile> baseFiles = fsView.getLatestBaseFilesBeforeOrOn(
                    partitionPath, commitTime);
            
            PartitionKey key = buildPartitionKeyFromPath(partitionPath);
            PartitionInfo info = new PartitionInfo();
            
            baseFiles.forEach(file -> {
                info.addFile(convertHudiFile(file));
            });
            
            if (!info.getDataFiles().isEmpty()) {
                partitions.put(key, info);
            }
        }
        
        return new UnifiedPartitionMetadata(versionId, partitions);
    }
    
    private HoodieTableMetaClient getHudiMetaClient(ExternalTable table) {
        return Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getMetaClientProcessor(table.getCatalog())
                .getHoodieTableMetaClient(
                        table.getOrBuildNameMapping(),
                        getHudiBasePath(table),
                        getHadoopConf(table)
                );
    }
}
```

## 五、配置设计

### 5.1 统一配置项

```properties
# ===== 统一元数据缓存配置 =====

# 是否启用统一缓存架构（灰度开关）
enable_unified_metadata_cache = false

# 兼容模式：新旧缓存并行运行
unified_cache_compatibility_mode = true

# ===== Level 1: TableMetadata 配置 =====
table_metadata_cache_expire_seconds = 86400        # 24小时
table_metadata_cache_refresh_seconds = 3600        # 1小时自动刷新
table_metadata_cache_max_size = 10000              # 最多缓存10000个表

# ===== Level 2: Version/Snapshot 配置 =====
version_cache_expire_seconds = 7200                # 2小时
version_cache_refresh_seconds = 1800               # 30分钟自动刷新
version_cache_max_size = 50000                     # 支持更多版本

# 历史版本过期时间（更短）
historical_version_cache_expire_seconds = 1800     # 30分钟

# ===== Level 3: Schema 配置 =====
schema_cache_expire_seconds = 43200                # 12小时
schema_cache_refresh_seconds = 3600                # 1小时自动刷新
schema_cache_max_size = 5000

# ===== Level 4: Partition/Files 配置 =====
partition_cache_expire_seconds = 3600              # 1小时
partition_cache_refresh_seconds = 600              # 10分钟自动刷新
partition_cache_max_size = 10000

# 分区缓存优化
partition_cache_incremental_load = true            # 启用增量加载
partition_cache_max_files_per_partition = 10000    # 单分区最多缓存文件数

# ===== 通用配置 =====
unified_cache_loader_thread_pool_size = 20         # 缓存加载线程池大小
unified_cache_enable_metrics = true                # 启用监控指标
unified_cache_metrics_report_interval_seconds = 60 # 指标上报间隔

# ===== 格式特定配置 =====
# Iceberg
iceberg_manifest_cache_enabled = true              # 启用 Manifest 缓存

# Hudi
hudi_timeline_cache_enabled = true                 # 启用 Timeline 缓存
hudi_fsview_cache_enabled = true                   # 启用 FSView 缓存

# Paimon
paimon_snapshot_cache_enabled = true               # 启用 Snapshot 缓存
```

### 5.2 动态配置

```java
public class UnifiedCacheConfig {
    
    // 运行时动态调整缓存大小
    public static void adjustCacheSize(CacheLevel level, long newSize) {
        Config config = getConfig(level);
        config.setMaxSize(newSize);
        
        // 触发缓存重建或调整
        getCacheOrchestrator().resizeCache(level, newSize);
    }
    
    // 运行时调整过期时间
    public static void adjustExpireTime(CacheLevel level, long seconds) {
        Config config = getConfig(level);
        config.setExpireSeconds(seconds);
        
        // 立即生效
        getCacheOrchestrator().updateExpirePolicy(level, seconds);
    }
}
```

## 六、监控和指标

### 6.1 监控指标

```java
public class TieredCacheMetrics {
    
    // 各层缓存指标
    private final Map<CacheLevel, LevelMetrics> levelMetrics;
    
    @Data
    public static class LevelMetrics {
        // 基础指标
        private AtomicLong hitCount = new AtomicLong(0);
        private AtomicLong missCount = new AtomicLong(0);
        private AtomicLong loadSuccessCount = new AtomicLong(0);
        private AtomicLong loadFailureCount = new AtomicLong(0);
        private AtomicLong evictionCount = new AtomicLong(0);
        
        // 时间指标
        private AtomicLong totalLoadTimeNanos = new AtomicLong(0);
        private AtomicLong maxLoadTimeNanos = new AtomicLong(0);
        private AtomicLong minLoadTimeNanos = new AtomicLong(Long.MAX_VALUE);
        
        // 大小指标
        private AtomicLong estimatedSize = new AtomicLong(0);
        private AtomicLong estimatedMemoryBytes = new AtomicLong(0);
        
        // 计算属性
        public double getHitRatio() {
            long hits = hitCount.get();
            long misses = missCount.get();
            long total = hits + misses;
            return total == 0 ? 0.0 : (double) hits / total;
        }
        
        public double getAverageLoadTimeMillis() {
            long count = loadSuccessCount.get();
            return count == 0 ? 0.0 : totalLoadTimeNanos.get() / count / 1_000_000.0;
        }
    }
    
    /**
     * 记录缓存命中
     */
    public void recordHit(CacheLevel level) {
        levelMetrics.get(level).hitCount.incrementAndGet();
    }
    
    /**
     * 记录缓存未命中
     */
    public void recordMiss(CacheLevel level) {
        levelMetrics.get(level).missCount.incrementAndGet();
    }
    
    /**
     * 记录加载开始
     */
    public long recordLoadStart(CacheLevel level) {
        return System.nanoTime();
    }
    
    /**
     * 记录加载成功
     */
    public void recordLoadSuccess(CacheLevel level, long startNanos) {
        LevelMetrics metrics = levelMetrics.get(level);
        long duration = System.nanoTime() - startNanos;
        
        metrics.loadSuccessCount.incrementAndGet();
        metrics.totalLoadTimeNanos.addAndGet(duration);
        metrics.maxLoadTimeNanos.updateAndGet(max -> Math.max(max, duration));
        metrics.minLoadTimeNanos.updateAndGet(min -> Math.min(min, duration));
    }
    
    /**
     * 获取综合指标
     */
    public UnifiedCacheMetrics getOverallMetrics() {
        return UnifiedCacheMetrics.builder()
                .levelMetrics(new HashMap<>(levelMetrics))
                .overallHitRatio(calculateOverallHitRatio())
                .totalMemoryUsageBytes(calculateTotalMemory())
                .totalCachedEntries(calculateTotalEntries())
                .build();
    }
}
```

### 6.2 SHOW PROC 扩展

```sql
-- 查看统一缓存的整体统计
SHOW PROC '/unified_cache_stats';

-- 输出示例:
+-------------+-----------+-----------+-----------+------------------+------------------+
| Cache Level | Hit Ratio | Hit Count | Miss Count| Avg Load Time(ms)| Estimated Size   |
+-------------+-----------+-----------+-----------+------------------+------------------+
| TableMeta   | 95.5%     | 12000     | 500       | 15.2             | 8500             |
| Version     | 88.3%     | 9800      | 1200      | 52.8             | 15000            |
| Schema      | 99.2%     | 11900     | 100       | 12.5             | 4200             |
| Partition   | 72.1%     | 7210      | 2790      | 285.3            | 6800             |
+-------------+-----------+-----------+-----------+------------------+------------------+

-- 查看指定表的缓存状态
SHOW PROC '/unified_cache_stats/{catalog}/{db}/{table}';

-- 输出示例:
+-------------+--------+----------------+-------------------+------------------+
| Cache Level | Cached | Version/Schema| Last Access Time  | Memory Size(KB)  |
+-------------+--------+----------------+-------------------+------------------+
| TableMeta   | Yes    | -              | 2025-11-05 10:30  | 2.5              |
| Version     | Yes    | v_100023       | 2025-11-05 10:30  | 5.2              |
| Schema      | Yes    | schema_5       | 2025-11-05 10:28  | 12.8             |
| Partition   | Yes    | v_100023       | 2025-11-05 10:30  | 1024.5           |
+-------------+--------+----------------+-------------------+------------------+

-- 查看各格式的缓存分布
SHOW PROC '/unified_cache_stats/by_format';

-- 输出示例:
+--------------+--------------+----------------+--------------+------------------+
| Table Format | Table Count  | Avg Hit Ratio  | Total Memory | Avg Load Time(ms)|
+--------------+--------------+----------------+--------------+------------------+
| ICEBERG      | 3500         | 92.3%          | 1.2 GB       | 45.2             |
| HUDI         | 1200         | 85.7%          | 450 MB       | 68.5             |
| PAIMON       | 800          | 89.1%          | 280 MB       | 52.3             |
| MAX_COMPUTE  | 500          | 78.5%          | 120 MB       | 95.8             |
+--------------+--------------+----------------+--------------+------------------+
```

### 6.3 告警规则

```yaml
# 统一缓存告警规则
alerts:
  # 缓存命中率低
  - name: low_cache_hit_ratio
    condition: cache_hit_ratio < 0.7
    severity: warning
    message: "Cache hit ratio is below 70% for {cache_level}"
    
  # 加载时间过长
  - name: slow_cache_load
    condition: avg_load_time_ms > 500
    severity: warning
    message: "Average cache load time exceeds 500ms for {cache_level}"
    
  # 内存使用过高
  - name: high_memory_usage
    condition: cache_memory_usage_mb > 10240
    severity: critical
    message: "Cache memory usage exceeds 10GB"
    
  # 驱逐率过高
  - name: high_eviction_rate
    condition: eviction_rate_per_second > 100
    severity: warning
    message: "Cache eviction rate is too high: {eviction_rate}/s"
    
  # 加载失败率高
  - name: high_load_failure_rate
    condition: load_failure_ratio > 0.05
    severity: critical
    message: "Cache load failure rate exceeds 5%"
```

## 七、实施计划

### 7.1 Phase 1: 基础框架（4周）

**Week 1-2: 核心接口和数据结构**
- [ ] 定义四层通用数据结构（UnifiedTableMetadata 等）
- [ ] 定义 FormatAdapter 接口
- [ ] 实现 TieredMetaCacheOrchestrator 框架
- [ ] 实现 UnifiedMetaCacheFacade

**Week 3: 第一个适配器 - Iceberg**
- [ ] 实现 IcebergFormatAdapter
- [ ] 迁移现有 IcebergMetadataCache 逻辑
- [ ] 编写单元测试

**Week 4: 集成测试和修复**
- [ ] 集成测试
- [ ] Bug 修复
- [ ] 性能基准测试

### 7.2 Phase 2: 其他格式适配（4周）

**Week 5: Hudi 适配器**
- [ ] 实现 HudiFormatAdapter
- [ ] 适配 Timeline, FSView 等特殊缓存
- [ ] 单元测试和集成测试

**Week 6: Paimon 适配器**
- [ ] 实现 PaimonFormatAdapter
- [ ] 单元测试和集成测试

**Week 7: MaxCompute 和其他格式**
- [ ] 实现 MaxComputeFormatAdapter
- [ ] 预留 DeltaLake 适配器接口
- [ ] 单元测试

**Week 8: 全量集成测试**
- [ ] 所有格式的集成测试
- [ ] 混合场景测试
- [ ] 性能对比测试

### 7.3 Phase 3: 高级特性（3周）

**Week 9: 智能失效和增量更新**
- [ ] 实现智能失效机制
- [ ] 实现增量更新逻辑
- [ ] 优化 REFRESH 命令

**Week 10: 监控和指标**
- [ ] 实现完整的监控指标
- [ ] 实现 SHOW PROC 扩展
- [ ] 集成告警系统

**Week 11: 性能优化**
- [ ] 缓存预热机制
- [ ] 自适应缓存大小
- [ ] 分区缓存优化

### 7.4 Phase 4: 灰度上线（2周）

**Week 12: 灰度准备**
- [ ] 实现特性开关
- [ ] 实现兼容模式（新旧并行）
- [ ] 压力测试

**Week 13: 灰度发布**
- [ ] 5% 流量切换
- [ ] 监控和调优
- [ ] 逐步扩大到 100%

### 7.5 Phase 5: 清理和文档（1周）

**Week 14: 完成**
- [ ] 清理旧代码
- [ ] 更新用户文档
- [ ] 编写运维手册

## 八、向后兼容性

### 8.1 兼容策略

```java
public class CompatibilityLayer {
    
    /**
     * 保持现有接口不变
     */
    public static Table getIcebergTable(ExternalTable table) {
        if (Config.enableUnifiedMetadataCache) {
            // 使用新缓存
            UnifiedMetaCacheFacade facade = getUnifiedCacheFacade();
            CompleteTableMetadata metadata = facade.getCompleteMetadata(table, false);
            return reconstructIcebergTable(metadata);
        } else {
            // 使用旧缓存
            return Env.getCurrentEnv()
                    .getExtMetaCacheMgr()
                    .getIcebergMetadataCache()
                    .getIcebergTable(table);
        }
    }
    
    /**
     * 从统一元数据重建 Iceberg Table 对象
     */
    private static Table reconstructIcebergTable(CompleteTableMetadata metadata) {
        // 使用统一元数据构建 Iceberg Table
        // 可能需要一些转换逻辑
        return IcebergTableBuilder.build(metadata);
    }
}
```

### 8.2 数据迁移

```java
public class CacheMigrationService {
    
    /**
     * 将旧缓存迁移到统一缓存
     */
    public void migrateToUnifiedCache() {
        // 1. 迁移 Iceberg 缓存
        migrateIcebergCache();
        
        // 2. 迁移 Hudi 缓存
        migrateHudiCache();
        
        // 3. 迁移 Paimon 缓存
        migratePaimonCache();
        
        // 4. 迁移 MaxCompute 缓存
        migrateMaxComputeCache();
    }
    
    private void migrateIcebergCache() {
        IcebergMetadataCache oldCache = getOldIcebergCache();
        UnifiedMetaCacheFacade newCache = getUnifiedCacheFacade();
        
        oldCache.tableCache.asMap().forEach((key, table) -> {
            try {
                // 转换并写入新缓存
                ExternalTable dorisTable = getDorisTable(key);
                newCache.getCompleteMetadata(dorisTable, false);
                
                LOG.info("Migrated Iceberg table: {}", key);
            } catch (Exception e) {
                LOG.warn("Failed to migrate Iceberg table: {}", key, e);
            }
        });
    }
}
```

## 九、性能对比

### 9.1 预期性能提升

| 指标 | 当前架构 | 统一架构 | 提升 |
|-----|---------|---------|------|
| 整体缓存命中率 | 70-75% | 90%+ | +20% |
| 平均查询延迟 | 100ms | 50ms | -50% |
| REFRESH 耗时 | 5s | 1s | -80% |
| 内存利用率 | 60% | 85% | +42% |
| 支持表数量 | 10K | 50K+ | +400% |
| 新格式接入成本 | 2-3周 | 3-5天 | -70% |

### 9.2 性能测试场景

```bash
#!/bin/bash
# 性能对比测试

# Scenario 1: 冷启动性能
test_cold_start() {
    # 清空所有缓存
    # 访问 1000 个表
    # 记录总耗时和平均延迟
}

# Scenario 2: 缓存命中性能
test_cache_hit() {
    # 预热缓存
    # 重复访问 1000 个表
    # 记录命中率和延迟
}

# Scenario 3: 混合负载
test_mixed_workload() {
    # 70% 读热点表
    # 20% 读长尾表
    # 10% 写操作（失效缓存）
    # 记录综合性能
}

# Scenario 4: 多格式混合
test_multi_format() {
    # 同时访问 Iceberg, Hudi, Paimon 表
    # 记录各格式性能和资源使用
}
```

## 十、风险和挑战

### 10.1 技术风险

| 风险 | 影响 | 概率 | 缓解措施 |
|-----|------|------|---------|
| 格式差异导致抽象泄露 | 高 | 中 | 提供丰富的扩展点，允许格式特定优化 |
| 性能回退 | 高 | 低 | 充分的性能测试，保留回滚机制 |
| 内存占用增加 | 中 | 中 | 细粒度缓存，智能驱逐策略 |
| 复杂度增加 | 中 | 高 | 清晰的文档和架构设计 |
| 兼容性问题 | 高 | 低 | 完善的兼容层和渐进式迁移 |

### 10.2 挑战应对

**挑战 1: 格式差异大**
- 方案：提供灵活的扩展机制（formatSpecificMetadata）
- 方案：允许适配器覆盖默认行为

**挑战 2: 迁移成本高**
- 方案：分阶段实施，每个格式独立迁移
- 方案：保持向后兼容，新旧缓存并存

**挑战 3: 性能要求高**
- 方案：充分的性能测试和优化
- 方案：提供性能对比工具

## 十一、总结

### 11.1 核心优势

1. **统一抽象**: 一套框架支持所有数据湖格式
2. **分层设计**: 按需加载，细粒度控制
3. **易于扩展**: 新格式只需实现适配器
4. **统一管理**: 统一配置、监控、失效策略
5. **性能优化**: 智能失效、增量更新、缓存预热

### 11.2 关键创新

1. **格式无关的四层缓存模型**
2. **适配器模式的格式扩展**
3. **智能失效机制**
4. **增量更新支持**
5. **统一监控和指标体系**

### 11.3 后续演进方向

1. **Phase 6: 分布式缓存同步**（可选）
   - 多 FE 之间的缓存一致性
   - 基于 gossip 的失效传播

2. **Phase 7: 智能预取**（可选）
   - 基于查询历史的智能预测
   - 自动预热热点表

3. **Phase 8: 持久化缓存**（可选）
   - 缓存持久化到本地磁盘
   - FE 重启快速恢复

---

**文档版本**: v1.0  
**最后更新**: 2025-11-05  
**作者**: AI 架构设计  
**状态**: 设计方案 - 待审核  



