# Iceberg 元数据缓存架构图

## 1. 整体架构图

```mermaid
graph TB
    subgraph "查询层"
        Query[查询请求]
        IcebergScanNode[IcebergScanNode]
        IcebergSource[IcebergSource]
    end
    
    subgraph "缓存管理层"
        ExternalMetaCacheMgr[ExternalMetaCacheMgr<br/>统一缓存管理器]
        IcebergMetadataCacheMgr[IcebergMetadataCacheMgr<br/>Iceberg 缓存管理器]
        ExternalSchemaCache[ExternalSchemaCache<br/>Schema 缓存]
    end
    
    subgraph "核心缓存层"
        IcebergMetadataCache[IcebergMetadataCache]
        
        subgraph "四种缓存"
            TableCache[tableCache<br/>Table 缓存]
            SnapshotListCache[snapshotListCache<br/>Snapshot 列表缓存]
            SnapshotCache[snapshotCache<br/>Snapshot 值缓存<br/>MTMV 用]
            ViewCache[viewCache<br/>View 缓存]
        end
        
        SchemaCache[schemaCache<br/>Schema 缓存]
    end
    
    subgraph "元数据操作层"
        IcebergMetadataOps[IcebergMetadataOps<br/>元数据操作]
        IcebergCatalog[Iceberg Catalog<br/>HMS/REST/Glue/DLF]
    end
    
    subgraph "外部存储"
        MetaStore[(元数据存储<br/>HMS/REST)]
        DataFiles[(数据文件<br/>HDFS/S3/OSS)]
    end
    
    Query --> IcebergScanNode
    IcebergScanNode --> IcebergSource
    IcebergSource --> ExternalMetaCacheMgr
    
    ExternalMetaCacheMgr --> IcebergMetadataCacheMgr
    ExternalMetaCacheMgr --> ExternalSchemaCache
    
    IcebergMetadataCacheMgr --> IcebergMetadataCache
    ExternalSchemaCache --> SchemaCache
    
    IcebergMetadataCache --> TableCache
    IcebergMetadataCache --> SnapshotListCache
    IcebergMetadataCache --> SnapshotCache
    IcebergMetadataCache --> ViewCache
    
    TableCache --> IcebergMetadataOps
    ViewCache --> IcebergMetadataOps
    SchemaCache --> IcebergMetadataOps
    
    IcebergMetadataOps --> IcebergCatalog
    IcebergCatalog --> MetaStore
    IcebergCatalog --> DataFiles
```

## 2. Table 缓存加载流程

```mermaid
sequenceDiagram
    participant User as 用户查询
    participant Utils as IcebergUtils
    participant CacheMgr as ExternalMetaCacheMgr
    participant MetaCache as IcebergMetadataCache
    participant TableCache as tableCache
    participant Ops as IcebergMetadataOps
    participant Catalog as Iceberg Catalog
    participant Store as 元数据存储

    User->>Utils: getIcebergTable(dorisTable)
    Utils->>CacheMgr: getIcebergMetadataCache()
    CacheMgr-->>Utils: IcebergMetadataCache
    Utils->>MetaCache: getIcebergTable(dorisTable)
    MetaCache->>TableCache: get(key)
    
    alt 缓存命中
        TableCache-->>MetaCache: 返回 Table
    else 缓存未命中
        TableCache->>MetaCache: 触发 loadTable(key)
        MetaCache->>Ops: loadTable(dbName, tblName)
        Ops->>Catalog: loadTable(identifier)
        Catalog->>Store: 读取元数据
        Store-->>Catalog: 元数据
        Catalog-->>Ops: Table 对象
        Ops-->>MetaCache: Table 对象
        MetaCache->>TableCache: 放入缓存
        TableCache-->>MetaCache: 返回 Table
    end
    
    MetaCache-->>Utils: Table
    Utils-->>User: Table
```

## 3. Schema 缓存加载流程

```mermaid
sequenceDiagram
    participant Table as IcebergExternalTable
    participant Utils as IcebergUtils
    participant CacheMgr as ExternalMetaCacheMgr
    participant SchemaCache as ExternalSchemaCache
    participant Cache as schemaCache
    participant Catalog as ExternalCatalog

    Table->>Utils: getIcebergSchema(dorisTable)
    Utils->>Utils: getOrFetchSnapshotCacheValue()<br/>获取 snapshotId 和 schemaId
    Utils->>Utils: getSchemaCacheValue(dorisTable, schemaId)
    Utils->>CacheMgr: getSchemaCache(catalog)
    CacheMgr-->>Utils: ExternalSchemaCache
    Utils->>SchemaCache: getSchemaValue(IcebergSchemaCacheKey)
    SchemaCache->>Cache: get(key)
    
    alt 缓存命中
        Cache-->>SchemaCache: IcebergSchemaCacheValue
    else 缓存未命中
        Cache->>SchemaCache: 触发 loadSchema(key)
        SchemaCache->>Catalog: getSchema(key)
        Catalog->>Table: initSchema(key)
        Table->>Utils: loadSchemaCacheValue()
        Utils->>Utils: 获取 Table/View<br/>解析 Schema
        Utils-->>Table: Optional<IcebergSchemaCacheValue>
        Table-->>Catalog: Optional<SchemaCacheValue>
        Catalog-->>SchemaCache: Optional<SchemaCacheValue>
        SchemaCache->>Cache: 放入缓存
        Cache-->>SchemaCache: IcebergSchemaCacheValue
    end
    
    SchemaCache-->>Utils: IcebergSchemaCacheValue
    Utils-->>Table: List<Column>
```

## 4. Snapshot 缓存加载流程（MTMV）

```mermaid
sequenceDiagram
    participant MTMV as MTMV 刷新
    participant Table as IcebergExternalTable
    participant Utils as IcebergUtils
    participant Cache as IcebergMetadataCache
    participant SnapshotCache as snapshotCache

    MTMV->>Table: getAndCopyPartitionItems()
    Table->>Utils: getOrFetchSnapshotCacheValue(snapshot, table)
    Utils->>Cache: getSnapshotCache(dorisTable)
    Cache->>SnapshotCache: get(key)
    
    alt 缓存命中
        SnapshotCache-->>Cache: IcebergSnapshotCacheValue
    else 缓存未命中
        SnapshotCache->>Cache: 触发 loadSnapshot(key)
        Cache->>Utils: getLastedIcebergSnapshot(table)
        Utils->>Utils: getIcebergTable()<br/>获取 Table
        Utils->>Utils: table.currentSnapshot()
        Utils-->>Cache: IcebergSnapshot<br/>(snapshotId, schemaId)
        
        alt 表有效且有分区
            Cache->>Utils: loadPartitionInfo(table, snapshotId)
            Utils->>Utils: loadIcebergPartition()<br/>扫描 Partitions 元数据表
            Utils->>Utils: getSchemaCacheValue()<br/>获取分区列
            Utils->>Utils: 构建分区信息
            Utils-->>Cache: IcebergPartitionInfo
        else 表无效或无分区
            Cache->>Cache: IcebergPartitionInfo.empty()
        end
        
        Cache->>SnapshotCache: 放入缓存<br/>IcebergSnapshotCacheValue
        SnapshotCache-->>Cache: IcebergSnapshotCacheValue
    end
    
    Cache-->>Utils: IcebergSnapshotCacheValue
    Utils->>Utils: getPartitionInfo()
    Utils-->>Table: Map<String, PartitionItem>
    Table-->>MTMV: 分区信息
```

## 5. 缓存失效流程

```mermaid
graph TB
    subgraph "失效触发"
        RefreshCatalog[REFRESH CATALOG]
        RefreshTable[REFRESH TABLE]
        DDL[DDL 操作]
        DropCatalog[DROP CATALOG]
    end
    
    subgraph "失效入口"
        ExternalMetaCacheMgr[ExternalMetaCacheMgr]
    end
    
    subgraph "失效目标"
        IcebergMetadataCacheMgr[IcebergMetadataCacheMgr]
        ExternalSchemaCache[ExternalSchemaCache]
    end
    
    subgraph "具体缓存失效"
        InvalidateTable[invalidateTableCache]
        InvalidateDb[invalidateDbCache]
        InvalidateCatalog[invalidateCatalogCache]
    end
    
    subgraph "清理动作"
        ClearTableCache[清理 tableCache]
        ClearSnapshotCache[清理 snapshotCache]
        ClearSnapshotListCache[清理 snapshotListCache]
        ClearViewCache[清理 viewCache]
        ClearSchemaCache[清理 schemaCache]
        DropManifestCache[调用 ManifestFiles.dropCache<br/>清理 Iceberg 内部缓存]
    end
    
    RefreshCatalog --> ExternalMetaCacheMgr
    RefreshTable --> ExternalMetaCacheMgr
    DDL --> ExternalMetaCacheMgr
    DropCatalog --> ExternalMetaCacheMgr
    
    ExternalMetaCacheMgr --> IcebergMetadataCacheMgr
    ExternalMetaCacheMgr --> ExternalSchemaCache
    
    IcebergMetadataCacheMgr --> InvalidateTable
    IcebergMetadataCacheMgr --> InvalidateDb
    IcebergMetadataCacheMgr --> InvalidateCatalog
    
    InvalidateTable --> ClearTableCache
    InvalidateTable --> ClearSnapshotCache
    InvalidateTable --> ClearSnapshotListCache
    InvalidateTable --> ClearViewCache
    InvalidateTable --> DropManifestCache
    InvalidateTable --> ClearSchemaCache
    
    InvalidateDb --> ClearTableCache
    InvalidateDb --> ClearSnapshotCache
    InvalidateDb --> ClearSnapshotListCache
    InvalidateDb --> ClearViewCache
    InvalidateDb --> DropManifestCache
    InvalidateDb --> ClearSchemaCache
    
    InvalidateCatalog --> ClearTableCache
    InvalidateCatalog --> ClearSnapshotCache
    InvalidateCatalog --> ClearSnapshotListCache
    InvalidateCatalog --> ClearViewCache
    InvalidateCatalog --> DropManifestCache
    InvalidateCatalog --> ClearSchemaCache
```

## 6. 缓存键值关系图

```mermaid
classDiagram
    class IcebergMetadataCacheKey {
        +NameMapping nameMapping
        +equals()
        +hashCode()
    }
    
    class NameMapping {
        +long ctlId
        +String localDbName
        +String localTblName
        +String remoteDbName
        +String remoteTblName
    }
    
    class IcebergSchemaCacheKey {
        +NameMapping nameMapping
        +long schemaId
        +equals()
        +hashCode()
    }
    
    class IcebergSnapshotCacheValue {
        +IcebergPartitionInfo partitionInfo
        +IcebergSnapshot snapshot
    }
    
    class IcebergSchemaCacheValue {
        +List~Column~ schema
        +List~Column~ partitionColumns
    }
    
    class IcebergPartitionInfo {
        +Map~String, PartitionItem~ nameToPartitionItem
        +Map~String, IcebergPartition~ nameToPartition
        +Map~String, Set~String~~ partitionNameMap
    }
    
    class IcebergSnapshot {
        +long snapshotId
        +long schemaId
    }
    
    IcebergMetadataCacheKey *-- NameMapping
    IcebergSchemaCacheKey --|> SchemaCacheKey
    IcebergSchemaCacheKey *-- NameMapping
    IcebergSnapshotCacheValue *-- IcebergPartitionInfo
    IcebergSnapshotCacheValue *-- IcebergSnapshot
    IcebergSchemaCacheValue --|> SchemaCacheValue
```

## 7. 缓存配置和线程池

```mermaid
graph TB
    subgraph "配置参数"
        ExpireTime[external_cache_expire_time_seconds_after_access<br/>默认: 86400s 24h]
        RefreshTime[external_cache_refresh_time_minutes<br/>默认: 60min]
        MaxSize[max_external_table_cache_num<br/>默认: 50000]
        ThreadPoolSize[max_external_cache_loader_thread_pool_size<br/>默认: 10]
    end
    
    subgraph "线程池"
        CommonExecutor[commonRefreshExecutor<br/>固定线程池<br/>队列: pool_size * 10000]
        RowCountExecutor[rowCountRefreshExecutor<br/>行数统计专用]
        FileListingExecutor[fileListingExecutor<br/>文件列举专用]
    end
    
    subgraph "缓存实例"
        TableCache[tableCache]
        SnapshotCache[snapshotCache]
        SnapshotListCache[snapshotListCache]
        ViewCache[viewCache]
        SchemaCache[schemaCache]
    end
    
    ExpireTime --> TableCache
    ExpireTime --> SnapshotCache
    ExpireTime --> SnapshotListCache
    ExpireTime --> ViewCache
    ExpireTime --> SchemaCache
    
    RefreshTime --> TableCache
    RefreshTime --> SnapshotCache
    RefreshTime --> SnapshotListCache
    RefreshTime --> ViewCache
    RefreshTime --> SchemaCache
    
    MaxSize --> TableCache
    MaxSize --> SnapshotCache
    MaxSize --> SnapshotListCache
    MaxSize --> ViewCache
    MaxSize --> SchemaCache
    
    ThreadPoolSize --> CommonExecutor
    CommonExecutor --> TableCache
    CommonExecutor --> SnapshotCache
    CommonExecutor --> SnapshotListCache
    CommonExecutor --> ViewCache
    CommonExecutor --> SchemaCache
```

## 8. 查询执行中的缓存使用

```mermaid
graph LR
    subgraph "查询规划阶段"
        A[查询解析]
        B[获取表元数据]
        C[获取 Schema]
        D[获取统计信息]
    end
    
    subgraph "缓存访问"
        E[IcebergMetadataCache<br/>getIcebergTable]
        F[ExternalSchemaCache<br/>getSchemaValue]
        G[IcebergUtils<br/>getIcebergRowCount]
    end
    
    subgraph "扫描执行阶段"
        H[IcebergScanNode 初始化]
        I[创建 IcebergSource]
        J[Snapshot 选择]
        K[分区裁剪]
        L[生成 Split]
    end
    
    A --> B
    B --> E
    B --> C
    C --> F
    B --> D
    D --> G
    
    E --> H
    F --> H
    H --> I
    I --> J
    J --> K
    K --> L
```

## 9. 重构优化方向

```mermaid
graph TB
    subgraph "当前架构问题"
        P1[缓存粒度粗<br/>整个 Table 对象]
        P2[Schema 与 Table 分离<br/>两次查询]
        P3[分区加载低效<br/>扫描元数据表]
        P4[失效策略粗暴<br/>全量清空]
    end
    
    subgraph "优化方向"
        O1[分层缓存<br/>Metadata/Snapshot/Schema/Partition]
        O2[增量更新<br/>基于 transaction log]
        O3[按需加载<br/>字段级懒加载]
        O4[统一缓存<br/>Schema 合并到 Table]
        O5[Manifest 级缓存<br/>优化分区信息]
        O6[监控增强<br/>详细指标统计]
    end
    
    P1 --> O1
    P1 --> O3
    P2 --> O4
    P3 --> O5
    P4 --> O2
    
    O1 --> Future[未来架构]
    O2 --> Future
    O3 --> Future
    O4 --> Future
    O5 --> Future
    O6 --> Future
```

## 10. 典型使用场景时序图

### 场景 1: 首次查询 Iceberg 表

```mermaid
sequenceDiagram
    participant User as 用户
    participant FE as Frontend
    participant TableCache as Table 缓存
    participant SchemaCache as Schema 缓存
    participant Ops as MetadataOps
    participant HMS as HMS/REST

    User->>FE: SELECT * FROM iceberg_table
    FE->>TableCache: getIcebergTable(table)
    Note over TableCache: 缓存未命中
    TableCache->>Ops: loadTable(dbName, tblName)
    Ops->>HMS: loadTable(identifier)
    HMS-->>Ops: Table 对象
    Ops-->>TableCache: Table 对象
    TableCache-->>FE: Table 对象 [已缓存]
    
    FE->>SchemaCache: getSchemaValue(key)
    Note over SchemaCache: 缓存未命中
    SchemaCache->>FE: loadSchema(key)
    FE->>TableCache: 使用已缓存的 Table
    FE->>FE: 解析 Schema
    FE-->>SchemaCache: Schema 对象
    SchemaCache-->>FE: Schema 对象 [已缓存]
    
    FE->>FE: 规划查询
    FE-->>User: 查询结果
```

### 场景 2: 后续查询（缓存命中）

```mermaid
sequenceDiagram
    participant User as 用户
    participant FE as Frontend
    participant TableCache as Table 缓存
    participant SchemaCache as Schema 缓存

    User->>FE: SELECT * FROM iceberg_table
    FE->>TableCache: getIcebergTable(table)
    Note over TableCache: 缓存命中
    TableCache-->>FE: Table 对象
    
    FE->>SchemaCache: getSchemaValue(key)
    Note over SchemaCache: 缓存命中
    SchemaCache-->>FE: Schema 对象
    
    FE->>FE: 规划查询
    FE-->>User: 查询结果
    Note over FE,User: 无远程元数据访问<br/>性能最优
```

### 场景 3: 表结构变更后刷新

```mermaid
sequenceDiagram
    participant User as 用户
    participant FE as Frontend
    participant CacheMgr as CacheMgr
    participant TableCache as Table 缓存
    participant SchemaCache as Schema 缓存
    participant HMS as HMS

    Note over HMS: 外部修改表结构
    
    User->>FE: REFRESH TABLE iceberg_table
    FE->>CacheMgr: invalidateTableCache(table)
    CacheMgr->>TableCache: invalidate(key)
    CacheMgr->>SchemaCache: invalidate(key)
    Note over TableCache,SchemaCache: 清空缓存
    CacheMgr-->>FE: 完成
    FE-->>User: 刷新成功
    
    User->>FE: SELECT * FROM iceberg_table
    Note over FE,HMS: 重新加载元数据<br/>参考场景 1
```

---

**说明**:
- 这些图表使用 Mermaid 语法，可以在支持 Mermaid 的 Markdown 渲染器中显示
- 推荐使用 GitHub、GitLab 或支持 Mermaid 的 Markdown 编辑器查看
- 可以复制到在线 Mermaid 编辑器（如 https://mermaid.live/）查看效果

