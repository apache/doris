# Apache Doris + Apache Fluss Integration - Implementation Strategy

## Executive Summary

This document outlines the production-grade implementation strategy for integrating Apache Fluss (streaming storage) with Apache Doris (OLAP engine). The integration enables real-time analytics by allowing Doris to read data from Fluss tables.

---

## 1. Fluss Data Model & Tiered Storage

### 1.1 Understanding Fluss Storage Tiers

Fluss uses a **tiered storage model** with two distinct layers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FLUSS TIERED STORAGE MODEL                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌────────────────────────────────────────────────────────────────────┐   │
│   │                      LOG TIER (Real-time)                           │   │
│   │  ┌──────────────────────────────────────────────────────────────┐  │   │
│   │  │  • Native Fluss format (Arrow-based)                         │  │   │
│   │  │  • Sub-second latency writes                                 │  │   │
│   │  │  • Append-only log per bucket                                │  │   │
│   │  │  • Requires Fluss SDK to read                                │  │   │
│   │  │  • Data: offset > lakeSnapshotOffset                         │  │   │
│   │  └──────────────────────────────────────────────────────────────┘  │   │
│   └────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    │ Tiering/Compaction                     │
│                                    ▼                                        │
│   ┌────────────────────────────────────────────────────────────────────┐   │
│   │                      LAKE TIER (Batch)                              │   │
│   │  ┌──────────────────────────────────────────────────────────────┐  │   │
│   │  │  • Parquet/ORC files (via Paimon/Iceberg)                    │  │   │
│   │  │  • Compacted, optimized for analytics                        │  │   │
│   │  │  • Standard file formats - direct read possible              │  │   │
│   │  │  • Data: offset <= lakeSnapshotOffset                        │  │   │
│   │  │  • Files stored on Apache Ozone/S3/HDFS                       │  │   │
│   │  └──────────────────────────────────────────────────────────────┘  │   │
│   └────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Key Metadata Structures

```java
// LakeSnapshot - tells us what data is in lake tier
class LakeSnapshot {
    long snapshotId;                              // Lake snapshot ID
    Map<TableBucket, Long> tableBucketsOffset;    // Per-bucket: max log offset in lake
}

// For each bucket, we can determine:
// - Lake data:  offset <= tableBucketsOffset[bucket]  → Read Parquet directly
// - Log data:   offset > tableBucketsOffset[bucket]   → Read via Fluss SDK
```

### 1.3 Split Types for Tiered Reading

| Split Type | Data Source | Reader | Use Case |
|------------|-------------|--------|----------|
| **LakeSnapshotSplit** | Parquet files only | Native Parquet reader | Historical data queries |
| **LogSplit** | Fluss log only | Fluss SDK (JNI) | Real-time streaming |
| **LakeSnapshotAndFlussLogSplit** | Both tiers | Hybrid reader | Complete table scan |

---

## 2. Current Implementation Status

### 2.1 Implemented Components (Feature Branch: `feature/fluss-table-integration`)

| Component | Status | Description |
|-----------|--------|-------------|
| **FlussExternalCatalog** | ✅ Complete | Catalog management with connection pooling, retry logic |
| **FlussExternalTable** | ✅ Complete | Table abstraction with Thrift serialization |
| **FlussExternalDatabase** | ✅ Complete | Database namespace management |
| **FlussMetadataOps** | ✅ Complete | Metadata operations with caching and retry |
| **FlussScanNode** | ⚠️ Partial | Query planning - needs tiered split generation |
| **FlussSplit** | ⚠️ Partial | Split representation - needs tier information |
| **FlussSource** | ✅ Complete | Source abstraction for table access |
| **FlussUtils** | ✅ Complete | Type mapping Fluss → Doris |
| **Thrift Definitions** | ⚠️ Partial | TFlussTable, TFlussFileDesc - needs tier fields |
| **BE FlussReader** | ⚠️ Skeleton | Needs tiered reader implementation |
| **Unit Tests** | ✅ Partial | Basic tests for catalog, metadata, utils |

### 2.2 Architecture Gaps for MVP

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           GAP ANALYSIS                                   │
├─────────────────────────────────────────────────────────────────────────┤
│ 1. FE: No LakeSnapshot metadata fetching                                 │
│ 2. FE: FlussScanNode doesn't generate tiered splits                      │
│ 3. FE: FlussSplit doesn't carry tier/file information                    │
│ 4. BE: No Parquet reader for lake files                                  │
│ 5. BE: No JNI bridge for Fluss log reads (Phase 2)                       │
│ 6. Thrift: Missing tier-specific fields in TFlussFileDesc                │
│ 7. No integration tests with tiered data scenarios                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. MVP Query Execution Flow

### 3.1 End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         MVP QUERY EXECUTION FLOW                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ STEP 1: FE - Fetch Metadata via Fluss Java SDK                           │   │
│  │ ─────────────────────────────────────────────────────────────────────── │   │
│  │  • Get TableInfo (schema, partitions, buckets)                          │   │
│  │  • Get LakeSnapshot (snapshotId, tableBucketsOffset)                    │   │
│  │  • Get LakeSplits (Parquet file paths from Paimon/Iceberg)              │   │
│  └───────────────────────────────────┬─────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ STEP 2: FE - Determine Data Tiers per Split                              │   │
│  │ ─────────────────────────────────────────────────────────────────────── │   │
│  │  For each bucket:                                                        │   │
│  │    lakeOffset = lakeSnapshot.tableBucketsOffset[bucket]                  │   │
│  │    currentOffset = getLatestLogOffset(bucket)                            │   │
│  │                                                                          │   │
│  │    if (lakeOffset exists && lakeSplits exist):                           │   │
│  │       → Generate LAKE_SPLIT with Parquet file paths                      │   │
│  │    if (currentOffset > lakeOffset):                                      │   │
│  │       → Generate LOG_SPLIT with offset range [lakeOffset, currentOffset] │   │
│  │    if (both):                                                            │   │
│  │       → Generate HYBRID_SPLIT                                            │   │
│  └───────────────────────────────────┬─────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ STEP 3: FE - Generate Execution Plan & Distribute to BEs                 │   │
│  │ ─────────────────────────────────────────────────────────────────────── │   │
│  │  • Create FlussSplit objects with tier information                       │   │
│  │  • Serialize via Thrift (TFlussFileDesc with tier fields)                │   │
│  │  • Distribute splits across BE nodes based on locality/load              │   │
│  └───────────────────────────────────┬─────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ STEP 4: BE - Process Splits Based on Tier                                │   │
│  │ ─────────────────────────────────────────────────────────────────────── │   │
│  │  ┌───────────────────────┐  ┌───────────────────────┐                   │   │
│  │  │   LAKE_SPLIT          │  │   LOG_SPLIT (Phase 2) │                   │   │
│  │  │   ───────────         │  │   ──────────          │                   │   │
│  │  │   • Read Parquet      │  │   • JNI → Fluss SDK   │                   │   │
│  │  │   • Native C++ reader │  │   • Stream log data   │                   │   │
│  │  │   • Direct S3/HDFS    │  │   • Apply projection  │                   │   │
│  │  └───────────┬───────────┘  └───────────┬───────────┘                   │   │
│  │              │                           │                               │   │
│  │              └─────────────┬─────────────┘                               │   │
│  │                            ▼                                              │   │
│  │                    Vectorized Batches                                     │   │
│  └───────────────────────────────────┬─────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ STEP 5: BE - Shuffle, Aggregate & Return Results                         │   │
│  │ ─────────────────────────────────────────────────────────────────────── │   │
│  │  • Apply predicates and projections                                      │   │
│  │  • Execute aggregations/joins                                            │   │
│  │  • Shuffle data between BEs if needed                                    │   │
│  │  • Return final results to FE → Client                                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 FlussSplit Tier Types

```java
public enum FlussSplitTier {
    LAKE_ONLY,      // Data only in lake (Parquet) - MVP Phase 1
    LOG_ONLY,       // Data only in log (Fluss SDK) - Phase 2
    HYBRID          // Data in both tiers - Phase 2
}

public class FlussSplit {
    // Existing fields
    String databaseName;
    String tableName;
    long tableId;
    int bucketId;
    String partitionName;
    String bootstrapServers;
    
    // NEW: Tier information
    FlussSplitTier tier;
    
    // NEW: Lake tier fields (for LAKE_ONLY and HYBRID)
    List<String> lakeFilePaths;    // Parquet file URIs
    String lakeFormat;              // "parquet" or "orc"
    long lakeSnapshotId;
    
    // NEW: Log tier fields (for LOG_ONLY and HYBRID)
    long logStartOffset;            // Starting log offset
    long logEndOffset;              // Ending log offset (-1 for unbounded)
}
```

### 3.3 Thrift Definition Updates

```thrift
enum TFlussSplitTier {
    LAKE_ONLY = 0,
    LOG_ONLY = 1,
    HYBRID = 2
}

struct TFlussFileDesc {
    // Existing fields
    1: optional string database_name
    2: optional string table_name
    3: optional i64 table_id
    4: optional i32 bucket_id
    5: optional string partition_name
    6: optional i64 snapshot_id
    7: optional string bootstrap_servers
    8: optional string file_format
    
    // NEW: Tier information
    10: optional TFlussSplitTier tier
    
    // NEW: Lake tier fields
    11: optional list<string> lake_file_paths
    12: optional i64 lake_snapshot_id
    
    // NEW: Log tier fields
    13: optional i64 log_start_offset
    14: optional i64 log_end_offset
}
```

---

## 4. Target Architecture (Production-Grade)

### 4.1 High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         DORIS-FLUSS INTEGRATION                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                          DORIS FE (Java)                                 │ │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │ │
│  │  │FlussExternalCatalog│  │FlussMetadataOps │  │ FlussScanNode       │  │ │
│  │  │- Connection pool  │  │- Cache + TTL    │  │ - Split planning    │  │ │
│  │  │- Health checks    │  │- Schema sync    │  │ - Predicate pushdown│  │ │
│  │  │- Circuit breaker  │  │- Retry logic    │  │ - Projection        │  │ │
│  │  └────────┬─────────┘  └────────┬────────┘  └──────────┬──────────┘  │ │
│  │           │                      │                       │             │ │
│  │           └──────────────────────┼───────────────────────┘             │ │
│  │                                  │                                      │ │
│  └──────────────────────────────────┼──────────────────────────────────────┘ │
│                                     │ Thrift RPC                             │
│  ┌──────────────────────────────────┼──────────────────────────────────────┐ │
│  │                          DORIS BE (C++)                                  │ │
│  │                                  │                                       │ │
│  │  ┌──────────────────────────────┴─────────────────────────────────────┐ │ │
│  │  │                        FlussReader                                  │ │ │
│  │  │  Option A: JNI Bridge to Fluss Java Client                         │ │ │
│  │  │  Option B: Read Fluss Lake (Paimon) files directly                 │ │ │
│  │  │  Option C: HTTP/gRPC proxy service                                 │ │ │
│  │  └────────────────────────────────────────────────────────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
│                                     │                                         │
└─────────────────────────────────────┼─────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              FLUSS CLUSTER                                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐ │
│  │  Coordinator    │  │  TabletServer   │  │  Lake Storage (Paimon/Iceberg) │ │
│  │  - Metadata     │  │  - Log storage  │  │  - Parquet/ORC files           │ │
│  │  - Scheduling   │  │  - KV storage   │  │  - Snapshots                   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow for Read Operations

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                              READ DATA FLOW                                     │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   User Query                                                                    │
│       │                                                                         │
│       ▼                                                                         │
│   ┌───────────────────────────────────────────────────────────────────────┐   │
│   │ 1. FE: Parse & Analyze Query                                           │   │
│   │    - Identify Fluss catalog/table                                      │   │
│   │    - Load schema from FlussMetadataOps (cached)                        │   │
│   └───────────────────────────────────────────────────────────────────────┘   │
│       │                                                                         │
│       ▼                                                                         │
│   ┌───────────────────────────────────────────────────────────────────────┐   │
│   │ 2. FE: Plan Generation (FlussScanNode)                                 │   │
│   │    - Get table snapshot from Fluss                                     │   │
│   │    - Generate FlussSplit per bucket/partition                          │   │
│   │    - Apply predicate/projection pushdown                               │   │
│   └───────────────────────────────────────────────────────────────────────┘   │
│       │                                                                         │
│       ▼                                                                         │
│   ┌───────────────────────────────────────────────────────────────────────┐   │
│   │ 3. FE→BE: Distribute Splits via Thrift                                 │   │
│   │    - TFlussFileDesc contains: table_id, bucket_id, snapshot_id,        │   │
│   │      bootstrap_servers, partition_name                                 │   │
│   └───────────────────────────────────────────────────────────────────────┘   │
│       │                                                                         │
│       ▼                                                                         │
│   ┌───────────────────────────────────────────────────────────────────────┐   │
│   │ 4. BE: Execute Scan (FlussReader)                                      │   │
│   │    MVP: Read from Fluss lake storage (Parquet/ORC)                     │   │
│   │    Future: Direct Fluss log/KV reads via JNI                           │   │
│   └───────────────────────────────────────────────────────────────────────┘   │
│       │                                                                         │
│       ▼                                                                         │
│   ┌───────────────────────────────────────────────────────────────────────┐   │
│   │ 5. BE→FE: Return Results                                               │   │
│   │    - Vectorized column batches                                         │   │
│   │    - Statistics for query optimization                                 │   │
│   └───────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. MVP Scope (Phase 1)

### 3.1 MVP Goal
**Enable Doris to read batch data from Fluss tables via lake storage (Paimon).**

### 3.2 MVP Features

| Feature | Priority | Description |
|---------|----------|-------------|
| Create Fluss Catalog | P0 | `CREATE CATALOG fluss_cat PROPERTIES (...)` |
| List Databases/Tables | P0 | `SHOW DATABASES`, `SHOW TABLES` |
| Describe Table | P0 | `DESC table_name` with accurate schema |
| SELECT Query | P0 | Basic SELECT with filtering |
| Predicate Pushdown | P1 | Push filters to reduce data scan |
| Column Projection | P1 | Read only required columns |
| Snapshot Reads | P1 | Read from specific snapshot ID |

### 3.3 MVP Architecture Decision: Lake Storage Path

For MVP, we read from **Fluss Lake Storage** (Parquet files managed by Paimon):

```
Fluss Table
    │
    ├── Log Storage (real-time, append-only)
    │   └── NOT used in MVP
    │
    └── Lake Storage (batch, compacted) ◄── MVP PATH
        └── Parquet/ORC files on S3/HDFS
            └── Read by Doris BE (native readers)
```

**Rationale:**
- Doris BE already has production-grade Parquet/ORC readers
- No JNI complexity or additional dependencies
- Consistent with Paimon/Iceberg patterns in Doris
- Sub-second latency not required for MVP (batch analytics)

---

## 4. Implementation Plan

### Phase 1: MVP - Lake Storage Reads (4-6 weeks)

#### Week 1-2: Complete FE Integration

```
Tasks:
├── 1.1 Fix FlussMetadataOps.getTableSchema()
│   └── Currently returns empty list, need to fetch actual schema
│
├── 1.2 Implement snapshot file listing
│   └── FlussScanNode.getSnapshotFiles() → list Parquet files
│
├── 1.3 Enhance FlussSplit with file paths
│   └── Add lakePath, fileSize, rowCount
│
└── 1.4 Unit tests for schema/split generation
```

#### Week 3-4: Complete BE Integration

```
Tasks:
├── 2.1 Implement FlussReader for Parquet
│   └── Use existing ParquetReader with Fluss metadata
│
├── 2.2 Wire FE→BE Thrift communication
│   └── Pass lake file paths, not just bucket IDs
│
├── 2.3 Handle Fluss-specific schema mapping
│   └── Ensure type conversion works end-to-end
│
└── 2.4 Unit tests for BE reader
```

#### Week 5-6: Integration Testing & Hardening

```
Tasks:
├── 3.1 Docker-based integration test suite
│   └── Fluss + Doris containers with test data
│
├── 3.2 Regression test suite (Groovy)
│   └── Follow Paimon test patterns
│
├── 3.3 Error handling & retry logic
│   └── Connection failures, timeout handling
│
└── 3.4 Documentation & examples
```

### Phase 2: Production Hardening (4 weeks)

```
├── Observability
│   ├── Metrics: scan latency, rows read, errors
│   ├── Tracing: distributed trace IDs
│   └── Logging: structured logs with context
│
├── Performance
│   ├── Connection pooling optimization
│   ├── Metadata cache tuning
│   └── Parallel split execution
│
├── Reliability
│   ├── Circuit breaker for Fluss failures
│   ├── Graceful degradation
│   └── Health check endpoints
│
└── Security
    ├── SASL/SSL authentication
    ├── ACL integration
    └── Audit logging
```

### Phase 3: Advanced Features (6-8 weeks)

```
├── Log Scanner (real-time reads)
│   └── JNI bridge to Fluss Java client
│
├── Primary Key Lookups
│   └── Point queries via Fluss KV store
│
├── Write Support
│   └── INSERT INTO fluss_table SELECT ...
│
└── Time Travel
    └── Query historical snapshots
```

---

## 5. Testing Strategy

### 5.1 Test Pyramid

```
                    ┌──────────────┐
                    │   E2E Tests  │  ← 10%
                    │  (Manual/CI) │
                    └──────┬───────┘
                           │
                ┌──────────┴──────────┐
                │  Integration Tests  │  ← 30%
                │  (Docker + Groovy)  │
                └──────────┬──────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │           Unit Tests                 │  ← 60%
        │  (JUnit/Mockito for FE, GTest for BE)│
        └──────────────────────────────────────┘
```

### 5.2 Unit Tests

**FE Unit Tests** (JUnit + Mockito):

```java
// FlussExternalCatalogTest.java
@Test void testCreateCatalogWithBootstrapServers()
@Test void testCheckPropertiesMissingBootstrapServers()
@Test void testCatalogSecurityProperties()
@Test void testCacheTtlProperty()

// FlussMetadataOpsTest.java
@Test void testTableExist()
@Test void testTableNotExist()
@Test void testListTableNames()
@Test void testGetTableInfo()
@Test void testRetryOnTransientFailure()
@Test void testCacheInvalidation()

// FlussUtilsTest.java
@Test void testPrimitiveTypes()
@Test void testComplexTypes()
@Test void testDecimalType()
@Test void testTimestampTypes()

// FlussScanNodeTest.java
@Test void testSplitGeneration()
@Test void testPredicatePushdown()
@Test void testProjection()
@Test void testPartitionPruning()

// FlussSplitTest.java
@Test void testSplitSerialization()
@Test void testConsistentHashString()
```

**BE Unit Tests** (GTest):

```cpp
// fluss_reader_test.cpp
TEST_F(FlussReaderTest, InitReader)
TEST_F(FlussReaderTest, GetNextBlock)
TEST_F(FlussReaderTest, HandleEmptyTable)
TEST_F(FlussReaderTest, TypeConversion)
```

### 5.3 Integration Tests

**Docker Compose Setup:**

```yaml
# docker/integration-test/docker-compose.yml
services:
  zookeeper:
    image: zookeeper:3.8
    
  fluss-coordinator:
    image: fluss/fluss:latest
    command: coordinator
    depends_on: [zookeeper]
    
  fluss-tablet-server:
    image: fluss/fluss:latest
    command: tablet-server
    depends_on: [fluss-coordinator]
    
  minio:
    image: minio/minio:latest
    command: server /data
    
  doris-fe:
    image: apache/doris:latest
    
  doris-be:
    image: apache/doris:latest
```

**Groovy Test Suite:**

```groovy
// regression-test/suites/external_table_p0/fluss/test_fluss_catalog.groovy
suite("test_fluss_catalog", "p0,external,fluss") {
    
    String catalog_name = "fluss_test_catalog"
    String bootstrap_servers = context.config.otherConfigs.get("flussBootstrapServers")
    
    // Test: Create catalog
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            "type" = "fluss",
            "bootstrap.servers" = "${bootstrap_servers}"
        );
    """
    
    // Test: List databases
    def dbs = sql """SHOW DATABASES FROM ${catalog_name}"""
    assertTrue(dbs.size() > 0)
    
    // Test: List tables
    sql """USE ${catalog_name}.test_db"""
    def tables = sql """SHOW TABLES"""
    assertTrue(tables.contains("test_table"))
    
    // Test: Describe table
    def schema = sql """DESC test_table"""
    assertEquals("id", schema[0][0])
    assertEquals("INT", schema[0][1])
    
    // Test: Select query
    def result = sql """SELECT * FROM test_table WHERE id > 0 LIMIT 10"""
    assertTrue(result.size() > 0)
    
    // Test: Predicate pushdown
    explain {
        sql """SELECT * FROM test_table WHERE id = 1"""
        contains "FLUSS_SCAN_NODE"
        contains "predicates: id = 1"
    }
    
    // Cleanup
    sql """DROP CATALOG ${catalog_name}"""
}
```

### 5.4 Test Data Setup

```sql
-- Fluss SQL (via Flink SQL client)
CREATE DATABASE test_db;

CREATE TABLE test_db.test_table (
    id INT PRIMARY KEY,
    name STRING,
    value DOUBLE,
    ts TIMESTAMP(3)
) WITH (
    'bucket.num' = '4'
);

-- Insert test data
INSERT INTO test_db.test_table VALUES
    (1, 'alice', 100.0, TIMESTAMP '2024-01-01 00:00:00'),
    (2, 'bob', 200.0, TIMESTAMP '2024-01-02 00:00:00'),
    (3, 'charlie', 300.0, TIMESTAMP '2024-01-03 00:00:00');
```

---

## 6. Distributed Systems Patterns

### 6.1 Connection Management

```java
public class FlussConnectionPool {
    private final ConcurrentHashMap<String, Connection> connections;
    private final ScheduledExecutorService healthChecker;
    private final CircuitBreaker circuitBreaker;
    
    // Pattern: Connection pooling with health checks
    public Connection getConnection(String bootstrapServers) {
        return connections.computeIfAbsent(bootstrapServers, this::createConnection);
    }
    
    // Pattern: Circuit breaker for failure isolation
    public <T> T execute(Supplier<T> operation) {
        return circuitBreaker.execute(operation);
    }
    
    // Pattern: Exponential backoff retry
    private Connection createConnectionWithRetry(String servers) {
        return RetryUtil.withExponentialBackoff(
            () -> ConnectionFactory.createConnection(config),
            MAX_RETRIES, INITIAL_DELAY_MS, MAX_DELAY_MS
        );
    }
}
```

### 6.2 Metadata Caching

```java
public class FlussMetadataCache {
    private final LoadingCache<TablePath, TableInfo> tableInfoCache;
    private final LoadingCache<String, List<String>> databaseTablesCache;
    
    public FlussMetadataCache(FlussExternalCatalog catalog) {
        this.tableInfoCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(Duration.ofMinutes(5))
            .refreshAfterWrite(Duration.ofMinutes(1))
            .recordStats()  // For observability
            .build(new CacheLoader<>() {
                @Override
                public TableInfo load(TablePath path) {
                    return catalog.getFlussAdmin().getTableInfo(path).get();
                }
            });
    }
    
    // Pattern: Read-through cache with async refresh
    public TableInfo getTableInfo(TablePath path) {
        return tableInfoCache.get(path);
    }
    
    // Pattern: Selective invalidation
    public void invalidate(TablePath path) {
        tableInfoCache.invalidate(path);
    }
}
```

### 6.3 Split Generation (Horizontal Scaling)

```java
public class FlussSplitGenerator {
    
    // Pattern: Partition-aware split generation for parallelism
    public List<FlussSplit> generateSplits(FlussExternalTable table, int numBackends) {
        List<FlussSplit> splits = new ArrayList<>();
        
        TableInfo tableInfo = table.getTableInfo();
        int numBuckets = tableInfo.getNumBuckets();
        List<String> partitions = tableInfo.getPartitionKeys().isEmpty() 
            ? Collections.singletonList(null) 
            : getPartitions(table);
        
        // Generate one split per bucket per partition
        for (String partition : partitions) {
            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                splits.add(new FlussSplit(
                    table.getDbName(),
                    table.getName(),
                    tableInfo.getTableId(),
                    bucketId,
                    partition,
                    getLatestSnapshotId(table),
                    table.getBootstrapServers()
                ));
            }
        }
        
        // Pattern: Adaptive split sizing based on backend count
        return balanceSplits(splits, numBackends);
    }
}
```

### 6.4 Error Handling

```java
public class FlussOperationExecutor {
    
    // Pattern: Categorized exception handling
    public <T> T executeWithRetry(Supplier<T> operation, String operationName) {
        int attempt = 0;
        Exception lastException = null;
        
        while (attempt < MAX_RETRIES) {
            try {
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                
                if (isNonRetryable(e)) {
                    throw new FlussException("Non-retryable error: " + operationName, e);
                }
                
                if (isTransient(e)) {
                    attempt++;
                    long delay = calculateBackoff(attempt);
                    LOG.warn("Transient failure for {}, retry {}/{} after {}ms",
                        operationName, attempt, MAX_RETRIES, delay);
                    Thread.sleep(delay);
                } else {
                    throw new FlussException("Unexpected error: " + operationName, e);
                }
            }
        }
        
        throw new FlussException("Max retries exceeded for " + operationName, lastException);
    }
    
    private boolean isTransient(Exception e) {
        return e instanceof TimeoutException
            || e instanceof ConnectionException
            || e.getMessage().contains("unavailable");
    }
    
    private boolean isNonRetryable(Exception e) {
        return e instanceof TableNotExistException
            || e instanceof AuthenticationException
            || e instanceof SchemaException;
    }
}
```

---

## 7. Observability

### 7.1 Metrics

```java
// FE Metrics
public class FlussMetrics {
    // Connection metrics
    private final Counter connectionAttempts;
    private final Counter connectionFailures;
    private final Gauge activeConnections;
    
    // Operation metrics
    private final Histogram scanLatency;
    private final Counter rowsRead;
    private final Counter splitsGenerated;
    
    // Cache metrics
    private final Gauge cacheHitRate;
    private final Counter cacheEvictions;
    
    public void recordScanLatency(long durationMs) {
        scanLatency.observe(durationMs);
    }
}
```

### 7.2 Logging

```java
// Structured logging with MDC
public class FlussLogger {
    
    public void logScanStart(String catalogName, String tableName, int numSplits) {
        MDC.put("catalog", catalogName);
        MDC.put("table", tableName);
        MDC.put("operation", "scan");
        LOG.info("Starting Fluss scan with {} splits", numSplits);
    }
    
    public void logScanComplete(long rowsRead, long durationMs) {
        LOG.info("Fluss scan completed: rows={}, duration={}ms", rowsRead, durationMs);
        MDC.clear();
    }
}
```

### 7.3 Health Checks

```java
public class FlussHealthChecker implements HealthCheck {
    
    @Override
    public HealthStatus check() {
        try {
            // Check coordinator connectivity
            admin.listDatabases().get(5, TimeUnit.SECONDS);
            return HealthStatus.healthy("Fluss cluster is reachable");
        } catch (TimeoutException e) {
            return HealthStatus.unhealthy("Fluss coordinator timeout");
        } catch (Exception e) {
            return HealthStatus.unhealthy("Fluss cluster unreachable: " + e.getMessage());
        }
    }
}
```

---

## 8. SLIs/SLOs

### 8.1 Service Level Indicators

| SLI | Description | Measurement |
|-----|-------------|-------------|
| **Availability** | Catalog operations succeed | Success rate of SHOW/DESC commands |
| **Latency** | Query response time | P50, P95, P99 scan latency |
| **Throughput** | Data read rate | Rows/second, MB/second |
| **Error Rate** | Failed operations | Errors per 1000 operations |

### 8.2 Service Level Objectives (MVP)

| SLO | Target | Measurement Window |
|-----|--------|-------------------|
| Catalog availability | 99.5% | Rolling 7 days |
| Metadata query latency (P95) | < 500ms | Rolling 1 hour |
| Scan query latency (P95) | < 30s for 1GB | Per query |
| Error rate | < 0.1% | Rolling 1 hour |

---

## 9. Security Considerations

### 9.1 Authentication

```sql
-- SASL/PLAIN authentication
CREATE CATALOG secure_fluss PROPERTIES (
    "type" = "fluss",
    "bootstrap.servers" = "fluss-coordinator:9123",
    "fluss.security.protocol" = "SASL_PLAINTEXT",
    "fluss.sasl.mechanism" = "PLAIN",
    "fluss.sasl.username" = "doris_user",
    "fluss.sasl.password" = "***"
);

-- SSL/TLS encryption
CREATE CATALOG secure_fluss_ssl PROPERTIES (
    "type" = "fluss",
    "bootstrap.servers" = "fluss-coordinator:9123",
    "fluss.security.protocol" = "SSL",
    "fluss.ssl.truststore.location" = "/path/to/truststore.jks",
    "fluss.ssl.truststore.password" = "***"
);
```

### 9.2 Authorization

```
Doris RBAC → Fluss ACLs mapping (future phase)
- GRANT SELECT ON fluss_catalog.* TO user
- Maps to Fluss table-level read permissions
```

---

## 10. Operational Runbook

### 10.1 Common Issues

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| Connection timeout | `TimeoutException` in logs | Check network, increase timeout |
| Schema mismatch | `Column not found` errors | Refresh catalog: `REFRESH CATALOG` |
| Stale metadata | Old table structure | `INVALIDATE METADATA fluss_cat.db.table` |
| OOM on large scan | BE memory exhaustion | Reduce `file_split_size`, add filters |

### 10.2 Monitoring Queries

```sql
-- Check catalog health
SHOW CATALOGS;
SHOW DATABASES FROM fluss_catalog;

-- Check table metadata
DESC fluss_catalog.db.table;
SHOW TABLE STATUS FROM fluss_catalog.db;

-- Analyze query plan
EXPLAIN SELECT * FROM fluss_catalog.db.table WHERE id > 100;
```

---

## 11. File Structure

```
doris/
├── fe/fe-core/src/main/java/org/apache/doris/datasource/fluss/
│   ├── FlussExternalCatalog.java          # Catalog management
│   ├── FlussExternalCatalogFactory.java   # Catalog factory
│   ├── FlussExternalDatabase.java         # Database abstraction
│   ├── FlussExternalTable.java            # Table abstraction
│   ├── FlussMetadataOps.java              # Metadata operations
│   ├── FlussUtils.java                    # Type mapping utilities
│   └── source/
│       ├── FlussScanNode.java             # Query plan node
│       ├── FlussSplit.java                # Split definition
│       └── FlussSource.java               # Source abstraction
│
├── fe/fe-core/src/test/java/org/apache/doris/datasource/fluss/
│   ├── FlussExternalCatalogTest.java
│   ├── FlussExternalTableTest.java
│   ├── FlussMetadataOpsTest.java
│   ├── FlussUtilsTest.java
│   └── source/
│       ├── FlussScanNodeTest.java
│       ├── FlussSplitTest.java
│       └── FlussSourceTest.java
│
├── be/src/vec/exec/format/table/
│   ├── fluss_reader.h                     # BE reader header
│   └── fluss_reader.cpp                   # BE reader implementation
│
├── gensrc/thrift/
│   ├── Descriptors.thrift                 # TFlussTable
│   └── PlanNodes.thrift                   # TFlussFileDesc
│
├── regression-test/suites/external_table_p0/fluss/
│   ├── test_fluss_catalog.groovy          # Catalog tests
│   ├── test_fluss_basic_read.groovy       # Basic read tests
│   ├── test_fluss_predicate_pushdown.groovy
│   └── test_fluss_types.groovy
│
└── docker/integration-test/fluss/
    ├── docker-compose.yml                 # Test environment
    └── setup-test-data.sql                # Test data
```

---

## 12. Next Steps

1. **Immediate (This Week):**
   - [ ] Complete `FlussMetadataOps.getTableSchema()` implementation
   - [ ] Add lake file path discovery in `FlussScanNode`
   - [ ] Write unit tests for schema loading

2. **Short-term (2 Weeks):**
   - [ ] Complete BE `FlussReader` for Parquet files
   - [ ] Set up Docker integration test environment
   - [ ] Create initial Groovy regression tests

3. **Medium-term (1 Month):**
   - [ ] Performance testing and optimization
   - [ ] Add observability (metrics, logging)
   - [ ] Security features (SASL/SSL)

4. **Long-term (3 Months):**
   - [ ] Real-time log reads via JNI
   - [ ] Write support
   - [ ] Time travel queries

---

*Document Version: 1.0*
*Last Updated: 2026-01-12*
*Authors: Doris-Fluss Integration Team*
