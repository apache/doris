# Doris-Fluss Integration - Next Steps

## Summary of Completed Work

### 1. Fluss Tiered Storage Model Analysis ✅
- Analyzed Fluss's two-tier storage: **Lake** (Parquet) + **Log** (native format)
- Studied `LakeSnapshot`, `LakeSplit`, `LakeSplitGenerator` in Fluss codebase
- Understood hybrid split model: `LakeSnapshotAndFlussLogSplit`

### 2. MVP Query Execution Flow Design ✅
The 6-step flow as requested:
1. **FE: Fetch metadata via Fluss Java SDK** - TableInfo, LakeSnapshot, LakeSplits
2. **FE: Determine data tiers per split** - Lake offset vs current offset
3. **FE: Generate tiered execution plan** - LAKE_ONLY, LOG_ONLY, or HYBRID splits
4. **FE→BE: Distribute splits** - Via Thrift with tier information
5. **BE: Process splits by tier** - Parquet reader for lake, JNI for log (Phase 2)
6. **BE: Shuffle, aggregate, return results**

### 3. Code Implementation ✅

#### FlussSplit.java - Tiered Split Support
- Added `SplitTier` enum: `LAKE_ONLY`, `LOG_ONLY`, `HYBRID`
- Added tier-related fields: `lakeFilePaths`, `lakeFormat`, `lakeSnapshotId`, `logStartOffset`, `logEndOffset`
- Factory methods: `createLakeSplit()`, `createLogSplit()`, `createHybridSplit()`
- Helper methods: `isLakeSplit()`, `hasLakeData()`, `hasLogData()`

#### FlussScanNode.java - Tiered Split Generation
- `getLakeSnapshot()` - Fetches LakeSnapshot via Fluss Admin API
- `generateSplitsForPartition()` - Creates tiered splits per bucket
- `getLakeFilesPerBucket()` - Discovers Parquet files for lake tier
- Split counting by tier for logging/debugging

#### FlussExternalTable.java
- Added `FlussTableType` enum (LOG_TABLE, PRIMARY_KEY_TABLE)
- Added `FlussTableMetadata` inner class with getters/setters
- Lazy loading with double-checked locking

#### FlussExternalCatalog.java
- Security constants: `FLUSS_SECURITY_PROTOCOL`, `FLUSS_SASL_*`
- `getBootstrapServers()`, `getSecurityProtocol()`, etc.

#### FlussMetadataOps.java
- `getTableMetadata()` - Fetches actual metadata from Fluss
- `getTableInfo()` - For schema loading
- Proper cache typing

#### Thrift Definitions (PlanNodes.thrift)
- Added `TFlussSplitTier` enum
- Extended `TFlussFileDesc` with tier fields:
  - `tier`, `lake_file_paths`, `lake_snapshot_id`
  - `log_start_offset`, `log_end_offset`

#### BE FlussReader (fluss_reader.cpp)
- Added logging for tier information
- Comments for future LOG tier implementation via JNI

### 4. Testing Infrastructure ✅
- Docker Compose environment (`docker/integration-test/fluss/`)
- Groovy regression tests for catalog, reads, predicates, types

---

## Immediate Next Steps (This Week)

### 1. Verify Code Compiles
```bash
cd /Users/shekhar.prasad/Documents/repos/oss/apache/doris
./build.sh --fe
```

### 2. Run Unit Tests
```bash
cd fe
mvn test -Dtest=org.apache.doris.datasource.fluss.*Test
```

### 3. Fix Any Remaining Compilation Issues
Check for:
- Missing imports in FlussExternalTable (TableInfo import may be unused)
- Any circular dependencies

---

## Short-Term (Next 2 Weeks)

### 1. Complete BE FlussReader Implementation

The current `fluss_reader.cpp` is a skeleton. For MVP, implement lake file reading:

```cpp
// be/src/vec/exec/format/table/fluss_reader.cpp

Status FlussReader::init_reader(/* params */) {
    // 1. Extract lake file paths from TFlussFileDesc
    // 2. Initialize ParquetReader with those paths
    // 3. Set up column projection and predicates
    return Status::OK();
}
```

### 2. Wire FE-BE Communication

Update `FlussScanNode.getSplits()` to:
1. Query Fluss for lake file paths (via snapshot API)
2. Include file paths in `TFlussFileDesc`
3. Pass S3/HDFS credentials for file access

### 3. Set Up Integration Test Environment

```bash
cd docker/integration-test/fluss
docker-compose up -d

# Wait for Fluss to be ready
sleep 60

# Verify
curl http://localhost:9123/health
```

### 4. Run Integration Tests

```bash
./run-regression-test.sh \
    --suite external_table_p0/fluss \
    -conf flussBootstrapServers=localhost:9123 \
    -conf enableFlussTest=true
```

---

## Medium-Term (4-6 Weeks)

### 1. Implement Lake File Discovery

```java
// In FlussScanNode.java
private List<String> getLakeFilePaths(FlussExternalTable table, long snapshotId) {
    Table flussTable = FlussUtils.getFlussTable(table);
    TableSnapshot snapshot = flussTable.getSnapshot(snapshotId);
    
    List<String> filePaths = new ArrayList<>();
    for (BucketSnapshot bucket : snapshot.getBucketSnapshots()) {
        filePaths.addAll(bucket.getDataFiles());
    }
    return filePaths;
}
```

### 2. Add Observability

```java
// Metrics
public class FlussMetrics {
    private final Counter scanOperations = Counter.build()
        .name("doris_fluss_scan_total")
        .help("Total Fluss scan operations")
        .register();
        
    private final Histogram scanLatency = Histogram.build()
        .name("doris_fluss_scan_latency_seconds")
        .help("Fluss scan latency")
        .register();
}
```

### 3. Performance Testing

- Benchmark with 1GB, 10GB, 100GB tables
- Measure scan latency P50/P95/P99
- Profile memory usage

---

## Files Modified

| File | Changes |
|------|---------|
| `fe/.../fluss/FlussExternalTable.java` | Added enum, metadata class, getters |
| `fe/.../fluss/FlussExternalCatalog.java` | Added constants and getter methods |
| `fe/.../fluss/FlussMetadataOps.java` | Fixed cache types, added getTableInfo |
| `regression-test/.../fluss/*.groovy` | New test files |
| `docker/integration-test/fluss/*` | New Docker setup |
| `docs/fluss-integration/*` | New documentation |

---

## Verification Checklist

- [ ] FE compiles without errors
- [ ] Unit tests pass
- [ ] Docker environment starts
- [ ] Can create Fluss catalog in Doris
- [ ] Can list databases/tables
- [ ] Can describe table schema
- [ ] Basic SELECT query works

---

## Resources

- [Fluss Documentation](https://fluss.apache.org/docs/)
- [Doris External Catalogs](https://doris.apache.org/docs/lakehouse/catalogs/)
- [Implementation Strategy](./IMPLEMENTATION_STRATEGY.md)
- [Integration Test README](../../docker/integration-test/fluss/README.md)
