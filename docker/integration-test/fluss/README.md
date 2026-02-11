# Doris-Fluss Integration Test Environment

This directory contains Docker Compose configuration for running integration tests between Apache Doris and Apache Fluss.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 8GB+ available RAM
- Network access for pulling images

## Quick Start

### 1. Start the Test Environment

```bash
cd docker/integration-test/fluss

# Start all services
docker-compose up -d

# Wait for services to be healthy (about 2-3 minutes)
docker-compose ps

# Check logs if needed
docker-compose logs -f fluss-coordinator
```

### 2. Verify Fluss is Running

```bash
# Check coordinator health
curl http://localhost:9123/health

# List databases via Fluss CLI
docker exec -it fluss-coordinator /opt/fluss/bin/fluss-client.sh \
    --bootstrap-server localhost:9123 \
    -e "SHOW DATABASES"
```

### 3. Run Doris Tests

```bash
# From Doris root directory
./run-regression-test.sh \
    --suite external_table_p0/fluss \
    -conf flussBootstrapServers=localhost:9123 \
    -conf enableFlussTest=true
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| ZooKeeper | 2181 | Coordination service for Fluss |
| Fluss Coordinator | 9123 | Metadata and cluster management |
| Fluss Tablet Server | 9124 | Data storage and serving |
| MinIO | 9000/9001 | S3-compatible storage for lake data |

## Test Tables

The `fluss-init` service automatically creates test tables:

### all_types
Primary key table with all supported data types:
```sql
CREATE TABLE test_db.all_types (
    id INT PRIMARY KEY,
    bool_col BOOLEAN,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    int_col INT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(10, 2),
    string_col STRING,
    date_col DATE,
    timestamp_col TIMESTAMP(3)
);
```

### partitioned_table
Partitioned primary key table:
```sql
CREATE TABLE test_db.partitioned_table (
    id INT,
    name STRING,
    value DOUBLE,
    dt STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt);
```

### log_table
Append-only log table:
```sql
CREATE TABLE test_db.log_table (
    id INT,
    message STRING,
    created_at TIMESTAMP(3)
);
```

## Loading Test Data

### Option 1: Via Flink SQL

```bash
# Start Flink services
docker-compose --profile flink up -d

# Connect to Flink SQL client
docker exec -it flink-jobmanager ./bin/sql-client.sh

# In Flink SQL:
CREATE CATALOG fluss WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'fluss-coordinator:9123'
);

USE CATALOG fluss;
USE test_db;

INSERT INTO all_types VALUES
    (1, true, 1, 100, 1000, 10000, 1.1, 2.2, 99.99, 'test1', DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00'),
    (2, false, 2, 200, 2000, 20000, 2.2, 3.3, 199.99, 'test2', DATE '2024-01-02', TIMESTAMP '2024-01-02 11:00:00');
```

### Option 2: Via Fluss Client

```bash
docker exec -it fluss-coordinator /opt/fluss/bin/fluss-client.sh \
    --bootstrap-server localhost:9123 <<EOF
USE test_db;
INSERT INTO all_types (id, bool_col, string_col) VALUES (1, true, 'hello');
INSERT INTO all_types (id, bool_col, string_col) VALUES (2, false, 'world');
EOF
```

## Testing from Doris

### Create Fluss Catalog

```sql
-- Connect to Doris
mysql -h 127.0.0.1 -P 9030 -u root

-- Create Fluss catalog
CREATE CATALOG fluss_catalog PROPERTIES (
    "type" = "fluss",
    "bootstrap.servers" = "localhost:9123"
);

-- Explore
SHOW DATABASES FROM fluss_catalog;
USE fluss_catalog.test_db;
SHOW TABLES;
DESC all_types;

-- Query data
SELECT * FROM all_types LIMIT 10;
SELECT COUNT(*) FROM all_types;
```

## Troubleshooting

### Services Not Starting

```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs fluss-coordinator
docker-compose logs fluss-tablet-server

# Restart specific service
docker-compose restart fluss-tablet-server
```

### Connection Issues

```bash
# Check network
docker network inspect fluss_doris-fluss-net

# Test connectivity
docker exec -it fluss-tablet-server ping fluss-coordinator
```

### Clean Restart

```bash
# Stop and remove everything
docker-compose down -v

# Remove orphan volumes
docker volume prune -f

# Start fresh
docker-compose up -d
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLUSS_MODE` | - | `coordinator` or `tablet-server` |
| `FLUSS_PROPERTIES` | - | Multi-line Fluss configuration |

### MinIO Configuration

- **Endpoint**: http://minio:9000
- **Console**: http://localhost:9001
- **Access Key**: minioadmin
- **Secret Key**: minioadmin
- **Bucket**: fluss-lake

MinIO provides:
- **Multi-arch support** (works on ARM64 Mac M1/M2/M3)
- **S3-compatible API** for compatibility with existing tools
- **Lightweight** and easy to set up for development

## CI/CD Integration

For automated testing in CI pipelines:

```yaml
# .github/workflows/fluss-integration.yml
jobs:
  fluss-integration-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start Fluss Environment
        run: |
          cd docker/integration-test/fluss
          docker-compose up -d
          sleep 60  # Wait for services
          
      - name: Run Integration Tests
        run: |
          ./run-regression-test.sh \
            --suite external_table_p0/fluss \
            -conf flussBootstrapServers=localhost:9123 \
            -conf enableFlussTest=true
            
      - name: Cleanup
        if: always()
        run: |
          cd docker/integration-test/fluss
          docker-compose down -v
```

## Related Resources

- [Apache Fluss Documentation](https://fluss.apache.org/docs/)
- [Apache Doris External Catalogs](https://doris.apache.org/docs/lakehouse/catalogs/)
- [Implementation Strategy](../../docs/fluss-integration/IMPLEMENTATION_STRATEGY.md)
