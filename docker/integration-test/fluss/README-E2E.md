# Fluss-Doris Integration E2E Tests

## Overview

The `run-e2e-tests.sh` script runs comprehensive end-to-end integration tests for Fluss-Doris integration. The same script works both locally and in GitHub CI.

## Prerequisites

- Docker and Docker Compose installed
- Doris FE Docker image available (either `doris-fe-fluss:latest` for local or `doris-fe-fluss:ci` for CI)

## Running Tests Locally

```bash
cd docker/integration-test/fluss
./run-e2e-tests.sh
```

The script will:
1. Start Fluss cluster (ZooKeeper, MinIO, Coordinator, Tablet Server)
2. Start Flink for test data setup
3. Create test tables (users, orders) with sample data
4. Start Doris FE
5. Run catalog tests (CREATE/SHOW/SWITCH/DROP catalog)
6. Run metadata tests (SHOW DATABASES/TABLES, DESC tables)
7. Collect service logs to `test-logs/` directory
8. Display test results

## Running in GitHub CI

The workflow automatically runs this script when:
- Pull requests modify Fluss-related code
- Pushes to `feature/fluss-table-integration` branch
- Manual workflow dispatch

The CI workflow additionally:
- Builds Doris FE from source with Fluss support
- Creates a custom Docker image
- Uploads test logs as artifacts

## Test Coverage

### Catalog Operations
- CREATE CATALOG with Fluss type
- SHOW CATALOGS
- SWITCH to Fluss catalog
- DROP CATALOG

### Metadata Operations
- SHOW DATABASES (lists Fluss databases)
- USE database (switches context)
- SHOW TABLES (lists Fluss tables)
- DESC table (shows table schema)

## Environment Detection

The script automatically detects the environment:
- **Local**: Uses existing services if already running, provides interactive output
- **CI**: Starts services from scratch, optimized for GitHub Actions

## Troubleshooting

### View Service Logs

```bash
# Fluss Coordinator
docker logs fluss-coordinator

# Fluss Tablet Server
docker logs fluss-tablet-server

# Doris FE
docker logs doris-fe

# Or check collected logs
ls -la test-logs/
```

### Connect to Doris Manually

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

### Clean Up Services

```bash
docker-compose down -v
```

### Test Individual Components

```bash
# Test Fluss is accessible
curl -s http://localhost:9123

# Test Doris FE is running
curl -s http://localhost:8030/api/bootstrap

# Test Flink UI
curl -s http://localhost:8081
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   E2E Test Flow                          │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  1. Start Fluss Cluster                                  │
│     ├── ZooKeeper (coordination)                         │
│     ├── MinIO (object storage)                           │
│     ├── Coordinator (metadata service)                   │
│     └── Tablet Server (data service)                     │
│                                                           │
│  2. Start Flink                                          │
│     ├── JobManager                                       │
│     └── TaskManager                                      │
│                                                           │
│  3. Setup Test Data                                      │
│     ├── Create Fluss catalog in Flink                    │
│     ├── Create database: demo_db                         │
│     ├── Create tables: users, orders                     │
│     └── Insert sample data                               │
│                                                           │
│  4. Start Doris FE                                       │
│     └── Wait for service ready                           │
│                                                           │
│  5. Run Catalog Tests                                    │
│     ├── CREATE CATALOG                                   │
│     ├── SHOW CATALOGS                                    │
│     ├── SWITCH catalog                                   │
│     └── DROP CATALOG                                     │
│                                                           │
│  6. Run Metadata Tests                                   │
│     ├── CREATE CATALOG                                   │
│     ├── SHOW DATABASES                                   │
│     ├── SHOW TABLES                                      │
│     └── DESC tables                                      │
│                                                           │
│  7. Collect Logs                                         │
│     └── Save to test-logs/                               │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

## Expected Output

```
==========================================
Fluss-Doris Integration E2E Tests
==========================================

Running locally

Step 1: Starting Fluss cluster...
Fluss cluster already running

Step 2: Verifying Fluss cluster health...
CONTAINER ID   IMAGE                    STATUS
abc123...      fluss:latest             Up 5 minutes
def456...      fluss:latest             Up 5 minutes

...

==========================================
Running Catalog Tests
==========================================
PASSED: Catalog tests

==========================================
Running Metadata Tests
==========================================
PASSED: Metadata tests

==========================================
E2E Test Results: SUCCESS
==========================================

All tests passed:
  - Catalog creation and operations
  - Metadata operations (databases, tables)
  - Fluss-Doris integration verified
```

## CI Workflow Integration

The GitHub Actions workflow (`.github/workflows/fluss-integration.yml`) executes this script:

```yaml
- name: Run E2E Tests
  working-directory: docker/integration-test/fluss
  run: |
    chmod +x run-e2e-tests.sh
    ./run-e2e-tests.sh
```

Benefits:
- Single source of truth for test logic
- Same tests run locally and in CI
- Easy to add new tests (edit one script)
- Consistent behavior across environments
