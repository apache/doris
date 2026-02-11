#!/bin/bash
# Fluss-Doris Integration E2E Tests
# This script runs both locally and in GitHub CI

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Detect environment
if [ -n "$GITHUB_ACTIONS" ]; then
    echo "Running in GitHub Actions CI"
    IS_CI=true
else
    echo "Running locally"
    IS_CI=false
fi

echo "=========================================="
echo "Fluss-Doris Integration E2E Tests"
echo "=========================================="
echo ""

# Function to wait for service
wait_for_service() {
    local service_name=$1
    local check_command=$2
    local max_attempts=${3:-30}
    local sleep_time=${4:-2}

    echo "Waiting for $service_name to be ready..."
    for i in $(seq 1 $max_attempts); do
        if eval "$check_command" &>/dev/null; then
            echo "$service_name is ready!"
            return 0
        fi
        echo "  Attempt $i/$max_attempts..."
        sleep $sleep_time
    done

    echo "ERROR: $service_name failed to start"
    return 1
}

# Step 1: Start Fluss cluster
echo "Step 1: Starting Fluss cluster..."
if [ "$IS_CI" = true ]; then
    # In CI, start services in order
    docker-compose up -d zookeeper minio
    sleep 10
    docker-compose up -d minio-init
    sleep 5
    docker-compose up -d fluss-coordinator fluss-tablet-server
    sleep 30
else
    # Locally, check if already running
    if ! docker ps | grep -q "fluss-coordinator"; then
        docker-compose up -d fluss-coordinator fluss-tablet-server
        sleep 30
    else
        echo "Fluss cluster already running"
    fi
fi

# Step 2: Verify Fluss cluster health
echo ""
echo "Step 2: Verifying Fluss cluster health..."
docker ps | grep -E "fluss-coordinator|fluss-tablet-server"

echo "Checking Fluss coordinator logs..."
docker logs fluss-coordinator | tail -20

echo "Checking Fluss tablet server logs..."
docker logs fluss-tablet-server | tail -20

# Step 3: Start Flink for test data setup
echo ""
echo "Step 3: Starting Flink cluster..."
docker-compose --profile flink up -d flink-jobmanager flink-taskmanager
sleep 15

wait_for_service "Flink" "curl -s http://localhost:8081/overview"

# Step 4: Setup test data via Flink SQL
echo ""
echo "Step 4: Setting up test data via Flink SQL..."
cat > /tmp/setup-data.sql << 'EOF'
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'fluss-coordinator:9123'
);

USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY NOT ENFORCED,
    name STRING,
    email STRING,
    age INT,
    city STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3)
) WITH ('bucket.num' = '4');

CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    user_id INT,
    product STRING,
    amount DOUBLE,
    status STRING,
    order_date STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_id, order_date) NOT ENFORCED
) PARTITIONED BY (order_date) WITH ('bucket.num' = '4');

INSERT INTO users VALUES
    (1, 'Alice', 'alice@test.com', 28, 'SF', TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2024-01-15 10:00:00'),
    (2, 'Bob', 'bob@test.com', 35, 'NY', TIMESTAMP '2024-01-15 11:00:00', TIMESTAMP '2024-01-15 11:00:00'),
    (3, 'Carol', 'carol@test.com', 42, 'LA', TIMESTAMP '2024-01-15 11:30:00', TIMESTAMP '2024-01-15 11:30:00');

INSERT INTO orders VALUES
    (101, 1, 'Laptop', 1299.99, 'completed', '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
    (102, 2, 'Mouse', 29.99, 'completed', '2024-01-15', TIMESTAMP '2024-01-15 11:15:00');
EOF

docker cp /tmp/setup-data.sql flink-jobmanager:/tmp/
if docker exec flink-jobmanager ./bin/sql-client.sh -f /tmp/setup-data.sql; then
    echo "Test data setup completed"
else
    echo "Warning: Test data setup had issues, but continuing..."
fi

# Step 5: Start Doris FE
echo ""
echo "Step 5: Starting Doris FE..."
if [ "$IS_CI" = true ]; then
    # In CI, always start fresh
    docker-compose --profile doris up -d doris-fe
    sleep 20
else
    # Locally, check if already running
    if ! docker ps | grep -q "doris-fe"; then
        docker-compose --profile doris up -d doris-fe
        sleep 20
    else
        echo "Doris FE already running"
    fi
fi

# Wait for Doris FE to be healthy
wait_for_service "Doris FE" \
    "docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e 'SELECT 1'" \
    30 2 || {
    echo "Doris FE logs:"
    docker logs doris-fe
    exit 1
}

# Step 6: Run Catalog Tests
echo ""
echo "=========================================="
echo "Running Catalog Tests"
echo "=========================================="

cat > /tmp/catalog-tests.sql << 'EOF'
-- Test 1: Create Fluss catalog
CREATE CATALOG IF NOT EXISTS test_fluss_catalog PROPERTIES (
    'type' = 'fluss',
    'bootstrap.servers' = 'fluss-coordinator:9123'
);

-- Test 2: Verify catalog exists
SHOW CATALOGS;

-- Test 3: Switch to catalog
SWITCH test_fluss_catalog;

-- Test 4: Drop catalog
DROP CATALOG test_fluss_catalog;
EOF

if docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot < /tmp/catalog-tests.sql; then
    echo "PASSED: Catalog tests"
else
    echo "FAILED: Catalog tests"
    echo "Doris FE logs:"
    docker logs doris-fe | tail -50
    exit 1
fi

# Step 7: Run Metadata Tests
echo ""
echo "=========================================="
echo "Running Metadata Tests"
echo "=========================================="

cat > /tmp/metadata-tests.sql << 'EOF'
-- Test 1: Create catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog PROPERTIES (
    'type' = 'fluss',
    'bootstrap.servers' = 'fluss-coordinator:9123'
);

-- Test 2: Switch to catalog
SWITCH fluss_catalog;

-- Test 3: List databases
SHOW DATABASES;

-- Test 4: Use database
USE demo_db;

-- Test 5: List tables
SHOW TABLES;

-- Test 6: Describe table
DESC users;
EOF

if docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot < /tmp/metadata-tests.sql; then
    echo "PASSED: Metadata tests"
else
    echo "FAILED: Metadata tests"
    echo "Doris FE logs:"
    docker logs doris-fe | tail -50
    exit 1
fi

# Step 8: Collect logs for debugging
echo ""
echo "Step 8: Collecting service logs..."
mkdir -p test-logs
docker logs fluss-coordinator > test-logs/fluss-coordinator.log 2>&1 || true
docker logs fluss-tablet-server > test-logs/fluss-tablet-server.log 2>&1 || true
docker logs doris-fe > test-logs/doris-fe.log 2>&1 || true
docker logs flink-jobmanager > test-logs/flink-jobmanager.log 2>&1 || true

echo "Logs saved to test-logs/"

# Summary
echo ""
echo "=========================================="
echo "E2E Test Results: SUCCESS"
echo "=========================================="
echo ""
echo "All tests passed:"
echo "  - Catalog creation and operations"
echo "  - Metadata operations (databases, tables)"
echo "  - Fluss-Doris integration verified"
echo ""
echo "Services running:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "fluss-|doris-fe|flink-"

if [ "$IS_CI" = false ]; then
    echo ""
    echo "To connect to Doris manually:"
    echo "  mysql -h 127.0.0.1 -P 9030 -uroot"
    echo ""
    echo "To view logs:"
    echo "  docker logs fluss-coordinator"
    echo "  docker logs doris-fe"
    echo ""
    echo "To stop services:"
    echo "  docker-compose down"
fi

exit 0
