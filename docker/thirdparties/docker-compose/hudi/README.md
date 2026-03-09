<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either implied.  See the License for the specific
language governing permissions and limitations
under the License.
-->

# Hudi Docker Environment

This directory contains the Docker Compose configuration for setting up a Hudi test environment with Spark, Hive Metastore, MinIO (S3-compatible storage), and PostgreSQL.

## Components

- **Spark**: Apache Spark 3.5.7 for processing Hudi tables
- **Hive Metastore**: Starburst Hive Metastore for table metadata management
- **PostgreSQL**: Database backend for Hive Metastore
- **MinIO**: S3-compatible object storage for Hudi data files

## Important Configuration Parameters

### Container UID
- **Parameter**: `CONTAINER_UID` in `custom_settings.env`
- **Default**: `doris--`
- **Note**: Must be set to a unique value to avoid conflicts with other Docker environments
- **Example**: `CONTAINER_UID="doris--bender--"`

### Port Configuration (`hudi.env.tpl`)
- `HIVE_METASTORE_PORT`: Port for Hive Metastore Thrift service (default: 19083)
- `MINIO_API_PORT`: MinIO S3 API port (default: 19100)
- `MINIO_CONSOLE_PORT`: MinIO web console port (default: 19101)
- `SPARK_UI_PORT`: Spark web UI port (default: 18080)

### MinIO Credentials (`hudi.env.tpl`)
- `MINIO_ROOT_USER`: MinIO access key (default: `minio`)
- `MINIO_ROOT_PASSWORD`: MinIO secret key (default: `minio123`)
- `HUDI_BUCKET`: S3 bucket name for Hudi data (default: `datalake`)

### Version Compatibility
⚠️ **Important**: Hadoop versions must match Spark's built-in Hadoop version
- **Spark Version**: 3.5.7 (uses Hadoop 3.3.4) - default build for Hudi 1.0.2
- **Hadoop AWS Version**: 3.3.4 (matching Spark's Hadoop)
- **Hudi Bundle Version**: 1.0.2 Spark 3.5 bundle (default build, matches Spark 3.5.7, matches Doris's Hudi version to avoid versionCode compatibility issues)
- **AWS SDK v1 Version**: 1.12.262 (required for Hadoop 3.3.4 S3A support, 1.12.x series)
- **PostgreSQL JDBC Version**: 42.7.1 (compatible with Hive Metastore)
- **Hudi 1.0.x Compatibility**: Supports Spark 3.5.x (default), 3.4.x, and 3.3.x

### JAR Dependencies (`hudi.env.tpl`)
All JAR file versions and URLs are configurable:
- `HUDI_BUNDLE_VERSION` / `HUDI_BUNDLE_URL`: Hudi Spark bundle
- `HADOOP_AWS_VERSION` / `HADOOP_AWS_URL`: Hadoop S3A filesystem support
- `AWS_SDK_BUNDLE_VERSION` / `AWS_SDK_BUNDLE_URL`: AWS Java SDK Bundle v1 (required for Hadoop 3.3.4 S3A support, 1.12.x series)

**Note**: `hadoop-common` is already included in Spark's built-in Hadoop distribution, so it's not configured here.
- `POSTGRESQL_JDBC_VERSION` / `POSTGRESQL_JDBC_URL`: PostgreSQL JDBC driver

## Starting the Environment

```bash
# Start Hudi environment
./docker/thirdparties/run-thirdparties-docker.sh -c hudi

# Stop Hudi environment
./docker/thirdparties/run-thirdparties-docker.sh -c hudi --stop
```

## Adding Data

⚠️ **Important**: To ensure data consistency after Docker restarts, **only use SQL scripts** to add data. Data added through `spark-sql` interactive shell is temporary and will not persist after container restart.

### Using SQL Scripts

Add new SQL files in `scripts/create_preinstalled_scripts/hudi/` directory:
- Files are executed in alphabetical order (e.g., `01_config_and_database.sql`, `02_create_user_activity_log_tables.sql`, etc.)
- Use descriptive names with numeric prefixes to control execution order
- Use environment variable substitution: `${HIVE_METASTORE_URIS}` and `${HUDI_BUCKET}`
- **Data created through SQL scripts will persist after Docker restart**

Example: Create `08_create_custom_table.sql`:
```sql
USE regression_hudi;

CREATE TABLE IF NOT EXISTS my_hudi_table (
  id BIGINT,
  name STRING,
  created_at TIMESTAMP
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'created_at',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/my_hudi_table';

INSERT INTO my_hudi_table VALUES
  (1, 'Alice', TIMESTAMP '2024-01-01 10:00:00'),
  (2, 'Bob', TIMESTAMP '2024-01-02 11:00:00');
```

After adding SQL files, restart the container to execute them:
```bash
docker restart doris--hudi-spark
```

## Creating Hudi Catalog in Doris

After starting the Hudi Docker environment, you can create a Hudi catalog in Doris to access Hudi tables:

```sql
-- Create Hudi catalog
CREATE CATALOG IF NOT EXISTS hudi_catalog PROPERTIES (
    'type' = 'hms',
    'hive.metastore.uris' = 'thrift://<externalEnvIp>:19083',
    's3.endpoint' = 'http://<externalEnvIp>:19100',
    's3.access_key' = 'minio',
    's3.secret_key' = 'minio123',
    's3.region' = 'us-east-1',
    'use_path_style' = 'true'
);

-- Switch to Hudi catalog
SWITCH hudi_catalog;

-- Use database
USE regression_hudi;

-- Show tables
SHOW TABLES;

-- Query Hudi table
SELECT * FROM user_activity_log_cow_partition LIMIT 10;
```

**Configuration Parameters:**
- `hive.metastore.uris`: Hive Metastore Thrift service address (default port: 19083)
- `s3.endpoint`: MinIO S3 API endpoint (default port: 19100)
- `s3.access_key`: MinIO access key (default: `minio`)
- `s3.secret_key`: MinIO secret key (default: `minio123`)
- `s3.region`: S3 region (default: `us-east-1`)
- `use_path_style`: Use path-style access for MinIO (required: `true`)

Replace `<externalEnvIp>` with your actual external environment IP address (e.g., `127.0.0.1` for localhost).

## Debugging with Spark SQL

⚠️ **Note**: The methods below are for debugging purposes only. Data created through `spark-sql` interactive shell will **not persist** after Docker restart. To add persistent data, use SQL scripts as described in the "Adding Data" section.

### 1. Connect to Spark Container

```bash
docker exec -it doris--hudi-spark bash
```

### 2. Start Spark SQL Interactive Shell

```bash
/opt/spark/bin/spark-sql \
  --master local[*] \
  --name hudi-debug \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.sql.warehouse.dir=s3a://datalake/warehouse
```

### 3. Common Debugging Commands

```sql
-- Show databases
SHOW DATABASES;

-- Use database
USE regression_hudi;

-- Show tables
SHOW TABLES;

-- Describe table structure
DESCRIBE EXTENDED user_activity_log_cow_partition;

-- Query data
SELECT * FROM user_activity_log_cow_partition LIMIT 10;

-- Check Hudi table properties
SHOW TBLPROPERTIES user_activity_log_cow_partition;

-- View Spark configuration
SET -v;

-- Check Hudi-specific configurations
SET hoodie.datasource.write.hive_style_partitioning;
```

### 4. View Spark Web UI

Access Spark Web UI at: `http://localhost:18080` (or configured `SPARK_UI_PORT`)

### 5. Check Container Logs

```bash
# View Spark container logs
docker logs doris--hudi-spark --tail 100 -f

# View Hive Metastore logs
docker logs doris--hudi-metastore --tail 100 -f

# View MinIO logs
docker logs doris--hudi-minio --tail 100 -f
```

### 6. Verify S3 Data

```bash
# Access MinIO console
# URL: http://localhost:19101 (or configured MINIO_CONSOLE_PORT)
# Username: minio (or MINIO_ROOT_USER)
# Password: minio123 (or MINIO_ROOT_PASSWORD)

# Or use MinIO client
docker exec -it doris--hudi-minio-mc mc ls myminio/datalake/warehouse/regression_hudi/
```

## Troubleshooting

### Container Exits Immediately
- Check logs: `docker logs doris--hudi-spark`
- Verify SUCCESS file exists: `docker exec doris--hudi-spark test -f /opt/hudi-scripts/SUCCESS`
- Ensure Hive Metastore is running: `docker ps | grep metastore`

### ClassNotFoundException Errors
- Verify JAR files are downloaded: `docker exec doris--hudi-spark ls -lh /opt/hudi-cache/`
- Check JAR versions match Spark's Hadoop version (3.3.4)
- Review `hudi.env.tpl` for correct version numbers

### S3A Connection Issues
- Verify MinIO is running: `docker ps | grep minio`
- Check MinIO credentials in `hudi.env.tpl`
- Test S3 connection: `docker exec doris--hudi-minio-mc mc ls myminio/`

### Hive Metastore Connection Issues
- Check Metastore is ready: `docker logs doris--hudi-metastore | grep "Metastore is ready"`
- Verify PostgreSQL is running: `docker ps | grep metastore-db`
- Test connection: `docker exec doris--hudi-metastore-db pg_isready -U hive`

## File Structure

```
hudi/
├── hudi.yaml.tpl          # Docker Compose template
├── hudi.env.tpl           # Environment variables template
├── scripts/
│   ├── init.sh            # Initialization script
│   ├── create_preinstalled_scripts/
│   │   └── hudi/          # SQL scripts (01_config_and_database.sql, 02_create_user_activity_log_tables.sql, ...)
│   └── SUCCESS            # Initialization marker (generated)
└── cache/                 # Downloaded JAR files (generated)
```

## Notes

- All generated files (`.yaml`, `.env`, `cache/`, `SUCCESS`) are ignored by Git
- SQL scripts support environment variable substitution using `${VARIABLE_NAME}` syntax
- Hadoop version compatibility is critical - must match Spark's built-in version
- Container keeps running after initialization for healthcheck and debugging

