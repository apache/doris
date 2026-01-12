// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_iceberg_jdbc_catalog", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is not enabled, skip this test")
        return;
    }

    String enabledJdbc = context.config.otherConfigs.get("enableJdbcTest")
    if (enabledJdbc == null || !enabledJdbc.equalsIgnoreCase("true")) {
        logger.info("Iceberg JDBC catalog test requires enableJdbcTest, skip this test")
        return;
    }

    // Get test environment configuration
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String jdbc_port = context.config.otherConfigs.get("pg_14_port")
    
    // JDBC Catalog specific test - uses PostgreSQL as the metadata store
    // If PostgreSQL port is not configured, this test will be skipped
    if (jdbc_port == null || jdbc_port.isEmpty()) {
        logger.info("Iceberg JDBC catalog PostgreSQL port not configured (pg_14_port), skip this test")
        return;
    }

    if (minio_port == null || minio_port.isEmpty() || externalEnvIp == null) {
        logger.info("Iceberg test environment not fully configured, skip this test")
        return;
    }

    String catalog_name = "test_iceberg_jdbc_catalog"
    String db_name = "jdbc_test_db"
    String driver_name = "postgresql-42.5.0.jar"
    String driver_download_url = "${getS3Url()}/regression/jdbc_driver/${driver_name}"
    String jdbc_drivers_dir = getFeConfig("jdbc_drivers_dir")
    String local_driver_dir = "${context.config.dataPath}/jdbc_driver"
    String local_driver_path = "${local_driver_dir}/${driver_name}"
    String pg_db = "postgres"
    String mysql_db = "iceberg_db"

    // MySQL driver config
    String mysql_driver_name = "mysql-connector-java-5.1.49-v2.jar"
    String mysql_driver_download_url = "${getS3Url()}/regression/jdbc_driver/mysql-connector-java-5.1.49.jar"
    String local_mysql_driver_path = "${local_driver_dir}/${mysql_driver_name}"

    def executeCommand = { String cmd, Boolean mustSuc ->
        try {
            logger.info("execute ${cmd}")
            def proc = new ProcessBuilder("/bin/bash", "-c", cmd).redirectErrorStream(true).start()
            int exitcode = proc.waitFor()
            if (exitcode != 0) {
                logger.info("exit code: ${exitcode}, output\n: ${proc.text}")
                if (mustSuc == true) {
                    assertTrue(false, "Execute failed: ${cmd}")
                }
            }
        } catch (IOException e) {
            assertTrue(false, "Execute timeout: ${cmd}")
        }
    }

    // Ensure the PostgreSQL JDBC driver is available on all FE/BE nodes.
    def host_ips = new ArrayList()
    String[][] backends = sql """ show backends """
    for (def b in backends) {
        host_ips.add(b[1])
    }
    String[][] frontends = sql """ show frontends """
    for (def f in frontends) {
        host_ips.add(f[1])
    }
    host_ips = host_ips.unique()

    executeCommand("mkdir -p ${local_driver_dir}", false)
    if (!new File(local_driver_path).exists()) {
        executeCommand("/usr/bin/curl --max-time 600 ${driver_download_url} --output ${local_driver_path}", true)
    }
    if (!new File(local_mysql_driver_path).exists()) {
        executeCommand("/usr/bin/curl --max-time 600 ${mysql_driver_download_url} --output ${local_mysql_driver_path}", true)
    }
    for (def ip in host_ips) {
        executeCommand("ssh -o StrictHostKeyChecking=no root@${ip} \"mkdir -p ${jdbc_drivers_dir}\"", false)
        scpFiles("root", ip, local_driver_path, jdbc_drivers_dir, false)
        scpFiles("root", ip, local_mysql_driver_path, jdbc_drivers_dir, false)
    }
    
    try {
        // Clean up existing catalog
        sql """DROP CATALOG IF EXISTS ${catalog_name}"""

        // Create Iceberg JDBC Catalog with PostgreSQL backend and MinIO storage
        sql """
            CREATE CATALOG ${catalog_name} PROPERTIES (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'jdbc',
                'uri' = 'jdbc:postgresql://${externalEnvIp}:${jdbc_port}/${pg_db}',
                'warehouse' = 's3://warehouse/jdbc_wh/',
                'iceberg.jdbc.driver_url' = '${driver_name}',
                'iceberg.jdbc.driver_class' = 'org.postgresql.Driver',
                'iceberg.jdbc.user' = 'postgres',
                'iceberg.jdbc.password' = '123456',
                'iceberg.jdbc.init-catalog-tables' = 'true',
                'iceberg.jdbc.schema-version' = 'V1',
                's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.region' = 'us-east-1'
            )
        """

        // Switch to the catalog
        sql """SWITCH ${catalog_name}"""

        // Test: Show catalogs
        def catalogs = sql """SHOW CATALOGS"""
        assertTrue(catalogs.toString().contains(catalog_name))

        // Test: Create database
        sql """DROP DATABASE IF EXISTS ${db_name} FORCE"""
        sql """CREATE DATABASE ${db_name}"""
        
        def databases = sql """SHOW DATABASES"""
        assertTrue(databases.toString().contains(db_name))

        sql """USE ${db_name}"""

        // Test: Create non-partitioned table with various data types
        sql """DROP TABLE IF EXISTS test_datatypes"""
        sql """
            CREATE TABLE test_datatypes (
                c_boolean BOOLEAN,
                c_int INT,
                c_bigint BIGINT,
                c_float FLOAT,
                c_double DOUBLE,
                c_decimal DECIMAL(10, 2),
                c_string STRING,
                c_date DATE,
                c_datetime DATETIME
            ) PROPERTIES (
                'write-format' = 'parquet',
                'compression-codec' = 'zstd'
            )
        """

        def tables = sql """SHOW TABLES"""
        assertTrue(tables.toString().contains("test_datatypes"))

        // Test: Insert data with various types
        sql """
            INSERT INTO test_datatypes VALUES
            (true, 1, 100000000000, 1.5, 2.5, 123.45, 'hello', '2025-01-01', '2025-01-01 10:00:00'),
            (false, 2, 200000000000, 2.5, 3.5, 234.56, 'world', '2025-01-02', '2025-01-02 11:00:00'),
            (true, 3, 300000000000, 3.5, 4.5, 345.67, 'test', '2025-01-03', '2025-01-03 12:00:00')
        """

        // Test: Query data with different data types
        order_qt_datatypes_select """SELECT * FROM test_datatypes ORDER BY c_int"""
        order_qt_datatypes_count """SELECT count(*) FROM test_datatypes"""
        order_qt_datatypes_filter """SELECT c_int, c_string FROM test_datatypes WHERE c_boolean = true ORDER BY c_int"""

        // Test: Create partitioned table
        sql """DROP TABLE IF EXISTS test_partitioned"""
        sql """
            CREATE TABLE test_partitioned (
                id INT,
                name STRING,
                category STRING,
                event_date DATE
            ) 
            PARTITION BY LIST (category) ()
            PROPERTIES (
                'write-format' = 'parquet'
            )
        """

        // Test: Insert into partitioned table
        sql """
            INSERT INTO test_partitioned VALUES
            (1, 'Item1', 'A', '2025-01-01'),
            (2, 'Item2', 'A', '2025-01-01'),
            (3, 'Item3', 'B', '2025-01-02'),
            (4, 'Item4', 'B', '2025-01-02'),
            (5, 'Item5', 'A', '2025-01-03')
        """

        order_qt_partition_select """SELECT * FROM test_partitioned ORDER BY id"""
        order_qt_partition_filter """SELECT * FROM test_partitioned WHERE category = 'A' ORDER BY id"""

        // Test: System tables
        order_qt_sys_snapshots """SELECT count(*) FROM test_datatypes\$snapshots"""
        order_qt_sys_history """SELECT count(*) FROM test_datatypes\$history"""

        // Test: DESCRIBE TABLE
        def desc = sql """DESCRIBE test_datatypes"""
        assertTrue(desc.toString().contains("c_int"))
        assertTrue(desc.toString().contains("c_string"))

        // Test: INSERT OVERWRITE
        sql """
            INSERT OVERWRITE TABLE test_partitioned
            SELECT * FROM test_partitioned WHERE category = 'A'
        """
        order_qt_after_overwrite """SELECT * FROM test_partitioned ORDER BY id"""

        // Test: Drop table
        sql """DROP TABLE IF EXISTS test_datatypes"""
        sql """DROP TABLE IF EXISTS test_partitioned"""

        // Test: Drop database
        sql """DROP DATABASE IF EXISTS ${db_name} FORCE"""

        logger.info("Iceberg JDBC Catalog test completed successfully")

        // MySQL Catalog Test
        String mysql_port = context.config.otherConfigs.get("mysql_57_port")
        if (mysql_port != null) {
            // Clean up MySQL database to remove old metadata
            // This prevents issues where the database contains metadata pointing to invalid S3 locations
            String cleanupCmd = "mysql -h ${externalEnvIp} -P ${mysql_port} -u root -p123456 -e 'DROP DATABASE IF EXISTS iceberg_db; CREATE DATABASE iceberg_db;'"
            executeCommand(cleanupCmd, false)

            String mysql_catalog_name = "iceberg_jdbc_mysql"
            try {
                sql """DROP CATALOG IF EXISTS ${mysql_catalog_name}"""
                sql """
                    CREATE CATALOG ${mysql_catalog_name} PROPERTIES (
                        'type' = 'iceberg',
                        'iceberg.catalog.type' = 'jdbc',
                        'uri' = 'jdbc:mysql://${externalEnvIp}:${mysql_port}/${mysql_db}',
                        'warehouse' = 's3://warehouse/jdbc_wh_mysql/',
                        'iceberg.jdbc.driver_url' = 'file://${jdbc_drivers_dir}/${mysql_driver_name}',
                        'iceberg.jdbc.driver_class' = 'com.mysql.jdbc.Driver',
                        'iceberg.jdbc.user' = 'root',
                        'iceberg.jdbc.password' = '123456',
                        'iceberg.jdbc.init-catalog-tables' = 'true',
                        'iceberg.jdbc.schema-version' = 'V1',
                        's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                        's3.access_key' = 'admin',
                        's3.secret_key' = 'password',
                        's3.region' = 'us-east-1'
                    )
                """
                
                sql """SWITCH ${mysql_catalog_name}"""
                
                String mysql_db_name = "mysql_test_db"
                sql """DROP DATABASE IF EXISTS ${mysql_db_name} FORCE"""
                sql """CREATE DATABASE ${mysql_db_name}"""
                sql """USE ${mysql_db_name}"""
                
                sql """DROP TABLE IF EXISTS test_mysql_catalog"""
                sql """
                    CREATE TABLE test_mysql_catalog (
                        id INT,
                        name STRING,
                        ts DATETIME
                    ) PROPERTIES (
                        'write-format' = 'parquet'
                    )
                """
                
                sql """
                    INSERT INTO test_mysql_catalog VALUES
                    (1, 'Alice', '2025-01-01 10:00:00'),
                    (2, 'Bob', '2025-01-02 11:00:00')
                """
                
                order_qt_mysql_select """SELECT * FROM test_mysql_catalog ORDER BY id"""
                
                sql """DROP TABLE IF EXISTS test_mysql_catalog"""
                sql """DROP DATABASE IF EXISTS ${mysql_db_name} FORCE"""
                
                logger.info("Iceberg JDBC Catalog (MySQL) test completed successfully")
            } catch (Exception e) {
                logger.warn("MySQL Catalog test failed: ${e.message}")
                // Don't fail the whole suite if MySQL is optional or misconfigured
                // But user asked for it, so maybe we should let it fail or log error
                throw e
            } finally {
                try {
                    sql """SWITCH internal"""
                    sql """DROP CATALOG IF EXISTS ${mysql_catalog_name}"""
                } catch (Exception e) {
                    logger.warn("Failed to cleanup MySQL catalog: ${e.message}")
                }
            }
        }

    } finally {
        // Cleanup
        try {
            sql """SWITCH internal"""
            sql """DROP CATALOG IF EXISTS ${catalog_name}"""
        } catch (Exception e) {
            logger.warn("Failed to cleanup catalog: ${e.message}")
        }
    }
}
