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

suite("test_iceberg_jdbc_catalog", "p0,external") {
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

    // This suite was written against the dynamic overwrite scope (the previous default). Pin it explicitly
    // now that iceberg_partition_overwrite_mode defaults to 'static'.
    sql """ set iceberg_partition_overwrite_mode = 'dynamic' """

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

    String catalog_name = "test_iceberg_jdbc_catalog_doris"
    String iceberg_jdbc_catalog_name = "test_iceberg_jdbc_catalog"
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
    Set<String> localHostIps = ["127.0.0.1", "localhost", "::1"] as Set
    java.util.Collections.list(java.net.NetworkInterface.getNetworkInterfaces()).each { networkInterface ->
        java.util.Collections.list(networkInterface.getInetAddresses()).each { address ->
            localHostIps.add(address.getHostAddress().split("%")[0])
        }
    }
    localHostIps.add(java.net.InetAddress.getLocalHost().getHostName())
    localHostIps.add(java.net.InetAddress.getLocalHost().getCanonicalHostName())

    executeCommand("mkdir -p ${local_driver_dir}", false)
    if (!new File(local_driver_path).exists()) {
        executeCommand("/usr/bin/curl --max-time 600 ${driver_download_url} --output ${local_driver_path}", true)
    }
    if (!new File(local_mysql_driver_path).exists()) {
        executeCommand("/usr/bin/curl --max-time 600 ${mysql_driver_download_url} --output ${local_mysql_driver_path}", true)
    }
    for (def ip in host_ips) {
        // Scenario: every FE/BE receives the JDBC drivers so distributed scans and FE failover
        // do not depend on the regression runner sharing a filesystem with one cluster node.
        if (localHostIps.contains(ip)) {
            // A local test node must not require root SSH merely to install a JDBC driver.
            executeCommand("mkdir -p ${jdbc_drivers_dir}", true)
            executeCommand("cp -f ${local_driver_path} ${jdbc_drivers_dir}/${driver_name}", true)
            executeCommand("cp -f ${local_mysql_driver_path} ${jdbc_drivers_dir}/${mysql_driver_name}", true)
        } else {
            executeCommand("ssh -o BatchMode=yes -o StrictHostKeyChecking=no root@${ip} \"mkdir -p ${jdbc_drivers_dir}\"", true)
            scpFiles("root", ip, local_driver_path, jdbc_drivers_dir, false)
            scpFiles("root", ip, local_mysql_driver_path, jdbc_drivers_dir, false)
        }
    }
    
    // Run the same commit/read-back checks against both JDBC metadata implementations.
    def checkCapabilities = { String backend, String dorisCatalog, String metadataCatalog, String database ->
        def check = { String name, String query ->
            quickTest("${backend}_${name}", query, true)
        }

        for (def variant : [[format: "parquet", version: 2], [format: "orc", version: 2],
                            [format: "parquet", version: 3], [format: "orc", version: 3]]) {
            String format = variant.format
            String prefix = "${format}_v${variant.version}"
            sql """DROP TABLE IF EXISTS jdbc_row_changes"""
            sql """
                CREATE TABLE jdbc_row_changes (id INT, category STRING, amount INT)
                PARTITION BY LIST (category) ()
                PROPERTIES (
                    'format-version' = '${variant.version}',
                    'write.format.default' = '${format}',
                    'write.delete.mode' = 'merge-on-read',
                    'write.update.mode' = 'merge-on-read',
                    'write.merge.mode' = 'merge-on-read'
                )
            """
            sql """INSERT INTO jdbc_row_changes VALUES
                    (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)"""
            sql """ALTER TABLE jdbc_row_changes CREATE TAG before_dml"""
            sql """UPDATE jdbc_row_changes SET amount = amount + 1 WHERE id = 1"""
            check("${prefix}_update", "SELECT * FROM jdbc_row_changes ORDER BY id")
            // A partial partition delete exercises V2 position deletes or V3 deletion vectors.
            sql """DELETE FROM jdbc_row_changes WHERE amount = 20"""
            check("${prefix}_delete", "SELECT * FROM jdbc_row_changes ORDER BY id")
            sql """
                MERGE INTO jdbc_row_changes t
                USING (SELECT 1 AS id, 'A' AS category, 100 AS amount, 'U' AS op
                       UNION ALL SELECT 3, 'B', 30, 'D'
                       UNION ALL SELECT 5, 'C', 50, 'I') s
                ON t.id = s.id
                WHEN MATCHED AND s.op = 'D' THEN DELETE
                WHEN MATCHED THEN UPDATE SET category = s.category, amount = s.amount
                WHEN NOT MATCHED THEN INSERT (id, category, amount) VALUES (s.id, s.category, s.amount)
            """
            check("${prefix}_merge", "SELECT * FROM jdbc_row_changes ORDER BY id")
            // Moving a row must remove it from its old partition without hiding the other rows.
            sql """UPDATE jdbc_row_changes SET category = 'D' WHERE id = 4"""
            check("${prefix}_partition_move", "SELECT * FROM jdbc_row_changes ORDER BY id")
            check("${prefix}_old_tag", "SELECT * FROM jdbc_row_changes@tag(before_dml) ORDER BY id")
            sql """UPDATE jdbc_row_changes SET amount = 0 WHERE id = 99"""
            sql """DELETE FROM jdbc_row_changes WHERE id = 99"""
            check("${prefix}_no_match", "SELECT * FROM jdbc_row_changes ORDER BY id")
        }

        sql """DROP TABLE IF EXISTS jdbc_ctas_renamed"""
        sql """DROP TABLE IF EXISTS jdbc_ctas"""
        sql """CREATE TABLE jdbc_ctas PROPERTIES ('format-version' = '2')
                AS SELECT id, category, amount FROM jdbc_row_changes"""
        sql """ALTER TABLE jdbc_ctas RENAME jdbc_ctas_renamed"""
        sql """ALTER TABLE jdbc_ctas_renamed ADD COLUMN note STRING"""
        sql """ALTER TABLE jdbc_ctas_renamed RENAME COLUMN amount total"""
        sql """ALTER TABLE jdbc_ctas_renamed MODIFY COLUMN total BIGINT"""
        check("ctas_schema", "SELECT * FROM jdbc_ctas_renamed ORDER BY id")
        sql """INSERT INTO jdbc_ctas_renamed VALUES (6, 'E', 6000000000, 'new')"""
        sql """ALTER TABLE jdbc_ctas_renamed DROP COLUMN note"""
        check("schema_write", "SELECT * FROM jdbc_ctas_renamed ORDER BY id")
        sql """REFRESH CATALOG ${dorisCatalog}"""
        check("reload", "SELECT * FROM jdbc_ctas_renamed ORDER BY id")

        sql """DROP TABLE IF EXISTS jdbc_overwrite"""
        sql """CREATE TABLE jdbc_overwrite (id INT, category STRING)
                PARTITION BY LIST (category) () PROPERTIES ('format-version' = '2')"""
        sql """INSERT INTO jdbc_overwrite VALUES (1, 'A'), (2, 'A'), (3, 'B')"""
        sql """INSERT OVERWRITE TABLE jdbc_overwrite VALUES (4, 'A')"""
        check("dynamic_overwrite", "SELECT * FROM jdbc_overwrite ORDER BY id")
        // The suite pins dynamic mode; restore it after checking static whole-table replacement.
        sql """SET iceberg_partition_overwrite_mode = 'static'"""
        try {
            sql """INSERT OVERWRITE TABLE jdbc_overwrite VALUES (5, 'C')"""
            check("static_overwrite", "SELECT * FROM jdbc_overwrite ORDER BY id")
        } finally {
            sql """SET iceberg_partition_overwrite_mode = 'dynamic'"""
        }

        sql """ALTER TABLE jdbc_overwrite ADD PARTITION KEY bucket(4, id)"""
        sql """INSERT INTO jdbc_overwrite VALUES (6, 'D')"""
        check("partition_add", "SELECT * FROM jdbc_overwrite ORDER BY id")
        sql """ALTER TABLE jdbc_overwrite DROP PARTITION KEY category"""
        sql """INSERT INTO jdbc_overwrite VALUES (7, 'E')"""
        check("partition_drop", "SELECT * FROM jdbc_overwrite ORDER BY id")
        check("partition_filter", "SELECT * FROM jdbc_overwrite WHERE category IN ('C', 'E') ORDER BY id")

        sql """DROP TABLE IF EXISTS jdbc_refs"""
        sql """CREATE TABLE jdbc_refs (id INT, name STRING)
                PROPERTIES ('format-version' = '2')"""
        sql """INSERT INTO jdbc_refs VALUES (1, 'base')"""
        String baseSnapshot = sql("""SELECT snapshot_id FROM jdbc_refs\$snapshots""")[0][0].toString()
        sql """ALTER TABLE jdbc_refs CREATE TAG base_tag"""
        sql """ALTER TABLE jdbc_refs CREATE BRANCH audit_branch"""
        sql """INSERT INTO jdbc_refs@branch(audit_branch) VALUES (2, 'branch')"""
        check("main_before_publish", "SELECT * FROM jdbc_refs ORDER BY id")
        check("branch", "SELECT * FROM jdbc_refs@branch(audit_branch) ORDER BY id")
        sql """ALTER TABLE jdbc_refs EXECUTE fast_forward('branch' = 'main', 'to' = 'audit_branch')"""
        check("main_after_publish", "SELECT * FROM jdbc_refs ORDER BY id")
        String branchSnapshot = sql("""SELECT snapshot_id FROM jdbc_refs\$refs WHERE name = 'main'""")[0][0].toString()
        check("snapshot", "SELECT * FROM jdbc_refs FOR VERSION AS OF ${baseSnapshot} ORDER BY id")
        check("tag", "SELECT * FROM jdbc_refs@tag(base_tag) ORDER BY id")
        sql """ALTER TABLE jdbc_refs DROP BRANCH audit_branch"""
        sql """ALTER TABLE jdbc_refs DROP TAG base_tag"""
        sql """ALTER TABLE jdbc_refs EXECUTE rollback_to_snapshot('snapshot_id' = '${baseSnapshot}')"""
        check("rollback", "SELECT * FROM jdbc_refs ORDER BY id")
        check("refs", "SELECT name, type FROM jdbc_refs\$refs ORDER BY name")
        sql """ALTER TABLE jdbc_refs EXECUTE expire_snapshots('snapshot_ids' = '${branchSnapshot}')"""
        check("expire_snapshot_count", "SELECT count(*) FROM jdbc_refs\$snapshots")
        check("after_expire", "SELECT * FROM jdbc_refs ORDER BY id")

        // Counts vary with writer parallelism; require actual work without recording file counts or sizes.
        quickRunTest("${backend}_rewrite_manifests_committed",
                "ALTER TABLE jdbc_row_changes EXECUTE rewrite_manifests()", false,
                { row -> [row[0].toLong() > 0] })
        check("rewrite_manifests", "SELECT * FROM jdbc_row_changes ORDER BY id")
        quickRunTest("${backend}_compact_committed",
                "ALTER TABLE jdbc_row_changes EXECUTE rewrite_data_files('min-input-files' = '1')", false,
                { row -> [row[0].toLong() > 0, row[1].toLong() > 0] })
        check("compact", "SELECT * FROM jdbc_row_changes ORDER BY id")

        sql """DROP TABLE IF EXISTS jdbc_v1"""
        sql """CREATE TABLE jdbc_v1 (id INT) PROPERTIES ('format-version' = '1')"""
        sql """INSERT INTO jdbc_v1 VALUES (1), (2)"""
        for (String statement : ["UPDATE jdbc_v1 SET id = 3 WHERE id = 1",
                                 "DELETE FROM jdbc_v1 WHERE id = 1",
                                 "MERGE INTO jdbc_v1 t USING (SELECT 1 AS id) s ON t.id = s.id "
                                         + "WHEN MATCHED THEN UPDATE SET id = 3"]) {
            test {
                sql statement
                exception "must have format version 2 or higher for position deletes"
            }
        }
        check("v1_unchanged", "SELECT * FROM jdbc_v1 ORDER BY id")

        sql """DROP TABLE IF EXISTS jdbc_cow"""
        sql """CREATE TABLE jdbc_cow (id INT) PROPERTIES (
                    'format-version' = '2',
                    'write.delete.mode' = 'copy-on-write',
                    'write.update.mode' = 'copy-on-write',
                    'write.merge.mode' = 'copy-on-write')"""
        sql """INSERT INTO jdbc_cow VALUES (1), (2)"""
        for (String statement : ["UPDATE jdbc_cow SET id = 3 WHERE id = 1",
                                 "DELETE FROM jdbc_cow WHERE id = 1",
                                 "MERGE INTO jdbc_cow t USING (SELECT 1 AS id) s ON t.id = s.id "
                                         + "WHEN MATCHED THEN UPDATE SET id = 3"]) {
            test {
                sql statement
                exception "copy-on-write"
            }
        }
        check("cow_unchanged", "SELECT * FROM jdbc_cow ORDER BY id")

        // catalog_name, rather than the Doris catalog name or JDBC URI, isolates metadata in the same database.
        sql """ALTER CATALOG ${dorisCatalog} SET PROPERTIES (
                    'iceberg.jdbc.catalog_name' = '${metadataCatalog}_isolated')"""
        try {
            check("isolated_namespaces", "SHOW DATABASES FROM ${dorisCatalog}")
        } finally {
            sql """ALTER CATALOG ${dorisCatalog} SET PROPERTIES (
                        'iceberg.jdbc.catalog_name' = '${metadataCatalog}')"""
            sql """USE ${dorisCatalog}.${database}"""
        }
        check("restored_namespace", "SELECT * FROM jdbc_ctas_renamed ORDER BY id")
    }

    String v0_writer = "test_iceberg_jdbc_v0_writer"
    String v0_reader = "test_iceberg_jdbc_v0_reader"
    String v0_database = "iceberg_jdbc_v0_test"
    String v0_uri = "jdbc:postgresql://${externalEnvIp}:${jdbc_port}/${v0_database}"
    def driverLoader = new URLClassLoader([new File(local_driver_path).toURI().toURL()] as URL[],
            this.class.classLoader)
    def pgDriver = driverLoader.loadClass("org.postgresql.Driver").getDeclaredConstructor().newInstance()
    def pgProperties = new Properties()
    pgProperties.setProperty("user", "postgres")
    pgProperties.setProperty("password", "123456")
    def pgConnection = pgDriver.connect("jdbc:postgresql://${externalEnvIp}:${jdbc_port}/${pg_db}", pgProperties)
    try {
        def statement = pgConnection.createStatement()
        try {
            // JDBC schema detection scans all PostgreSQL schemas, so isolate V0 from the V1 database.
            statement.execute("DROP DATABASE IF EXISTS ${v0_database} WITH (FORCE)")
            statement.execute("CREATE DATABASE ${v0_database}")
            statement.execute("DROP ROLE IF EXISTS iceberg_jdbc_v0_reader")
            statement.execute("CREATE ROLE iceberg_jdbc_v0_reader LOGIN PASSWORD '123456'")
        } finally {
            statement.close()
        }
        pgConnection.close()
        pgConnection = pgDriver.connect(v0_uri, pgProperties)
        statement = pgConnection.createStatement()
        try {
            // Use the PyIceberg-compatible V0 layout, which has no iceberg_type column.
            statement.execute("""CREATE TABLE iceberg_tables (
                catalog_name VARCHAR(255) NOT NULL, table_namespace VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL, metadata_location VARCHAR(1000),
                previous_metadata_location VARCHAR(1000),
                PRIMARY KEY (catalog_name, table_namespace, table_name))""")
            statement.execute("""CREATE TABLE iceberg_namespace_properties (
                catalog_name VARCHAR(255) NOT NULL, namespace VARCHAR(255) NOT NULL,
                property_key VARCHAR(255), property_value VARCHAR(1000),
                PRIMARY KEY (catalog_name, namespace, property_key))""")
            statement.execute("GRANT CONNECT ON DATABASE ${v0_database} TO iceberg_jdbc_v0_reader")
            statement.execute("GRANT USAGE ON SCHEMA public TO iceberg_jdbc_v0_reader")
            statement.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO iceberg_jdbc_v0_reader")
        } finally {
            statement.close()
        }
    } finally {
        pgConnection.close()
        driverLoader.close()
    }

    def createV0Catalog = { String name, String uri, String user ->
        sql """DROP CATALOG IF EXISTS ${name}"""
        sql """CREATE CATALOG ${name} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'jdbc',
            'uri' = '${uri}', 'warehouse' = 's3://warehouse/jdbc_v0_wh/',
            'iceberg.jdbc.driver_url' = '${driver_name}',
            'iceberg.jdbc.driver_class' = 'org.postgresql.Driver',
            'iceberg.jdbc.user' = '${user}', 'iceberg.jdbc.password' = '123456',
            'iceberg.jdbc.catalog_name' = 'jdbc_v0_catalog',
            'iceberg.jdbc.init-catalog-tables' = 'false', 'iceberg.jdbc.schema-version' = 'V0',
            's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.region' = 'us-east-1', 'use_path_style' = 'true')"""
    }

    try {
        createV0Catalog(v0_writer, v0_uri, "postgres")
        sql """CREATE DATABASE IF NOT EXISTS ${v0_writer}.v0_db"""
        sql """CREATE TABLE ${v0_writer}.v0_db.v0_table (id INT) PROPERTIES ('format-version' = '2')"""
        sql """INSERT INTO ${v0_writer}.v0_db.v0_table VALUES (1), (2)"""
        // PostgreSQL enforces read-only transactions, so accidental catalog migration or writes fail.
        createV0Catalog(v0_reader, v0_uri + "?options=-c%20default_transaction_read_only%3Don",
                "iceberg_jdbc_v0_reader")
        assertTrue(sql("SHOW DATABASES FROM ${v0_reader}").collect { it[0] }.contains("v0_db"))
        assertEquals([["v0_table"]], sql("SHOW TABLES FROM ${v0_reader}.v0_db"))
        assertEquals("id", sql("DESC ${v0_reader}.v0_db.v0_table")[0][0])
        assertEquals([[1], [2]], sql("SELECT * FROM ${v0_reader}.v0_db.v0_table ORDER BY id"))
        def v0_snapshot = sql("SELECT snapshot_id FROM ${v0_reader}.v0_db.v0_table\$snapshots")[0][0]
        assertEquals([[1], [2]], sql("""SELECT * FROM ${v0_reader}.v0_db.v0_table
            FOR VERSION AS OF ${v0_snapshot} ORDER BY id"""))
        sql """INSERT INTO ${v0_writer}.v0_db.v0_table VALUES (3)"""
        sql """REFRESH CATALOG ${v0_reader}"""
        assertEquals([[3L]], sql("SELECT COUNT(*) FROM ${v0_reader}.v0_db.v0_table"))
        assertEquals([[2L]], sql("""SELECT COUNT(*) FROM ${v0_reader}.v0_db.v0_table
            FOR VERSION AS OF ${v0_snapshot}"""))
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
                'iceberg.jdbc.catalog_name' = '${iceberg_jdbc_catalog_name}',
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
        // A JDBC catalog stores namespace properties, but would only record a location without placing
        // new tables under it.
        test {
            sql """CREATE DATABASE ${db_name} PROPERTIES (
                    'location' = 's3://warehouse/rejected_database_location/'
                )"""
            exception "database property 'location' for iceberg catalog type: jdbc"
        }
        sql """CREATE DATABASE ${db_name} PROPERTIES ('owner' = 'doris')"""
        
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

        // Scenario TC09-JDBC: schema evolution and historical binding work through JDBC catalog.
        String jdbcOldSnapshot = sql("""
            select snapshot_id
            from test_datatypes\$snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
        sql """alter table test_datatypes create tag jdbc_before_rename"""
        sql """alter table test_datatypes rename column c_string jdbc_renamed_string"""
        assertEquals([["hello"], ["world"], ["test"]], sql("""
            select c_string
            from test_datatypes for version as of ${jdbcOldSnapshot}
            order by c_int
        """))
        assertEquals([["hello"], ["world"], ["test"]], sql("""
            select c_string
            from test_datatypes@tag(jdbc_before_rename)
            order by c_int
        """))
        assertEquals([["hello"], ["world"], ["test"]], sql("""
            select jdbc_renamed_string from test_datatypes order by c_int
        """))
        test {
            sql """select c_string from test_datatypes"""
            exception "Unknown column 'c_string'"
        }

        // Test: INSERT OVERWRITE
        sql """
            INSERT OVERWRITE TABLE test_partitioned
            SELECT * FROM test_partitioned WHERE category = 'A'
        """
        order_qt_after_overwrite """SELECT * FROM test_partitioned ORDER BY id"""

        checkCapabilities("postgresql", catalog_name, iceberg_jdbc_catalog_name, db_name)

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

            String mysql_catalog_name = "iceberg_jdbc_mysql_doris"
            String iceberg_jdbc_mysql_catalog_name = "iceberg_jdbc_mysql"
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
                        'iceberg.jdbc.catalog_name' = '${iceberg_jdbc_mysql_catalog_name}',
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
                sql """CREATE DATABASE ${mysql_db_name} PROPERTIES ('owner' = 'doris')"""
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
                
                checkCapabilities("mysql", mysql_catalog_name, iceberg_jdbc_mysql_catalog_name, mysql_db_name)

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
            sql """DROP CATALOG IF EXISTS ${v0_reader}"""
            sql """DROP CATALOG IF EXISTS ${v0_writer}"""
        } catch (Exception e) {
            logger.warn("Failed to cleanup catalog: ${e.message}")
        }
    }
}
