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

suite("test_hive_write_static_partition", "p0,external,hive,external_docker,external_docker_hive") {

    // Static partition insert:
    //   INSERT [OVERWRITE] TABLE t PARTITION(col='val', ...) SELECT ...
    // The partition column value comes from the PARTITION clause instead of the query output.
    def testStaticPartitionWrite = { String catalog_name ->
        String tableName = "hive_static_par_tbl"

        hive_docker """ DROP TABLE IF EXISTS ${tableName}; """
        hive_docker """
            CREATE TABLE ${tableName} (
                tag_value string,
                user_id   string,
                ts        int
            )
            PARTITIONED BY (ts_date string)
            STORED AS parquet;
        """
        sql """ refresh catalog ${catalog_name}; """

        // 1. INSERT INTO ... PARTITION(ts_date='2026-07-24') without listing the partition column in SELECT.
        //    This used to fail with "insert into cols should be corresponding to the query output".
        sql """
            INSERT INTO ${tableName} PARTITION(ts_date='2026-07-24')
            SELECT 'tagA', 'u1', 100;
        """
        // 2. Append another row into the SAME partition.
        sql """
            INSERT INTO ${tableName} PARTITION(ts_date='2026-07-24')
            SELECT 'tagB', 'u2', 200;
        """
        // 3. Insert into a DIFFERENT partition.
        sql """
            INSERT INTO ${tableName} PARTITION(ts_date='2026-07-25')
            SELECT 'tagC', 'u3', 300;
        """
        sql """ refresh catalog ${catalog_name}; """

        // Verify: 2 rows in partition 2026-07-24 and 1 row in 2026-07-25, partition column filled correctly.
        def all = sql """ select tag_value, user_id, ts, ts_date from ${tableName} order by ts_date, ts; """
        assertEquals(3, all.size())
        assertEquals("tagA", all[0][0]); assertEquals("2026-07-24", all[0][3])
        assertEquals("tagB", all[1][0]); assertEquals("2026-07-24", all[1][3])
        assertEquals("tagC", all[2][0]); assertEquals("2026-07-25", all[2][3])

        // Partition pruning should work on the statically written partition value.
        def p24 = sql """ select tag_value from ${tableName} where ts_date='2026-07-24' order by ts; """
        assertEquals(2, p24.size())

        // 4. INSERT OVERWRITE a single partition: it should only replace 2026-07-24, leaving 2026-07-25 intact.
        sql """
            INSERT OVERWRITE TABLE ${tableName} PARTITION(ts_date='2026-07-24')
            SELECT 'tagX', 'u9', 999;
        """
        sql """ refresh catalog ${catalog_name}; """

        def afterOverwrite = sql """ select tag_value, user_id, ts, ts_date from ${tableName} order by ts_date, ts; """
        assertEquals(2, afterOverwrite.size())
        // partition 2026-07-24 replaced by a single row
        assertEquals("tagX", afterOverwrite[0][0]); assertEquals(999, afterOverwrite[0][2])
        assertEquals("2026-07-24", afterOverwrite[0][3])
        // partition 2026-07-25 untouched
        assertEquals("tagC", afterOverwrite[1][0]); assertEquals("2026-07-25", afterOverwrite[1][3])

        // 5. Static partition combined with an explicit (non-partition) column list.
        sql """
            INSERT INTO ${tableName} (tag_value, user_id, ts) PARTITION(ts_date='2026-07-26')
            SELECT 'tagD', 'u4', 400;
        """
        sql """ refresh catalog ${catalog_name}; """
        def p26 = sql """ select tag_value, ts_date from ${tableName} where ts_date='2026-07-26'; """
        assertEquals(1, p26.size())
        assertEquals("tagD", p26[0][0])

        hive_docker """ DROP TABLE IF EXISTS ${tableName}; """
    }

    // Error cases for static partition validation.
    def testStaticPartitionErrors = { String catalog_name ->
        String tableName = "hive_static_par_err_tbl"
        String nonParTableName = "hive_static_par_nonpar_tbl"

        hive_docker """ DROP TABLE IF EXISTS ${tableName}; """
        hive_docker """
            CREATE TABLE ${tableName} (
                tag_value string,
                user_id   string,
                ts        int
            )
            PARTITIONED BY (ts_date string)
            STORED AS parquet;
        """
        hive_docker """ DROP TABLE IF EXISTS ${nonParTableName}; """
        hive_docker """
            CREATE TABLE ${nonParTableName} (
                tag_value string,
                user_id   string
            )
            STORED AS parquet;
        """
        sql """ refresh catalog ${catalog_name}; """

        // 5.1 partition column not exists
        test {
            sql """
                INSERT INTO ${tableName} PARTITION(not_exist_col='2026-07-24')
                SELECT 'tagA', 'u1', 100;
            """
            exception "Unknown partition column"
        }

        // 5.2 static partition column also appears in the insert column list
        test {
            sql """
                INSERT INTO ${tableName} (tag_value, user_id, ts, ts_date) PARTITION(ts_date='2026-07-24')
                SELECT 'tagA', 'u1', 100, '2026-07-24';
            """
            exception "is a static partition column"
        }

        // 5.3 use static partition syntax on a non-partitioned table
        test {
            sql """
                INSERT INTO ${nonParTableName} PARTITION(ts_date='2026-07-24')
                SELECT 'tagA', 'u1';
            """
            exception "is not a partitioned table"
        }

        hive_docker """ DROP TABLE IF EXISTS ${tableName}; """
        hive_docker """ DROP TABLE IF EXISTS ${nonParTableName}; """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_${hivePrefix}_write_static_partition"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );"""
            sql """use `${catalog_name}`.`write_test`"""
            logger.info("hive sql: use `write_test`")
            hive_docker """use `write_test`"""

            sql """set enable_fallback_to_original_planner=false;"""

            testStaticPartitionWrite(catalog_name)
            testStaticPartitionErrors(catalog_name)

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
