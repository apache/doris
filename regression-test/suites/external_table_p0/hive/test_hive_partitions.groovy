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

suite("test_hive_partitions", "p0,external,hive,external_docker,external_docker_hive") {
    def q01 = {
        qt_q01 """
        select id, data from table_with_pars where dt_par = '2023-02-01' order by id;
        """
        qt_q02 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00' order by id;
        """
        qt_q03 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00'
        and decimal_par1 = '1' order by id;
        """
        qt_q04 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00'
        and decimal_par1 = '1' and decimal_par2 = '1.2' order by id;
        """
        qt_q05 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00'
        and decimal_par1 = '1' and decimal_par2 = '1.2' and decimal_par3 = '1.22' order by id;
        """
        qt_q11 """
            show partitions from partition_table;
        """
        qt_q12 """
            show partitions from partition_table WHERE partitionName='nation=cn/city=beijing';
        """
        qt_q13 """
            show partitions from partition_table WHERE partitionName like 'nation=us/%';
        """
        qt_q14 """
            show partitions from partition_table WHERE partitionName like 'nation=%us%';
        """
        qt_q16 """
            show partitions from partition_table LIMIT 3;
        """
        qt_q17 """
            show partitions from partition_table LIMIT 3 OFFSET 2;
        """
        qt_q18 """
            show partitions from partition_table LIMIT 3 OFFSET 4;
        """
        qt_q19 """
            show partitions from partition_table ORDER BY partitionName desc LIMIT 3 OFFSET 2;
        """
        qt_q20 """
            show partitions from partition_table ORDER BY partitionName asc;
        """
        qt_q21 """
            show partitions from partition_table
                WHERE partitionName like '%X%'
                ORDER BY partitionName DESC
                LIMIT 1;
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_partitions"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            q01()

            // Test cache miss scenario: Hive adds partition, then Doris writes to it
            def test_cache_miss = {
                def dbName = "test_cache_miss_db"
                def tblName = "test_cache_miss_table"

                try {
                    // Clean up
                    hive_docker """DROP TABLE IF EXISTS ${dbName}.${tblName}"""
                    hive_docker """DROP DATABASE IF EXISTS ${dbName}"""

                    // Create database and partitioned table in Hive
                    hive_docker """CREATE DATABASE ${dbName}"""
                    hive_docker """
                        CREATE TABLE ${dbName}.${tblName} (
                            id INT,
                            name STRING
                        )
                        PARTITIONED BY (pt INT)
                        STORED AS ORC
                    """

                    // Hive writes 3 partitions
                    hive_docker """
                        INSERT INTO ${dbName}.${tblName} PARTITION(pt=1)
                        VALUES (1, 'hive_pt1')
                    """
                    hive_docker """
                        INSERT INTO ${dbName}.${tblName} PARTITION(pt=2)
                        VALUES (2, 'hive_pt2')
                    """
                    hive_docker """
                        INSERT INTO ${dbName}.${tblName} PARTITION(pt=3)
                        VALUES (3, 'hive_pt3')
                    """

                    sql """refresh catalog `${catalog_name}`"""      
                    // Doris reads data to populate cache (only knows about 3 partitions)
                    def result1 = sql """SELECT COUNT(*) as cnt FROM `${catalog_name}`.`${dbName}`.`${tblName}`"""
                    assertEquals(3, result1[0][0])
                    logger.info("Doris cache populated with 3 partitions")

                    // Hive writes 4th partition (Doris cache doesn't know about it)
                    hive_docker """
                        INSERT INTO ${dbName}.${tblName} PARTITION(pt=4)
                        VALUES (4, 'hive_pt4')
                    """
                    logger.info("Hive added 4th partition (pt=4)")

                    // Doris writes to the 4th partition
                    // This should trigger cache miss detection and treat as APPEND instead of NEW
                    sql """
                        INSERT INTO `${catalog_name}`.`${dbName}`.`${tblName}`
                        VALUES (40, 'doris_pt4', 4)
                    """
                    logger.info("Doris wrote to 4th partition (should handle cache miss)")

                    // Verify: should have 5 rows total (3 from hive + 1 from hive pt4 + 1 from doris pt4)
                    def result2 = sql """SELECT COUNT(*) as cnt FROM `${catalog_name}`.`${dbName}`.`${tblName}`"""
                    assertEquals(5, result2[0][0])

                    // Verify partition 4 has 2 rows
                    def result3 = sql """
                        SELECT COUNT(*) as cnt
                        FROM `${catalog_name}`.`${dbName}`.`${tblName}`
                        WHERE pt = 4
                    """
                    assertEquals(2, result3[0][0])

                    // Verify data content
                    def result4 = sql """
                        SELECT id, name
                        FROM `${catalog_name}`.`${dbName}`.`${tblName}`
                        WHERE pt = 4
                        ORDER BY id
                    """
                    assertEquals(2, result4.size())
                    assertEquals(4, result4[0][0])
                    assertEquals("hive_pt4", result4[0][1])
                    assertEquals(40, result4[1][0])
                    assertEquals("doris_pt4", result4[1][1])

                    logger.info("Cache miss test passed!")

                } finally {
                    // Clean up
                    try {
                        hive_docker """DROP TABLE IF EXISTS ${dbName}.${tblName}"""
                        hive_docker """DROP DATABASE IF EXISTS ${dbName}"""
                    } catch (Exception e) {
                        logger.warn("Cleanup failed: ${e.message}")
                    }
                }
            }

            test_cache_miss()

            qt_string_partition_table_with_comma """
                select * from partition_tables.string_partition_table_with_comma order by id;
            """

            sql """set num_partitions_in_batch_mode=1"""
            explain {
                sql ("select * from partition_table")
                verbose (true)

                contains "(approximate)inputSplitNum=60"
            }
            sql """unset variable num_partitions_in_batch_mode"""
        } finally {
        }
    }
}

