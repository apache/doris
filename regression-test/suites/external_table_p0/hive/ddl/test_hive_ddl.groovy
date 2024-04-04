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

suite("test_hive_ddl", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def file_formats = ["parquet", "orc"]

        def test_db = { String catalog_name ->
            logger.info("Test create/drop database...")
            sql """switch ${catalog_name}"""
            sql """ create database if not exists ${catalog_name}.`test_hive_db` """;
            def create_db_res = sql """show create database test_hive_db"""
            logger.info("${create_db_res}")
            assertTrue(create_db_res.toString().containsIgnoreCase("/user/hive/warehouse/test_hive_db.db"))
            sql """use `test_hive_db`"""
            sql """ drop database if exists `test_hive_db` """;
        }

        def test_loc_db = { String externalEnvIp, String hdfs_port, String catalog_name ->
            logger.info("Test create/drop database with location...")
            sql """switch ${catalog_name}"""
            def loc = "${externalEnvIp}:${hdfs_port}/tmp/hive/test_hive_loc_db"
            sql """ create database if not exists `test_hive_loc_db`
                    properties('location_uri'='hdfs://${loc}')
                """

            def create_db_res = sql """show create database test_hive_loc_db"""
            logger.info("${create_db_res}")
            assertTrue(create_db_res.toString().containsIgnoreCase("${loc}"))

            sql """use `test_hive_loc_db`"""
            sql """ drop database if exists `test_hive_loc_db` """;

            sql """ create database if not exists `test_hive_loc_no_exist`
                properties('location_uri'='hdfs://${externalEnvIp}:${hdfs_port}/exist_check')
            """
            sql """ create database if not exists `test_hive_loc_exist`
                properties('location_uri'='hdfs://${externalEnvIp}:${hdfs_port}/exist_check')
            """
            sql """ drop database if exists `test_hive_loc_no_exist` """;
            sql """ drop database if exists `test_hive_loc_exist` """;

            try {
                sql """ create database if not exists `test_hive_loc_no_exist`
                        properties('location_uri'='tt://${externalEnvIp}:${hdfs_port}/exist_check')
                    """
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("No FileSystem for scheme: tt"))
            }
            try {
                sql """ create database if not exists `test_hive_loc_no_exist`
                        properties('location_uri'='hdfs://err_${externalEnvIp}:${hdfs_port}/exist_check')
                    """
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Incomplete HDFS URI, no host"))
            }
            try {
                sql """ create database if not exists `test_hive_loc_no_exist`
                        properties('location_uri'='hdfs:err//${externalEnvIp}:${hdfs_port}/exist_check')
                    """
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Relative path in absolute URI"))
            }
        }

        def test_loc_tbl = { String file_format, String externalEnvIp, String hdfs_port, String catalog_name ->
            logger.info("Test create/drop table with location...")
            sql """switch ${catalog_name}"""
            def loc = "${externalEnvIp}:${hdfs_port}/tmp/hive/test_hive_loc_db"
            sql  """ create database if not exists `test_hive_loc`
                    properties('location_uri'='hdfs://${loc}')
                 """
            sql """use `test_hive_loc`"""

            // case1. the table default location is inherited from db
            sql """
                    CREATE TABLE loc_tbl_${file_format}_default (
                      `col` STRING COMMENT 'col'
                    )  ENGINE=hive 
                    PROPERTIES (
                      'file_format'='${file_format}'
                    )
                 """
            def create_tbl_res = sql """ show create table loc_tbl_${file_format}_default """
            logger.info("${create_tbl_res}")
            assertTrue(create_tbl_res.toString().containsIgnoreCase("${loc}/loc_tbl_${file_format}"))
            sql """DROP TABLE `loc_tbl_${file_format}_default`"""

            // case2. use a custom location to create table
            def tbl_loc = "hdfs://${loc}/custom_loc"
            sql """
                    CREATE TABLE loc_tbl_${file_format}_custom (
                      `col` STRING COMMENT 'col'
                    )  ENGINE=hive 
                    PROPERTIES (
                      'file_format'='${file_format}',
                      'location_uri'='${tbl_loc}'
                    )
                 """
            def create_tbl_res2 = sql """ show create table loc_tbl_${file_format}_custom """
            logger.info("${create_tbl_res2}")
            assertTrue(create_tbl_res2.toString().containsIgnoreCase("${tbl_loc}"))

            sql """DROP TABLE `loc_tbl_${file_format}_custom`"""

            def comment_check = sql """ CREATE TABLE ex_tbl_${file_format}(
                                          `col1` INT COMMENT 'col1',
                                          `col2` STRING COMMENT 'col2',
                                          `pt1` VARCHAR COMMENT 'pt1'
                                        )  ENGINE=hive 
                                        COMMENT 'test'
                                        PARTITION BY LIST (pt1) ()
                                        PROPERTIES (
                                          'file_format'='${file_format}'
                                        )
                                    """
            def comment_check_res = sql """ show create table ex_tbl_${file_format} """
            logger.info("${comment_check_res}")
            assertTrue(comment_check_res.toString().containsIgnoreCase("COMMENT 'col1'"))
            assertTrue(comment_check_res.toString().containsIgnoreCase("COMMENT 'col2'"))
            sql """DROP TABLE `ex_tbl_${file_format}`"""

            test {
                sql """
                        CREATE TABLE nullable_check (
                            `col` STRING NOT NULL COMMENT 'col'
                        )  ENGINE=hive 
                        PROPERTIES (
                            'file_format'='${file_format}'
                        )
                    """
                exception "errCode = 2, detailMessage = hive catalog doesn't support column with 'NOT NULL'."
            }

            test {
                sql """
                        CREATE TABLE schema_check ENGINE=hive ;
                    """
                exception "AnalysisException, msg: Should contain at least one column in a table"
            }
            sql """ drop database if exists `test_hive_loc_db` """;
        }

        def test_db_tbl = { String file_format, String catalog_name ->
            logger.info("Test create/drop table...")
            sql """switch ${catalog_name}"""
            sql """ create database if not exists `test_hive_db_tbl` """;
            sql """use `${catalog_name}`.`test_hive_db_tbl`"""

            sql """
                CREATE TABLE unpart_tbl_${file_format}(
                  `col1` BOOLEAN COMMENT 'col1',
                  `col2` INT COMMENT 'col2',
                  `col3` BIGINT COMMENT 'col3',
                  `col4` CHAR(10) COMMENT 'col4',
                  `col5` FLOAT COMMENT 'col5',
                  `col6` DOUBLE COMMENT 'col6',
                  `col7` DECIMAL(9,4) COMMENT 'col7',
                  `col8` VARCHAR(11) COMMENT 'col8',
                  `col9` STRING COMMENT 'col9',
                  `col10` DATE COMMENT 'col10',
                  `col11` DATETIME COMMENT 'col11'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
            """;

            // test all columns
            sql """ INSERT INTO unpart_tbl_${file_format} (`col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9`) 
                    VALUES 
                    (true, 123, 9876543210, 'abcdefghij', 3.14, 6.28, 123.4567, 'varcharval', 'stringval');
                """
            order_qt_insert01 """ SELECT `col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9` FROM unpart_tbl_${file_format};  """

            // test part of columns
            sql """ INSERT INTO unpart_tbl_${file_format} (`col1`, `col2`, `col3`, `col8`, `col9`) 
                    VALUES 
                    (true, 123, 9876543210, 'varcharval', 'stringval');
                """
            sql """ INSERT INTO unpart_tbl_${file_format} (`col1`, `col2`, `col8`, `col9`) 
                    VALUES 
                    (null, 123, 'varcharval', 8.98);
                """
            order_qt_insert02 """ SELECT `col1`, `col2`, `col3`, `col7`, `col9` FROM unpart_tbl_${file_format};  """

            // test data diff
            sql """ INSERT INTO unpart_tbl_${file_format} (`col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9`) 
                    VALUES 
                    (true, null, 9876543210, 'abcdefghij', '2.3', 6.28, null, 'varcharval', 'stringval');
                """
            sql """ INSERT INTO unpart_tbl_${file_format} (`col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9`) 
                    VALUES 
                    (false, '1', 9876543210, 'abcdefghij', '2.3', 6.28, 0, 2223, 'stringval');
                """
            order_qt_insert03 """ SELECT `col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9` FROM unpart_tbl_${file_format} """

            sql """ drop table if exists unpart_tbl_${file_format}"""

            // partitioned table test
            sql """
                CREATE TABLE part_tbl_${file_format}(
                  `col1` BOOLEAN COMMENT 'col1',
                  `col2` INT COMMENT 'col2',
                  `col3` BIGINT COMMENT 'col3',
                  `col4` DECIMAL(2,1) COMMENT 'col4',
                  `pt1` VARCHAR COMMENT 'pt1',
                  `pt2` VARCHAR COMMENT 'pt2'
                )  ENGINE=hive 
                PARTITION BY LIST (pt1, pt2) ()
                PROPERTIES (
                  'file_format'='${file_format}'
                )
            """;

            // test all columns
            sql """ INSERT INTO part_tbl_${file_format} (col1, col2, col3, col4, pt1, pt2)
                VALUES (true, 1, 1000, 2.3, 'value_for_pt1', 'value_for_pt2')
                """
            order_qt_insert04 """ SELECT col1, col2, col3, col4, pt1, pt2 FROM part_tbl_${file_format};  """

            // test part of columns
            sql """ INSERT INTO part_tbl_${file_format} (col1, col2, col3, col4, pt1, pt2)
                VALUES (true, 1, 1000, 2.3, 'value_for_pt1', 'value_for_pt2')
                """
            sql """ INSERT INTO part_tbl_${file_format} (col1, col2, col3, col4, pt1, pt2)
                VALUES (true, 1, 1000, 2.3, 'value_for_pt1', 'value_for_pt2')
                """
            order_qt_insert05 """ SELECT col1, col2, col3, col4 FROM part_tbl_${file_format} """

            // test data diff
            sql """ INSERT INTO part_tbl_${file_format} (col1, col2, col3, col4, pt1, pt2)
                VALUES (0, '1', 1000, null, 2.56, 'value_for_pt2')
                """
            sql """ INSERT INTO part_tbl_${file_format} (col1, col2, col3, col4, pt1, pt2)
                VALUES (null, 1, '1000', '1.3', 'value_for_pt1', 2345)
                """
            order_qt_insert06 """ SELECT col1, col2, col3, col4 FROM part_tbl_${file_format}  """

            sql """ drop table if exists part_tbl_${file_format} """
            sql """ drop database if exists `test_hive_db_tbl` """;
        }


        try {
            String hms_port = context.config.otherConfigs.get("hms_port")
            String hdfs_port = context.config.otherConfigs.get("hdfs_port")
            String catalog_name = "test_hive_ddl"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );"""
            sql """switch ${catalog_name}"""

            sql """set enable_fallback_to_original_planner=false;"""
            test_db(catalog_name)
            test_loc_db(externalEnvIp, hdfs_port, catalog_name)
            for (String file_format in file_formats) {
                logger.info("Process file format" + file_format)
                test_loc_tbl(file_format, externalEnvIp, hdfs_port, catalog_name)
                test_db_tbl(file_format, catalog_name)
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
