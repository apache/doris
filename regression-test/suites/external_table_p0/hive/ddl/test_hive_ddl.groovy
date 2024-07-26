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
            sql """ drop database if exists `test_hive_db` """;
            sql """ create database if not exists ${catalog_name}.`test_hive_db` """;
            def create_db_res = sql """ show create database test_hive_db """
            logger.info("${create_db_res}")
            assertTrue(create_db_res.toString().containsIgnoreCase("/user/hive/warehouse/test_hive_db.db"))
            sql """ use `test_hive_db` """
            sql """
                    CREATE TABLE test_hive_db_has_tbl (
                      `col` STRING COMMENT 'col'
                    )  ENGINE=hive
                 """
            test {
                sql """ drop database `test_hive_db` """;
                exception "java.sql.SQLException: Unexpected exception: failed to drop database from hms client. reason: org.apache.hadoop.hive.metastore.api.InvalidOperationException: Database test_hive_db is not empty. One or more tables exist."
            }

            sql """ DROP TABLE `test_hive_db_has_tbl` """
            sql """ drop database `test_hive_db` """;
            sql """ drop database if exists `test_hive_db` """;
        }

        def test_loc_db = { String externalEnvIp, String hdfs_port, String catalog_name ->
            logger.info("Test create/drop database with location...")
            sql """switch ${catalog_name}"""
            def loc = "${externalEnvIp}:${hdfs_port}/tmp/hive/test_hive_loc_db"
            sql """ create database if not exists `test_hive_loc_db`
                    properties('location'='hdfs://${loc}')
                """

            def create_db_res = sql """show create database test_hive_loc_db"""
            logger.info("${create_db_res}")
            assertTrue(create_db_res.toString().containsIgnoreCase("${loc}"))

            sql """use `test_hive_loc_db`"""
            sql """ drop database if exists `test_hive_loc_db` """;

            sql """ create database if not exists `test_hive_loc_no_exist`
                properties('location'='hdfs://${externalEnvIp}:${hdfs_port}/exist_check')
            """
            sql """ create database if not exists `test_hive_loc_exist`
                properties('location'='hdfs://${externalEnvIp}:${hdfs_port}/exist_check')
            """
            sql """ drop database if exists `test_hive_loc_no_exist` """;
            sql """ drop database if exists `test_hive_loc_exist` """;

            try {
                sql """ create database if not exists `test_hive_loc_no_exist`
                        properties('location'='tt://${externalEnvIp}:${hdfs_port}/exist_check')
                    """
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("No FileSystem for scheme"))
            }
            try {
                sql """ create database if not exists `test_hive_loc_no_exist`
                        properties('location'='hdfs://err_${externalEnvIp}:${hdfs_port}/exist_check')
                    """
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Incomplete HDFS URI, no host"))
            }
            try {
                sql """ create database if not exists `test_hive_loc_no_exist`
                        properties('location'='hdfs:err//${externalEnvIp}:${hdfs_port}/exist_check')
                    """
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Relative path in absolute URI"))
            }
        }

        def test_tbl_default_val = { String file_format, String externalEnvIp, String hms_port,
                                     String hdfs_port, String catalog_name ->

            // create and insert default value is supported on hive3, we can test default hive version 2.3
            sql """switch ${catalog_name}"""
            sql  """ create database if not exists `test_hive_default_val`
                 """
            sql """use `test_hive_default_val`"""
            test {
                sql """ 
                        CREATE TABLE all_default_values_${file_format}_hive2(
                          `col1` BOOLEAN DEFAULT 'false' COMMENT 'col1',
                          `col2` TINYINT DEFAULT '127' COMMENT 'col2'
                        )  ENGINE=hive
                        PROPERTIES (
                          'file_format'='${file_format}'
                        )
                    """
                exception "java.sql.SQLException: errCode = 2, detailMessage = errCode = 2, detailMessage = failed to create table from hms client. reason: java.lang.UnsupportedOperationException: Table with default values is not supported if the hive version is less than 3.0. Can set 'hive.version' to 3.0 in properties."
            }
            sql """DROP DATABASE `test_hive_default_val`"""

            test {
                sql """
                CREATE TABLE all_default_values_${file_format}_err_bool(
                  `col1` BOOLEAN DEFAULT '-1' COMMENT 'col1'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = Invalid BOOLEAN literal: -1"
            }

            test {
                sql """
                CREATE TABLE all_default_values_${file_format}_err_float(
                  `col1` FLOAT DEFAULT '1.1234' COMMENT 'col1'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = Default value will loose precision: 1.1234"
            }

            test {
                sql """
                CREATE TABLE all_default_values_${file_format}_err_double(
                  `col1` DOUBLE DEFAULT 'abc' COMMENT 'col1'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = Invalid floating-point literal: abc"
            }

            test {
                sql """
                CREATE TABLE all_default_values_${file_format}_err_int(
                  `col1` INT DEFAULT 'abcd' COMMENT 'col1'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = Invalid number format: abcd"
            }

            test {
                sql """
                CREATE TABLE all_default_values_${file_format}_err_date(
                  `col1` DATE DEFAULT '123' COMMENT 'col1'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = date literal [123] is invalid: null"
            }

            test {
                sql """
                CREATE TABLE all_default_values_${file_format}_err_datetime(
                   `col1` DATETIME DEFAULT '1512561000000' COMMENT 'col1'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = date literal [1512561000000] is invalid: errCode = 2, detailMessage = Invalid date value: 1512561000000"
            }

            test {
                sql """
                CREATE TABLE all_default_values_${file_format}_err_datetime(
                   `col1` DATETIME DEFAULT '2020-09-20 02:60' COMMENT 'col1'
                )  ENGINE=hive 
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = date literal [2020-09-20 02:60] is invalid: Text '2020-09-20 02:60' could not be parsed: Invalid value for MinuteOfHour (valid values 0 - 59): 60"
            }

            // test 'hive.version' = '3.0'
            //            sql """drop catalog if exists ${catalog_name}_hive3"""
            //            sql """create catalog if not exists ${catalog_name}_hive3 properties (
            //                'type'='hms',
            //                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            //                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
            //                'hive.version' = '3.0'
            //            );"""
            //            sql """ switch ${catalog_name}_hive3 """
            //            sql  """ create database if not exists `test_hive_default_val_hive3`
            //                 """
            //            sql """use `test_hive_default_val_hive3`"""
            //            // test create hive3 table when use 'hive.version' = '3.0'
            //            sql """
            //                CREATE TABLE all_default_values_${file_format}(
            //                  `col1` BOOLEAN DEFAULT 'false' COMMENT 'col1',
            //                  `col2` TINYINT DEFAULT '127' COMMENT 'col2',
            //                  `col3` SMALLINT DEFAULT '32767' COMMENT 'col3',
            //                  `col4` INT DEFAULT '2147483647' COMMENT 'col4',
            //                  `col5` BIGINT DEFAULT '9223372036854775807' COMMENT 'col5',
            //                  `col6` CHAR(10) DEFAULT 'default' COMMENT 'col6',
            //                  `col7` FLOAT DEFAULT '1' COMMENT 'col7',
            //                  `col8` DOUBLE DEFAULT '3.141592653' COMMENT 'col8',
            //                  `col9` DECIMAL(9,4) DEFAULT '99999.9999' COMMENT 'col9',
            //                  `col10` VARCHAR(11) DEFAULT 'default' COMMENT 'col10',
            //                  `col11` STRING DEFAULT 'default' COMMENT 'col11',
            //                  `col12` DATE DEFAULT '2023-05-29' COMMENT 'col12',
            //                  `col13` DATETIME DEFAULT current_timestamp COMMENT 'col13'
            //                )  ENGINE=hive
            //                PROPERTIES (
            //                  'file_format'='${file_format}'
            //                )
            //                """
            //            // TODO: work on hive3
            //            sql """ INSERT INTO all_default_values_${file_format}
            //                    VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null)
            //                """
            //            sql """ INSERT INTO all_default_values_${file_format} (col1, col3, col5, col7, col12)
            //                    VALUES(false, null, 3.4, null, null)
            //                """
            //            sql """ INSERT INTO all_default_values_${file_format} (col2, col4, col6, col9, col11)
            //                    VALUES(-128, null, 'A', null, '2024-07-30')
            //                """
            //            order_qt_default_val01 """ SELECT * FROM all_default_values_${file_format} """
            //            test {
            //                sql """ INSERT INTO all_default_values_${file_format} (col2, col4, col6, col9, col11)
            //                    VALUES("123", "abcd", 'Ab', null, '2024-07-30')
            //                """
            //                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = Invalid number format: abcd"
            //            }
            //
            //            sql """DROP TABLE `all_default_values_${file_format}`"""
        }

        def test_loc_tbl = { String file_format, String externalEnvIp, String hdfs_port, String catalog_name ->
            logger.info("Test create/drop table with location...")
            sql """switch ${catalog_name}"""
            def loc = "${externalEnvIp}:${hdfs_port}/tmp/hive/test_hive_loc_db"
            sql  """ create database if not exists `test_hive_loc`
                    properties('location'='hdfs://${loc}')
                 """
            sql """use `test_hive_loc`"""

            // case1. the table default location is inherited from db
            sql """DROP TABLE IF EXISTS `loc_tbl_${file_format}_default`"""
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
            assertTrue(create_tbl_res.toString().containsIgnoreCase("${loc}/loc_tbl_${file_format}_default"))

            sql """ INSERT INTO loc_tbl_${file_format}_default values(1)  """

            def tvfRes = sql """ SELECT * FROM hdfs(
                                  'uri'='hdfs://${loc}/loc_tbl_${file_format}_default/*',
                                  'format' = '${file_format}',
                                  'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
                                 )
                             """
            logger.info("${tvfRes}")
            assertTrue(!tvfRes.isEmpty())
            sql """DROP TABLE `loc_tbl_${file_format}_default`"""
            def tvfDropRes = sql """ SELECT * FROM hdfs(
                                  'uri'='hdfs://${loc}/loc_tbl_${file_format}_default/*',
                                  'format' = '${file_format}',
                                  'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
                                 )
                             """
            logger.info("${tvfDropRes}")
            assertTrue(tvfDropRes.isEmpty())

            // case2. use a custom location to create table
            def tbl_loc = "hdfs://${loc}/custom_loc"
            sql """ DROP TABLE IF EXISTS loc_tbl_${file_format}_customm"""
            sql """
                    CREATE TABLE loc_tbl_${file_format}_custom (
                      `col` STRING COMMENT 'col'
                    )  ENGINE=hive 
                    PROPERTIES (
                      'file_format'='${file_format}',
                      'location'='${tbl_loc}'
                    )
                 """
            def create_tbl_res2 = sql """ show create table loc_tbl_${file_format}_custom """
            logger.info("${create_tbl_res2}")
            assertTrue(create_tbl_res2.toString().containsIgnoreCase("${tbl_loc}"))
            sql """ INSERT INTO loc_tbl_${file_format}_custom values(1)  """
            def tvfRes2 = sql """ SELECT * FROM hdfs(
                                    'uri'='${tbl_loc}/*',
                                    'format' = '${file_format}',
                                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
                                  )
                              """
            logger.info("${tvfRes2}")
            assertTrue(!tvfRes2.isEmpty())
            sql """DROP TABLE `loc_tbl_${file_format}_custom`"""
            def tvfDropRes2 = sql """ SELECT * FROM hdfs(
                                        'uri'='${tbl_loc}/*',
                                        'format' = '${file_format}',
                                        'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
                                      )
                                  """
            logger.info("${tvfDropRes2}")
            assertTrue(tvfDropRes2.isEmpty())

            // case3. check some exceptions
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
            sql """ DROP DATABASE IF EXISTS `test_hive_loc` """
        }

        def test_tbl_compress = { String compression, String file_format, String catalog_name ->
            logger.info("Test create table with compression...")
            sql """ switch ${catalog_name} """
            sql  """ create database if not exists `test_hive_compress`
                 """
            sql """use `test_hive_compress`"""

            // check table compression here, write/test_hive_write_insert.groovy contains the insert into compression
            sql """ DROP TABLE IF EXISTS tbl_${file_format}_${compression} """
            sql """
                    CREATE TABLE tbl_${file_format}_${compression} (
                      `col` STRING COMMENT 'col'
                    )  ENGINE=hive 
                    PROPERTIES (
                      'file_format'='${file_format}',
                      'compression'='${compression}'
                    )
                 """
            def create_tbl_res = sql """ show create table tbl_${file_format}_${compression} """
            logger.info("${create_tbl_res}")
            if (file_format.equals("parquet")) {
                assertTrue(create_tbl_res.toString().containsIgnoreCase("'parquet.compression'='${compression}'"))
            } else if (file_format.equals("orc")) {
                assertTrue(create_tbl_res.toString().containsIgnoreCase("'orc.compress'='${compression}'"))
            } else {
                throw new Exception("Invalid compression type: ${compression} for tbl_${file_format}_${compression}")
            }

            sql """DROP TABLE `tbl_${file_format}_${compression}`"""
            sql """ drop database if exists `test_hive_compress` """;
        }

        def test_create_tbl_cross_catalog = { String file_format, String catalog_name ->
            sql """switch ${catalog_name}"""
            sql """ CREATE DATABASE IF NOT EXISTS `test_olap_cross_catalog` """;
            sql """ USE test_olap_cross_catalog """;
            test {
                sql """
                        CREATE TABLE `test_olap_cross_catalog_tbl` (
                            `col1` INT COMMENT 'col1',
                            `col2` STRING COMMENT 'col2'
                        )
                        ENGINE=olap
                        DISTRIBUTED BY HASH(col1) BUCKETS 16
                        PROPERTIES (
                            'replication_num' = '1'
                        );
                    """
                exception "Cannot create olap table out of internal catalog. Make sure 'engine' type is specified when use the catalog: test_hive_ddl"
            }

            // test default engine is hive in hive catalog
            sql """
                    CREATE TABLE `test_olap_cross_catalog_tbl` (
                        `col1` INT COMMENT 'col1',
                        `col2` STRING COMMENT 'col2'
                    )
                """
            sql """ DROP TABLE `test_olap_cross_catalog_tbl`
                """

            test {
                sql """
                        CREATE TABLE `test_olap_cross_catalog_tbl` (
                            `col1` INT COMMENT 'col1',
                            `col2` STRING COMMENT 'col2'
                        ) DISTRIBUTED BY HASH(col1) BUCKETS 16
                        PROPERTIES (
                            'replication_num' = '1'
                        );
                    """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = errCode = 2, detailMessage = Create hive bucket table need set enable_create_hive_bucket_table to true"
            }

            sql """ SWITCH internal """
            sql """ CREATE DATABASE IF NOT EXISTS test_hive_cross_catalog """;
            sql """ USE internal.test_hive_cross_catalog """

            test {
                sql """
                        CREATE TABLE test_hive_cross_catalog_tbl (
                          `col` STRING COMMENT 'col'
                        )  ENGINE=hive 
                    """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = Cannot create hive table in internal catalog, should switch to hive catalog."
            }

            sql """ DROP DATABASE IF EXISTS test_olap_cross_catalog """
            sql """ DROP DATABASE IF EXISTS test_hive_cross_catalog """
        }

        def test_db_tbl = { String file_format,  String externalEnvIp, String hdfs_port, String catalog_name ->
            logger.info("Test create/drop table...")
            sql """switch ${catalog_name}"""
            sql """ create database if not exists `test_hive_db_tbl` """;
            sql """use `${catalog_name}`.`test_hive_db_tbl`"""

            sql """ drop table if exists unpart_tbl_${file_format}"""
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
                """

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

            // test partitions
            sql """
                CREATE TABLE all_part_types_tbl_${file_format}(
                  `col` INT COMMENT 'col',
                  `pt1` BOOLEAN COMMENT 'pt1',
                  `pt2` TINYINT COMMENT 'pt2',
                  `pt3` SMALLINT COMMENT 'pt3',
                  `pt4` INT COMMENT 'pt4',
                  `pt5` BIGINT COMMENT 'pt5',
                  `pt6` DATE COMMENT 'pt6',
                  `pt7` DATETIME COMMENT 'pt7',
                  `pt8` CHAR COMMENT 'pt8',
                  `pt9` VARCHAR COMMENT 'pt9',
                  `pt10` STRING COMMENT 'pt10'
                )  ENGINE=hive 
                PARTITION BY LIST (pt1, pt2, pt3, pt4, pt5, pt6, pt7, pt8, pt9, pt10) ()
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """
            sql """ 
                    INSERT INTO all_part_types_tbl_${file_format} (`col`, `pt1`, `pt2`, `pt3`, `pt4`, `pt5`, `pt6`, `pt7`, `pt8`, `pt9`, `pt10`)
                    VALUES
                    (1, true, 1, 123, 456789, 922232355, '2024-04-09', '2024-04-09 12:34:56', 'A', 'example', 'string_value');
                """
            def loc = "hdfs://${externalEnvIp}:${hdfs_port}/user/hive/warehouse/test_hive_db_tbl.db/all_part_types_tbl_${file_format}/pt1=1/pt2=1/pt3=123/pt4=456789/pt5=922232355/pt6=2024-04-09/*/pt8=A/pt9=example/pt10=string_value"
            def pt_check = sql """ SELECT * FROM hdfs(
                                    'uri'='${loc}/*',
                                    'format' = '${file_format}',
                                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
                                  )
                               """
            logger.info("${pt_check}")
            assertEquals(1, pt_check[0].size())
            sql """ drop table if exists all_part_types_tbl_${file_format} """

            test {
                sql """
                    CREATE TABLE all_part_types_tbl_${file_format}_err3(
                      `col` INT COMMENT 'col',
                      `pt1` STRING COMMENT 'pt1'
                    )  ENGINE=hive 
                    PARTITION BY LIST (pt000) ()
                    PROPERTIES (
                      'file_format'='${file_format}'
                    )
                    """
                exception "errCode = 2, detailMessage = partition key pt000 is not exists"
            }

            test {
                sql """ 
                    CREATE TABLE all_part_types_tbl_${file_format}_err3(
                      `col` INT COMMENT 'col',
                      `pt1` STRING COMMENT 'pt1'
                    )  ENGINE=hive 
                    PARTITION BY LIST (pt1)
                    (PARTITION pp VALUES IN ('2014-01-01'))
                    PROPERTIES (
                      'file_format'='${file_format}'
                    ) 
                    """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = errCode = 2, detailMessage = Partition values expressions is not supported in hive catalog."
            }

            test {
                sql """ 
                    CREATE TABLE all_part_types_tbl_${file_format}_err3(
                      `col` INT COMMENT 'col',
                      `pt1` STRING COMMENT 'pt1'
                    )  ENGINE=hive 
                    PARTITION BY LIST (pt000)
                    (PARTITION pp VALUES IN ('2014-01-01'))
                    PROPERTIES (
                      'file_format'='${file_format}'
                    ) 
                    """
                exception "errCode = 2, detailMessage = partition key pt000 is not exists"
            }

            test {
                sql """
                    CREATE TABLE all_part_types_tbl_${file_format}_err1(
                      `col` INT COMMENT 'col',
                      `pt1` LARGEINT COMMENT 'pt1'
                    )  ENGINE=hive 
                    PARTITION BY LIST (pt1) ()
                    PROPERTIES (
                      'file_format'='${file_format}'
                    )
                    """
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = failed to create table from hms client. reason: org.apache.doris.datasource.hive.HMSClientException: Unsupported primitive type conversion of largeint"
            }

            test {
                sql """
                    CREATE TABLE all_part_types_tbl_${file_format}_err2(
                      `col` INT COMMENT 'col',
                      `pt1` FLOAT COMMENT 'pt1'
                    )  ENGINE=hive 
                    PARTITION BY LIST (pt1) ()
                    PROPERTIES (
                      'file_format'='${file_format}'
                    )
                    """
                exception "errCode = 2, detailMessage = Floating point type column can not be partition column"
            }

            test {
                sql """
                    CREATE TABLE all_part_types_tbl_${file_format}_err3(
                      `col` INT COMMENT 'col',
                      `pt1` DOUBLE COMMENT 'pt1'
                    )  ENGINE=hive 
                    PARTITION BY LIST (pt1) ()
                    PROPERTIES (
                      'file_format'='${file_format}'
                    )
                    """
                exception "errCode = 2, detailMessage = Floating point type column can not be partition column"
            }

            sql """ drop database if exists `test_hive_db_tbl` """;
        }


        try {
            String hms_port = context.config.otherConfigs.get("hive2HmsPort")
            String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
            String catalog_name = "test_hive_ddl"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
            sql """switch ${catalog_name}"""

            sql """set enable_fallback_to_original_planner=false;"""
            test_db(catalog_name)
            test_loc_db(externalEnvIp, hdfs_port, catalog_name)
            def compressions = ["snappy", "zlib", "zstd"]
            for (String file_format in file_formats) {
                logger.info("Process file format " + file_format)
                test_loc_tbl(file_format, externalEnvIp, hdfs_port, catalog_name)
                test_tbl_default_val(file_format, externalEnvIp, hms_port, hdfs_port, catalog_name)
                test_db_tbl(file_format, externalEnvIp, hdfs_port, catalog_name)

                for (String compression in compressions) {
                    if (file_format.equals("parquet") && compression.equals("zlib")) {
                        continue
                    }
                    logger.info("Process file compression " + compression)
                    test_tbl_compress(compression, file_format, catalog_name)
                }
                test_create_tbl_cross_catalog(file_format, catalog_name)
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
