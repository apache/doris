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

suite("test_hive_ctas", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : [ "hive3"]) {
        def file_formats = ["parquet", "orc"]
        setHivePrefix(hivePrefix)
        def generateSrcDDLForCTAS = { String file_format, String catalog_name ->
            sql """ switch `${catalog_name}` """
            sql """ create database if not exists `test_ctas` """;
            sql """ switch internal """
            sql """ create database if not exists test_ctas_olap """;
            sql """ use internal.test_ctas_olap """
            sql """ DROP TABLE IF EXISTS internal.test_ctas_olap.unpart_ctas_olap_src """
            sql """
                CREATE TABLE `unpart_ctas_olap_src` (
                    `col1` INT COMMENT 'col1',
                    `col2` STRING COMMENT 'col2'
                )
                ENGINE=olap
                DISTRIBUTED BY HASH(col1) BUCKETS 16
                PROPERTIES (
                    'replication_num' = '1'
                );
            """

            sql """ INSERT INTO `unpart_ctas_olap_src` (col1, col2) VALUES
                (1, 'string value for col2'),
                (2, 'another string value for col2'),
                (3, 'yet another string value for col2'); 
            """
            sql """ DROP TABLE IF EXISTS internal.test_ctas_olap.part_ctas_olap_src """
            sql """
                CREATE TABLE `part_ctas_olap_src`(
                    `col1` INT COMMENT 'col1',
                    `pt1` VARCHAR(16) COMMENT 'pt1',
                    `pt2` VARCHAR(16) COMMENT 'pt2'
                )
                ENGINE=olap
                PARTITION BY LIST (pt1, pt2) (
                    PARTITION pp1 VALUES IN(
                        ('value_for_pt1', 'value_for_pt2'),
                        ('value_for_pt11', 'value_for_pt22')
                    )
                )
                DISTRIBUTED BY HASH(col1) BUCKETS 16
                PROPERTIES (
                    'replication_num' = '1'
                );
            """

            sql """
            INSERT INTO `part_ctas_olap_src` (col1, pt1, pt2) VALUES
             (11, 'value_for_pt1', 'value_for_pt2'),
             (22, 'value_for_pt11', 'value_for_pt22');
            """

            sql """ use `${catalog_name}`.`test_ctas` """
            sql """ DROP TABLE IF EXISTS `test_ctas`.unpart_ctas_src """
            sql """
                CREATE TABLE `unpart_ctas_src`(
                  `col1` INT COMMENT 'col1',
                  `col2` STRING COMMENT 'col2'
                ) ENGINE=hive
                PROPERTIES (
                  'file_format'='${file_format}'
                );
            """

            sql """ INSERT INTO `unpart_ctas_src` (col1, col2) VALUES
                (1, 'string value for col2'),
                (2, 'another string value for col2'),
                (3, 'yet another string value for col2'); 
            """

            sql """ DROP TABLE IF EXISTS `test_ctas`.part_ctas_src """
            sql """
                CREATE TABLE `part_ctas_src`(
                  `col1` INT COMMENT 'col1',
                  `pt1` VARCHAR COMMENT 'pt1',
                  `pt2` VARCHAR COMMENT 'pt2'
                ) ENGINE=hive
                PARTITION BY LIST (pt1, pt2) (
                    
                )
                PROPERTIES (
                  'file_format'='${file_format}'
                );
            """

            sql """
            INSERT INTO `part_ctas_src` (col1, pt1, pt2) VALUES
             (11, 'value_for_pt1', 'value_for_pt2'),
             (22, 'value_for_pt11', 'value_for_pt22');
            """
        }

        def destroySrcDDLForCTAS = { String catalog_name ->
            sql """ switch `${catalog_name}` """
            sql """ DROP TABLE IF EXISTS `test_ctas`.part_ctas_src """
            sql """ DROP TABLE IF EXISTS `test_ctas`.unpart_ctas_src """
            sql """ drop database if exists `test_ctas` """;
            sql """ DROP TABLE IF EXISTS internal.test_ctas_olap.part_ctas_olap_src """
            sql """ DROP TABLE IF EXISTS internal.test_ctas_olap.unpart_ctas_olap_src """
            sql """ switch internal """;
            sql """ drop database if exists test_ctas_olap """;
        }

        def test_ctas_tbl = { String file_format, String catalog_name ->
            generateSrcDDLForCTAS(file_format, catalog_name)
            try {
                sql """ switch `${catalog_name}` """
                sql """ use test_ctas """
                String db = "test_ctas"
                // 1. external to external un-partitioned table
                sql """ CREATE TABLE hive_ctas1 ENGINE=hive AS SELECT col1 FROM unpart_ctas_src; 
                """

                sql """ INSERT INTO hive_ctas1 SELECT col1 FROM unpart_ctas_src WHERE col1 > 1;
                """
                sql """ INSERT OVERWRITE TABLE hive_ctas1 SELECT col1 FROM unpart_ctas_src WHERE col1 > 1;
                """

                order_qt_ctas_01 """ SELECT * FROM hive_ctas1 """
                order_qt_hive_docker_ctas_01 """ SELECT * FROM ${db}.hive_ctas1 """
                sql """ DROP TABLE hive_ctas1 """

                // 2. external to external un-partitioned table with columns
                sql """ CREATE TABLE hive_ctas2 (col1) ENGINE=hive AS SELECT col1 FROM unpart_ctas_src; 
                """

                sql """ INSERT INTO hive_ctas2 SELECT col1 FROM unpart_ctas_src WHERE col1 > 1;
                """
                sql """ INSERT OVERWRITE TABLE hive_ctas2 SELECT col1 FROM unpart_ctas_src WHERE col1 > 1;
                """
                order_qt_ctas_02 """ SELECT * FROM hive_ctas2  """
                order_qt_hive_docker_ctas_02 """ SELECT * FROM ${db}.hive_ctas2 """
                sql """ DROP TABLE hive_ctas2 """

                // 3. external to external partitioned table
                sql """ CREATE TABLE hive_ctas3 ENGINE=hive AS SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>0;
                """

                sql """ INSERT INTO hive_ctas3 SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>=22;
                """
                sql """ INSERT OVERWRITE TABLE hive_ctas3 SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>=22;
                """
                order_qt_ctas_03 """ SELECT * FROM hive_ctas3  """
                order_qt_hive_docker_ctas_03 """ SELECT * FROM ${db}.hive_ctas3 """
                sql """ DROP TABLE hive_ctas3 """

                sql """ CREATE TABLE hive_ctas4 AS SELECT * FROM part_ctas_src WHERE col1>0;
                """

                sql """ INSERT INTO hive_ctas4 SELECT * FROM part_ctas_src WHERE col1>=22;
                """
                sql """ INSERT OVERWRITE TABLE hive_ctas4 SELECT * FROM part_ctas_src WHERE col1>=22;
                """
                order_qt_ctas_04 """ SELECT * FROM ${catalog_name}.test_ctas.hive_ctas4 """
                order_qt_hive_docker_ctas_04 """ SELECT * FROM ${db}.hive_ctas4 """
                sql """ DROP TABLE hive_ctas4 """

                // 4. external to external partitioned table with partitions and cols
                sql """ CREATE TABLE hive_ctas5 ENGINE=hive PARTITION BY LIST (pt1, pt2) ()
                AS SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>0;
                """

                sql """ INSERT INTO hive_ctas5 SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>=22;
                """
                sql """ INSERT OVERWRITE TABLE hive_ctas5 SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>=22;
                """
                order_qt_ctas_05 """ SELECT * FROM hive_ctas5  """
                order_qt_hive_docker_ctas_05 """ SELECT * FROM ${db}.hive_ctas5 """
                sql """ DROP TABLE hive_ctas5 """

                sql """ CREATE TABLE hive_ctas6 PARTITION BY LIST (pt1, pt2) ()
                AS SELECT * FROM part_ctas_src WHERE col1>0;
                """

                sql """ INSERT INTO hive_ctas6 SELECT * FROM part_ctas_src WHERE col1>=22;
                """
                sql """ INSERT OVERWRITE TABLE hive_ctas6 SELECT * FROM part_ctas_src WHERE col1>=22;
                """
                order_qt_ctas_06 """ SELECT * FROM ${catalog_name}.test_ctas.hive_ctas6 """
                order_qt_hive_docker_ctas_06 """ SELECT * FROM ${db}.hive_ctas6 """
                sql """ DROP TABLE hive_ctas6 """

            } finally {
                destroySrcDDLForCTAS(catalog_name)
            }
        }

        def test_ctas_extend = { String file_format, String catalog_name ->
            generateSrcDDLForCTAS(file_format, catalog_name)
            sql """ switch ${catalog_name} """

            try {
                sql """ DROP DATABASE IF EXISTS ${catalog_name}.test_ctas_ex """;
                sql """ DROP DATABASE IF EXISTS `test_ctas_ex` """;
                sql """ CREATE DATABASE IF NOT EXISTS ${catalog_name}.test_ctas_ex
                    PROPERTIES (
                        "location" = "/user/hive/warehouse/test_ctas_ex"
                    )
                    """;
                sql """ CREATE DATABASE IF NOT EXISTS `test_ctas_ex`
                    PROPERTIES (
                        "location" = "/user/hive/warehouse/test_ctas_ex"
                    )
                    """;
                sql """ use `${catalog_name}`.`test_ctas_ex` """
                String db = "test_ctas_ex"

                // 1. external to external un-partitioned table
                sql """ DROP TABLE IF EXISTS ${catalog_name}.test_ctas_ex.hive_ctas1 """
                sql """ CREATE TABLE ${catalog_name}.test_ctas_ex.hive_ctas1 (col1) ENGINE=hive 
                        PROPERTIES (
                            "location" = "/user/hive/warehouse/test_ctas_ex/loc_hive_ctas1",
                            "file_format"="orc",
                            "orc.compress"="zlib"
                        ) AS SELECT col1 FROM test_ctas.unpart_ctas_src; 
                    """
                sql """ INSERT INTO ${catalog_name}.test_ctas_ex.hive_ctas1
                        SELECT col1 FROM test_ctas.unpart_ctas_src WHERE col1 > 1;
                    """
                sql """ INSERT OVERWRITE TABLE ${catalog_name}.test_ctas_ex.hive_ctas1
                        SELECT col1 FROM test_ctas.unpart_ctas_src WHERE col1 > 1;
                    """
                order_qt_ctas_ex01 """ SELECT * FROM hive_ctas1 """
                order_qt_hive_docker_ctas_ex01 """ SELECT * FROM ${db}.hive_ctas1 """
                sql """ DROP TABLE hive_ctas1 """

                // 2. external to external partitioned table
                sql """ DROP TABLE IF EXISTS ${catalog_name}.test_ctas_ex.hive_ctas2 """
                sql """ CREATE TABLE ${catalog_name}.test_ctas_ex.hive_ctas2 (col1,pt1,pt2) ENGINE=hive 
                        PARTITION BY LIST (pt1, pt2) ()
                        PROPERTIES (
                            "location" = "/user/hive/warehouse/test_ctas_ex/loc_hive_ctas2",
                            "file_format"="parquet",
                            "parquet.compression"="snappy"
                        )
                        AS SELECT col1,pt1,pt2 FROM test_ctas.part_ctas_src WHERE col1>0; 
                    """
                sql """ INSERT INTO ${catalog_name}.test_ctas_ex.hive_ctas2 (col1,pt1,pt2)
                        SELECT col1,pt1,pt2 FROM test_ctas.part_ctas_src WHERE col1>=22;
                    """
                sql """ INSERT OVERWRITE TABLE hive_ctas2 (col1,pt1,pt2)
                        SELECT col1,pt1,pt2 FROM test_ctas.part_ctas_src WHERE col1>=22;
                    """
                sql """ INSERT INTO ${catalog_name}.test_ctas_ex.hive_ctas2 (pt1,col1)
                        SELECT pt1,col1 FROM test_ctas.part_ctas_src WHERE col1>=22;
                    """
                order_qt_ctas_ex02 """ SELECT * FROM hive_ctas2  """
                order_qt_hive_docker_ctas_ex02 """ SELECT * FROM ${db}.hive_ctas2 """
                sql """ DROP TABLE hive_ctas2 """

                // 3. internal to external un-partitioned table
                sql """ DROP TABLE IF EXISTS ${catalog_name}.test_ctas_ex.ctas_o1 """
                sql """ CREATE TABLE ${catalog_name}.test_ctas_ex.ctas_o1 (col1,col2) ENGINE=hive
                        PROPERTIES (
                            "location" = "/user/hive/warehouse/test_ctas_ex/loc_ctas_o1",
                            "file_format"="parquet",
                            "parquet.compression"="snappy"
                        )
                        AS SELECT col1,col2 FROM internal.test_ctas_olap.unpart_ctas_olap_src;
                """
                sql """ INSERT INTO ${catalog_name}.test_ctas_ex.ctas_o1 (col2,col1)
                        SELECT col2,col1 FROM internal.test_ctas_olap.unpart_ctas_olap_src;
                """
                sql """ INSERT OVERWRITE TABLE ${catalog_name}.test_ctas_ex.ctas_o1 (col2)
                        SELECT col2 FROM internal.test_ctas_olap.unpart_ctas_olap_src;
                """
                order_qt_ctas_03 """ SELECT * FROM ctas_o1  """
                order_qt_hive_docker_ctas_ex03 """ SELECT * FROM ${db}.ctas_o1 """
                sql """ DROP TABLE ctas_o1 """

                // 4. internal to external partitioned table
                sql """ DROP TABLE IF EXISTS ${catalog_name}.test_ctas_ex.ctas_o2 """
                sql """ CREATE TABLE ${catalog_name}.test_ctas_ex.ctas_o2 (col1,col2,pt1) ENGINE=hive
                        PARTITION BY LIST (pt1) ()
                        PROPERTIES (
                            "location" = "/user/hive/warehouse/test_ctas_ex/loc_ctas_o2",
                            "file_format"="orc",
                            "orc.compress"="zlib"
                        ) 
                        AS SELECT null as col1, pt2 as col2, pt1 FROM internal.test_ctas_olap.part_ctas_olap_src WHERE col1>0;
                    """
                sql """ INSERT INTO ${catalog_name}.test_ctas_ex.ctas_o2 (col1,pt1,col2)
                        SELECT col1,pt1,pt2 FROM internal.test_ctas_olap.part_ctas_olap_src;
                """
                sql """ INSERT INTO ${catalog_name}.test_ctas_ex.ctas_o2 (col2,pt1,col1)
                        SELECT pt2,pt1,col1 FROM internal.test_ctas_olap.part_ctas_olap_src;
                """
                sql """ INSERT OVERWRITE TABLE ${catalog_name}.test_ctas_ex.ctas_o2 (pt1,col2)
                        SELECT pt1,col1 FROM internal.test_ctas_olap.part_ctas_olap_src;
                """
                order_qt_ctas_04 """ SELECT * FROM ctas_o2  """
                order_qt_hive_docker_ctas_ex04 """ SELECT * FROM ${db}.ctas_o2 """
                sql """ DROP TABLE ctas_o2 """
                sql """ DROP DATABASE IF EXISTS test_ctas_ex """
            } finally {
                destroySrcDDLForCTAS(catalog_name)
            }
        }

        def test_ctas_exception = { String file_format, String catalog_name ->
            sql """ switch ${catalog_name} """

            sql """ create database if not exists `test_hive_ex_ctas` """;
            test {
                sql """ create database `test_hive_ex_ctas` """
                exception "errCode = 2, detailMessage = Can't create database 'test_hive_ex_ctas'; database exists"
            }
            sql """use `${catalog_name}`.`test_hive_ex_ctas`"""
            sql """ DROP DATABASE IF EXISTS ${catalog_name}.test_hive_ex_ctas """
            // check ctas error
            generateSrcDDLForCTAS(file_format, catalog_name)
            try {
                test {
                    sql """ DROP DATABASE ${catalog_name}.test_no_exist """
                    exception "errCode = 2, detailMessage = Can't drop database 'test_no_exist'; database doesn't exist"
                }
                sql """ DROP DATABASE IF EXISTS ${catalog_name}.test_err """
                sql """ CREATE DATABASE ${catalog_name}.test_err """
                test {
                    sql """ CREATE DATABASE ${catalog_name}.test_err
                    PROPERTIES (
                        "location" = "/user/hive/warehouse/test_err",
                        "owner" = "err"
                    )
                    """;
                    exception "errCode = 2, detailMessage = Can't create database 'test_err'; database exists"
                }
                sql """ DROP DATABASE IF EXISTS ${catalog_name}.test_err """

                sql """ CREATE DATABASE IF NOT EXISTS `test_no_err`""";
                sql """ use `${catalog_name}`.`test_no_err` """

                // 1. external to external un-partitioned table
                test {
                    sql """ DROP TABLE ${catalog_name}.test_no_err.hive_ctas1 """
                    exception "errCode = 2, detailMessage = Unknown table 'hive_ctas1' in test_no_err"
                }
                test {
                    sql """ CREATE TABLE ${catalog_name}.test_no_err.hive_ctas1 (col1) ENGINE=hive 
                            PROPERTIES (
                                "file_format"="orc",
                                "orc.compress"="zstd"
                            ) AS SELECT col1,col2 FROM test_ctas.unpart_ctas_src; 
                        """
                    exception "errCode = 2, detailMessage = ctas column size is not equal to the query's"
                }

                test {
                    sql """ CREATE TABLE ${catalog_name}.test_no_err.ctas_o2 (col1,pt1,pt2) ENGINE=hive 
                        PARTITION BY LIST (pt1,pt2,pt3) ()
                        PROPERTIES (
                            "file_format"="parquet",
                            "orc.compress"="zstd"
                        )
                        AS SELECT * FROM test_ctas.part_ctas_src WHERE col1>0; 
                    """
                    exception "errCode = 2, detailMessage = partition key pt3 is not exists"
                }

                sql """ DROP TABLE IF EXISTS ${catalog_name}.test_no_err.ctas_o2 """
                sql """ CREATE TABLE ${catalog_name}.test_no_err.ctas_o2 (col1,col2,pt1) ENGINE=hive 
                        PARTITION BY LIST (pt1) ()
                        PROPERTIES (
                            "file_format"="parquet",
                            "parquet.compression"="zstd"
                        )
                        AS SELECT col1,pt1 as col2,pt2 as pt1 FROM test_ctas.part_ctas_src WHERE col1>0; 
                    """

                test {
                    sql """ INSERT INTO ${catalog_name}.test_no_err.ctas_o2 (col1,col2)
                            SELECT col1 FROM test_ctas.part_ctas_src;
                        """
                    exception "errCode = 2, detailMessage = insert into cols should be corresponding to the query output"
                }

                test {
                    sql """ INSERT INTO ${catalog_name}.test_no_err.ctas_o2 (col1)
                            SELECT col1,pt1 as col2 FROM test_ctas.part_ctas_src WHERE col1>0;
                        """
                    exception "errCode = 2, detailMessage = insert into cols should be corresponding to the query output"
                }
                sql """ DROP TABLE IF EXISTS ${catalog_name}.test_no_err.ctas_o2  """

                // test ctas with qualified table name
                sql """drop table if exists ${catalog_name}.test_no_err.qualified_table1"""
                sql """use internal.test_ctas_olap"""
                sql """create table ${catalog_name}.test_no_err.qualified_table1 as SELECT col1,pt1 as col2 FROM ${catalog_name}.test_ctas.part_ctas_src WHERE col1>0;"""
                order_qt_qualified_table1 """select * from ${catalog_name}.test_no_err.qualified_table1"""

                sql """drop table if exists ${catalog_name}.test_no_err.qualified_table2"""
                sql """switch ${catalog_name}"""
                sql """create table test_no_err.qualified_table2 as SELECT col1,pt1 as col2 FROM ${catalog_name}.test_ctas.part_ctas_src WHERE col1>0;"""
                order_qt_qualified_table2 """select * from ${catalog_name}.test_no_err.qualified_table2"""

                sql """drop table if exists ${catalog_name}.test_no_err.qualified_table1"""
                sql """drop table if exists ${catalog_name}.test_no_err.qualified_table2"""
                sql """ DROP DATABASE IF EXISTS test_no_err """

            } finally {
                destroySrcDDLForCTAS(catalog_name)
            }
        }

        def test_ctas_all_types = { String file_format, String catalog_name ->
            sql """ switch `${catalog_name}` """
            sql """ CREATE DATABASE IF NOT EXISTS `test_ctas_all_type` """;
            sql """ use test_ctas_all_type """;
            String db = "test_ctas_all_type"
            // TODO: work on hive3
            //            sql """
            //                CREATE TABLE IF NOT EXISTS all_types_ctas_${file_format}_with_dv(
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

            sql """
                CREATE TABLE IF NOT EXISTS all_types_ctas_${file_format}(
                  `col1` BOOLEAN COMMENT 'col1',
                  `col2` TINYINT COMMENT 'col2',
                  `col3` SMALLINT COMMENT 'col3',
                  `col4` INT COMMENT 'col4',
                  `col5` BIGINT COMMENT 'col5',
                  `col6` CHAR(10) COMMENT 'col6',
                  `col7` FLOAT COMMENT 'col7',
                  `col8` DOUBLE COMMENT 'col8',
                  `col9` DECIMAL(9,4) COMMENT 'col9',
                  `col10` VARCHAR(11) COMMENT 'col10',
                  `col11` STRING COMMENT 'col11',
                  `col12` DATE COMMENT 'col12',
                  `col13` DATETIME COMMENT 'col13'
                )  ENGINE=hive
                PROPERTIES (
                  'file_format'='${file_format}'
                )
                """

            sql """
                    INSERT INTO all_types_ctas_${file_format} (
                        col1,
                        col2,
                        col3,
                        col4,
                        col5,
                        col6,
                        col7,
                        col8,
                        col9,
                        col10,
                        col11,
                        col12,
                        col13
                    ) VALUES (
                        true,        -- col1 (BOOLEAN)
                        127,         -- col2 (TINYINT)
                        32767,       -- col3 (SMALLINT)
                        2147483647,  -- col4 (INT)
                        9223372036854775807, -- col5 (BIGINT)
                        'default',   -- col6 (CHAR)
                        22.12345,         -- col7 (FLOAT)
                        3.141592653, -- col8 (DOUBLE)
                        99999.9999,  -- col9 (DECIMAL)
                        'default',   -- col10 (VARCHAR)
                        'default',   -- col11 (STRING)
                        '2023-05-29',-- col12 (DATE)
                        '2023-05-29 23:19:34' -- col13 (DATETIME)
                    );
                """

            sql """
                    CREATE TABLE IF NOT EXISTS all_types_ctas1 AS SELECT * FROM all_types_ctas_${file_format}
                """
            sql """
                    INSERT INTO all_types_ctas1 SELECT * FROM all_types_ctas_${file_format}
                """
            sql """
                    INSERT OVERWRITE TABLE all_types_ctas1 SELECT * FROM all_types_ctas_${file_format}
                """
            order_qt_ctas_types_01 """ SELECT * FROM all_types_ctas1 """
            order_qt_hive_docker_ctas_types_01 """ SELECT * FROM ${db}.all_types_ctas1 """
            sql """
                    DROP TABLE all_types_ctas1
                """

            sql """
                    CREATE TABLE IF NOT EXISTS all_types_ctas2 (col1, col2, col3, col4, col6, col7, col8, col9, col11) 
                    AS SELECT col1, col2, col3, col4, col6, col7, col8, col9, col11 FROM all_types_ctas_${file_format}
                """
            sql """
                    INSERT INTO all_types_ctas2 (col1, col3, col7, col9) 
                    SELECT col1, col3, col7, col9 FROM all_types_ctas_${file_format}
                """
            sql """
                    INSERT OVERWRITE TABLE all_types_ctas2 (col1, col2, col3, col4, col6, col7, col8, col9, col11)
                    SELECT col1, col2, col3, col4, col6, col7, col8, col9, col11 FROM all_types_ctas_${file_format}
                """
            order_qt_ctas_types_02 """ SELECT * FROM all_types_ctas2 """
            order_qt_hive_docker_ctas_types_02 """ SELECT * FROM ${db}.all_types_ctas2 """
            sql """
                DROP TABLE all_types_ctas2
                """
            sql """
                DROP TABLE all_types_ctas_${file_format}
                """
            sql """ drop database if exists `test_ctas_all_type` """;
        }

        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_${hivePrefix}_ctas"
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
            for (String file_format in file_formats) {
                logger.info("Process file format" + file_format)
                test_ctas_tbl(file_format, catalog_name)
                test_ctas_extend(file_format, catalog_name)
                test_ctas_exception(file_format, catalog_name)
                test_ctas_all_types(file_format, catalog_name)
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
