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

suite("test_hive_ddl_and_ctas", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def file_formats = ["parquet", "orc"]

        def test_db = { String catalog_name ->
            sql """switch ${catalog_name}"""
            sql """ create database if not exists `test_hive_db` """;
            sql """use `test_hive_db`"""
            sql """ drop database if exists `test_hive_db` """;
        }

        def test_loc_db = { String externalEnvIp, String hdfs_port, String catalog_name ->
            sql """switch ${catalog_name}"""
            sql """ create database if not exists `test_hive_loc_db`
                    properties('location_uri'='hdfs://${externalEnvIp}:${hdfs_port}/tmp/hive/test_hive_loc_db')
                """;
            sql """use `test_hive_loc_db`"""
            sql """ drop database if exists `test_hive_loc_db` """;
        }

        def test_db_tbl = { String file_format, String catalog_name ->
            sql """switch ${catalog_name}"""
            sql """ create database if not exists `test_hive_db` """;
            sql """use `${catalog_name}`.`test_hive_db`"""

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
                  `col9` STRING COMMENT 'col9'
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

            sql """ drop table if exists part_tbl_${file_format}"""
            sql """ drop database if exists `test_hive_db` """;
        }

        def test_ctas_tbl = { String file_format, String catalog_name ->
            sql """ switch `${catalog_name}` """
            sql """ create database if not exists `test_ctas` """;
            sql """ switch internal """
            sql """ create database if not exists test_ctas_olap """;
            sql """ use internal.test_ctas_olap """

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
            sql """
                CREATE TABLE `unpart_ctas_src`(
                  `col1` INT COMMENT 'col1',
                  `col2` STRING COMMENT 'col2'
                ) ENGINE=hive
                PROPERTIES (
                  'file_format'='parquet'
                );
            """

            sql """ INSERT INTO `unpart_ctas_src` (col1, col2) VALUES
                (1, 'string value for col2'),
                (2, 'another string value for col2'),
                (3, 'yet another string value for col2'); 
            """

            sql """
                CREATE TABLE `part_ctas_src`(
                  `col1` INT COMMENT 'col1',
                  `pt1` VARCHAR COMMENT 'pt1',
                  `pt2` VARCHAR COMMENT 'pt2'
                ) ENGINE=hive
                PARTITION BY LIST (pt1, pt2) ()
                PROPERTIES (
                  'file_format'='orc'
                );
            """

            sql """
            INSERT INTO `part_ctas_src` (col1, pt1, pt2) VALUES
             (11, 'value_for_pt1', 'value_for_pt2'),
             (22, 'value_for_pt11', 'value_for_pt22');
            """

            sql """ switch `${catalog_name}` """
            // 1. external to external un-partitioned table
            sql """ CREATE TABLE hive_ctas1 ENGINE=hive AS SELECT col1 FROM unpart_ctas_src; 
            """

            sql """ INSERT INTO hive_ctas1 SELECT col1 FROM unpart_ctas_src WHERE col1 > 1;
            """

            order_qt_ctas_01 """ SELECT * FROM hive_ctas1 """
            sql """ DROP TABLE hive_ctas1 """

            // 2. external to external partitioned table
            sql """ CREATE TABLE hive_ctas2 ENGINE=hive AS SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>0;
                """

            sql """ INSERT INTO hive_ctas2 SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>=22;
                """

            order_qt_ctas_02 """ SELECT * FROM hive_ctas2  """
            sql """ DROP TABLE hive_ctas2 """

            // 3. internal to external un-partitioned table
            sql """ CREATE TABLE ctas_o1 ENGINE=hive AS SELECT col1,col2 FROM internal.test_ctas_olap.unpart_ctas_olap_src;
            """

            sql """ INSERT INTO ctas_o1 SELECT col1,col2 FROM internal.test_ctas_olap.unpart_ctas_olap_src;
            """

            order_qt_ctas_03 """ SELECT * FROM ctas_o1  """
            sql """ DROP TABLE ctas_o1 """

            // 4. internal to external partitioned table
            sql """ CREATE TABLE ctas_o2 ENGINE=hive AS SELECT col1,pt1,pt2 FROM internal.test_ctas_olap.part_ctas_olap_src WHERE col1>0;
            """
            sql """ INSERT INTO ctas_o2 SELECT col1,pt1,pt2 FROM internal.test_ctas_olap.part_ctas_olap_src WHERE col1>2;
            """
            order_qt_ctas_04 """ SELECT * FROM ctas_o2  """
            sql """ DROP TABLE ctas_o2 """

            // 5. check external to internal un-partitioned table
            sql """ use internal.test_ctas_olap """
            sql """  CREATE TABLE olap_ctas1
                 PROPERTIES (
                     "replication_allocation" = "tag.location.default: 1"
                 ) AS SELECT col1,col2 
                 FROM `${catalog_name}`.`test_ctas`.unpart_ctas_src;
                """
            order_qt_ctas_05 """ SELECT * FROM olap_ctas1  """
            sql """ DROP TABLE olap_ctas1 """

            // 6. check external to internal partitioned table
            sql """ CREATE TABLE olap_ctas2 
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                ) AS SELECT col1,pt1,pt2 
                FROM `${catalog_name}`.`test_ctas`.part_ctas_src WHERE col1>0;
                """
            order_qt_ctas_06 """ SELECT * FROM olap_ctas2  """
            sql """ DROP TABLE olap_ctas2 """

            sql """ switch `${catalog_name}` """
            sql """ DROP TABLE `test_ctas`.part_ctas_src """
            sql """ DROP TABLE `test_ctas`.unpart_ctas_src """
            sql """ drop database if exists `test_ctas` """;
            sql """ DROP TABLE internal.test_ctas_olap.part_ctas_olap_src """
            sql """ DROP TABLE internal.test_ctas_olap.unpart_ctas_olap_src """
            sql """ switch internal """;
            sql """ drop database if exists test_ctas_olap """;
        }

        def test_complex_type_tbl = { String file_format, String catalog_name ->
            sql """ switch ${catalog_name} """
            sql """ create database if not exists `test_complex_type` """;
            sql """ use `${catalog_name}`.`test_complex_type` """

            sql """
                CREATE TABLE unpart_tbl_${file_format} (
                  `col1` CHAR,
                  `col2` CHAR(1),
                  `col3` CHAR(16),
                  `col4` VARCHAR,
                  `col5` VARCHAR(255),
                  `col6` DECIMAL(2,1),
                  `col7` DECIMAL(5,0),
                  `col8` DECIMAL(8,8),
                  `col9` STRING,
                  `col10` ARRAY<DECIMAL(4,3)>,
                  `col11` ARRAY<CHAR(16)>,
                  `col12` ARRAY<CHAR>,
                  `col13` ARRAY<STRING>,
                  `col14` ARRAY<MAP<INT, CHAR>>,
                  `col15` MAP<BIGINT, CHAR>,
                  `col16` MAP<BIGINT, DECIMAL(8,8)>,
                  `col17` MAP<STRING, ARRAY<CHAR(16)>>,
                  `col18` STRUCT<id:INT,gender:BOOLEAN,name:CHAR(16)>,
                  `col19` STRUCT<scale:DECIMAL(7,4),metric:ARRAY<STRING>>,
                  `col20` STRUCT<codes:ARRAY<INT>,props:MAP<STRING, ARRAY<CHAR(16)>>>
                )  ENGINE=hive
                PROPERTIES (
                  'file_format'='${file_format}'
                )
            """;

            sql """
            INSERT INTO unpart_tbl_${file_format} (
              col1, col2, col3, col4, col5, col6, col7, col8, col9,
              col10, col11, col12, col13, col14, col15, col16, col17,
              col18, col19, col20
            ) VALUES (
              'a', -- CHAR
              'b', -- CHAR(1)
              'c', -- CHAR(16)
              'd', -- VARCHAR
              'e', -- VARCHAR(255)
              1.1, -- DECIMAL(2,1)
              12345, -- DECIMAL(5,0)
              0.12345678, -- DECIMAL(8,8)
              'string', -- STRING
              ARRAY(0.001, 0.002), -- ARRAY<DECIMAL(4,3)>
              ARRAY('char1', 'char2'), -- ARRAY<CHAR(16)>
              ARRAY('c', 'd'), -- ARRAY<CHAR>
              ARRAY('string1', 'string2'), -- ARRAY<STRING>
              ARRAY(MAP(1, 'a'), MAP(2, 'b')), -- ARRAY<MAP<INT, CHAR>>
              MAP(1234567890123456789, 'a'), -- MAP<BIGINT, CHAR>
              MAP(1234567890123456789, 0.12345678), -- MAP<BIGINT, DECIMAL(8,8)>
              MAP('key', ARRAY('char1', 'char2')), -- MAP<STRING, ARRAY<CHAR(16)>>
              STRUCT(1, TRUE, 'John Doe'), -- STRUCT<id:INT,gender:BOOLEAN,name:CHAR(16)>
              STRUCT(123.4567, ARRAY('metric1', 'metric2')), -- STRUCT<scale:DECIMAL(7,4),metric:ARRAY<STRING>>
              STRUCT(ARRAY(123, 456), MAP('key1', ARRAY('char1', 'char2'))) -- STRUCT<codes:ARRAY<INT>,props:MAP<STRING, ARRAY<CHAR(16)>>
            );
        """

            sql """
            INSERT INTO unpart_tbl_${file_format} (
              col1, col11, col12, col13, col14, col15, col16, col17,
              col18, col19
            ) VALUES (
              'a', -- CHAR
              ARRAY('char1', 'char2'), -- ARRAY<CHAR(16)>
              ARRAY('c', 'd'), -- ARRAY<CHAR>
              ARRAY('string1', 'string2'), -- ARRAY<STRING>
              ARRAY(MAP(1, 'a'), MAP(2, 'b')), -- ARRAY<MAP<INT, CHAR>>
              MAP(1234567890123456789, 'a'), -- MAP<BIGINT, CHAR>
              MAP(1234567890123456789, 0.12345678), -- MAP<BIGINT, DECIMAL(8,8)>
              MAP('key', ARRAY('char1', 'char2')), -- MAP<STRING, ARRAY<CHAR(16)>>
              STRUCT(1, TRUE, 'John Doe'), -- STRUCT<id:INT,gender:BOOLEAN,name:CHAR(16)>
              STRUCT(123.4567, ARRAY('metric1', 'metric2')) -- STRUCT<scale:DECIMAL(7,4),metric:ARRAY<STRING>>
            );
        """

            sql """
            INSERT INTO unpart_tbl_${file_format} (
              col1, col2, col3, col4, col5, col6, col7, col8, col9
            ) VALUES (
              'a', -- CHAR
              'b', -- CHAR(1)
              'c', -- CHAR(16)
              'd', -- VARCHAR
              'e', -- VARCHAR(255)
              1.1, -- DECIMAL(2,1)
              12345, -- DECIMAL(5,0)
              0.12345678, -- DECIMAL(8,8)
              'string' -- STRING
            );
        """

            order_qt_complex_type01 """ SELECT * FROM unpart_tbl_${file_format} """
            order_qt_complex_type02 """ SELECT * FROM unpart_tbl_${file_format} WHERE col2='b' """

            sql """ DROP TABLE unpart_tbl_${file_format} """
            sql """ drop database if exists `test_complex_type` """;
        }

        try {
            String hms_port = context.config.otherConfigs.get("hms_port")
            String hdfs_port = context.config.otherConfigs.get("hdfs_port")
            String catalog_name = "test_hive_ddl_and_ctas"
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
                test_db_tbl(file_format, catalog_name)
                test_ctas_tbl(file_format, catalog_name)
                test_complex_type_tbl(file_format, catalog_name)
                // todo: test bucket table: test_db_buck_tbl()
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
