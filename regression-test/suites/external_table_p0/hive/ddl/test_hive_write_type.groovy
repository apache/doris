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

suite("test_hive_write_type", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        def file_formats = ["parquet", "orc"]
        def test_complex_type_tbl = { String file_format, String catalog_name ->
            sql """ switch ${catalog_name} """
            sql """ create database if not exists `test_complex_type` """;
            sql """ use `${catalog_name}`.`test_complex_type` """

            sql """ drop table if exists unpart_tbl_${file_format} """
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
            INSERT OVERWRITE TABLE unpart_tbl_${file_format} (
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

            sql """ drop table unpart_tbl_${file_format} """
            sql """ drop database if exists `test_complex_type` """;
        }

        def test_insert_exception = { String file_format, String catalog_name ->
            sql """ switch ${catalog_name} """

            sql """ create database if not exists `test_hive_ex` """;
            test {
                sql """ create database `test_hive_ex` """
                exception "errCode = 2, detailMessage = Can't create database 'test_hive_ex'; database exists"
            }
            sql """use `${catalog_name}`.`test_hive_ex`"""

            sql """
                CREATE TABLE IF NOT EXISTS test_hive_ex.ex_tbl_${file_format}(
                  `col1` BOOLEAN COMMENT 'col1',
                  `col2` INT COMMENT 'col2',
                  `col3` BIGINT COMMENT 'col3',
                  `col4` CHAR(10) COMMENT 'col4',
                  `col5` FLOAT COMMENT 'col5',
                  `col6` DOUBLE COMMENT 'col6',
                  `col7` DECIMAL(6,4) COMMENT 'col7',
                  `col8` VARCHAR(11) COMMENT 'col8',
                  `col9` STRING COMMENT 'col9',
                  `pt1` VARCHAR COMMENT 'pt1',
                  `pt2` STRING COMMENT 'pt2',
                  `pt3` DATE COMMENT 'pt3'
                )  ENGINE=hive 
                PARTITION BY LIST (pt1, pt2) ()
                PROPERTIES (
                  'file_format'='${file_format}'
                )
            """;

            try {
                // test  columns
                sql """ INSERT INTO ex_tbl_${file_format} (`col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9`) 
                    VALUES 
                    (true, 123, 987654321099, 'abcdefghij', 3.1214, 63.28, 123.4567, 'varcharval', 'stringval');
                """
            } catch (Exception e) {
                log.info(e.getMessage())
                // BE err msg need use string contains to check
                assertTrue(e.getMessage().contains("Arithmetic overflow, convert failed from 1234567, expected data is [-999999, 999999]"))
            }

            try {
                // test type diff columns
                sql """ INSERT INTO ex_tbl_${file_format} (`col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9`) 
                    VALUES 
                    ('1', 123, 987654319, 'abcdefghij', '3.15', '6.28', 123.4567, 432, 'stringval');
                """
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Arithmetic overflow, convert failed from 1234567, expected data is [-999999, 999999]"))
            }

            test {
                sql """
                        CREATE TABLE test_hive_ex.ex_tbl_${file_format}(
                          `col1` BOOLEAN COMMENT 'col1'
                        )  ENGINE=hive 
                        PROPERTIES (
                          'file_format'='${file_format}'
                        )
                    """;
                exception "errCode = 2, detailMessage = errCode = 2, detailMessage = Table 'ex_tbl_${file_format}' already exists"
            }

            test {
                // test columns
                sql """ INSERT INTO ex_tbl_${file_format} (`col1`, `col2`, `col3`, `col4`, `col5`) 
                        VALUES 
                        (true, 123, 9876543210, 'abcdefghij', 3.14, 6.28, 123.4567, 'varcharval', 'stringval');
                """
                exception "errCode = 2, detailMessage = Column count doesn't match value count"
            }

            test {
                // test columns
                sql """ INSERT INTO ex_tbl_${file_format} (`col1`, `col2`, `col3`, `col4`, `col5`, `pt00`) 
                    VALUES 
                    (true, 123, 9876543210, 'abcdefghij', 3.14, 'error');
                """
                exception "errCode = 2, detailMessage = Unknown column 'pt00' in target table."
            }

            test {
                sql """ INSERT INTO ex_tbl_${file_format} partition(`pt1`,`pt2`) (`col1`, `col9`) 
                    VALUES 
                    ('abcdefghij', 'error', true, 123);
                """
                exception "errCode = 2, detailMessage = Column count doesn't match value count"
            }

            sql """ INSERT INTO ex_tbl_${file_format} partition(`pt1`,`pt2`) (`col3`, `col6`, `col9`) 
                VALUES 
                (9876543210, 6.28, 'no_error');
            """

            test {
                sql """ INSERT INTO ex_tbl_${file_format} partition(`pt0`, `pt1`,`pt3`) (`col3`, `col6`, `col9`) 
                    VALUES 
                    ('err', 'err', 'err', 9876543210, 6.28, 'error');
                """
                exception "errCode = 2, detailMessage = Column count doesn't match value count"
            }

            sql """ DROP TABLE ${catalog_name}.test_hive_ex.ex_tbl_${file_format} """
            sql """ DROP DATABASE ${catalog_name}.test_hive_ex """
        }

        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_${hivePrefix}_write_type"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );"""
            sql """switch ${catalog_name}"""

            sql """set enable_fallback_to_original_planner=false;"""
            for (String file_format in file_formats) {
                logger.info("Process file format" + file_format)
                test_complex_type_tbl(file_format, catalog_name)
                test_insert_exception(file_format, catalog_name)
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
