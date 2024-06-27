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

suite("test_hive_parquet_add_column", "all_types,p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    try {
        String hms_port = context.config.otherConfigs.get("hive3HmsPort")
        String catalog_name = "hive3_test_parquet_add_column"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """use `${catalog_name}`.`default`"""

        qt_parquet_add_col1 """select * from test_hive_parquet_add_column order by id ;"""
        qt_parquet_add_col2 """select * from test_hive_parquet_add_column where col1 is null order by id ;"""
        qt_parquet_add_col3 """select col1 from test_hive_parquet_add_column where col1 is null;"""
        qt_parquet_add_col4 """select * from test_hive_parquet_add_column where col2 is null order by id ;"""
        qt_parquet_add_col5 """select col2 from test_hive_parquet_add_column where col2 is null;"""
        qt_parquet_add_col6 """select * from test_hive_parquet_add_column where col3 is null order by id ;"""
        qt_parquet_add_col7 """select col3 from test_hive_parquet_add_column where col3 is null;"""
        qt_parquet_add_col8 """select * from test_hive_parquet_add_column where col4 is null order by id ;"""
        qt_parquet_add_col9 """select col4 from test_hive_parquet_add_column where col4 is null;"""
        qt_parquet_add_col10 """select * from test_hive_parquet_add_column where col1 is not null order by id ;"""
        qt_parquet_add_col11 """select col1 from test_hive_parquet_add_column where col1 is not null order by col1;"""
        qt_parquet_add_col12 """select * from test_hive_parquet_add_column where col2 is not null order by id ;"""
        qt_parquet_add_col13 """select col2 from test_hive_parquet_add_column where col2 is not null order by col2;"""
        qt_parquet_add_col14 """select * from test_hive_parquet_add_column where col3 is not null order by id ;"""
        qt_parquet_add_col15 """select col3 from test_hive_parquet_add_column where col3 is not null order by col3;"""
        qt_parquet_add_col16 """select * from test_hive_parquet_add_column where col4 is not null order by id ;"""
        qt_parquet_add_col17 """select col4 from test_hive_parquet_add_column where col4 is not null order by col4;"""
        qt_parquet_add_col18 """select * from test_hive_parquet_add_column where col2 = 9 order by id ;"""
        qt_parquet_add_col19 """select * from test_hive_parquet_add_column where col2 = 190 order by id ;"""
        qt_parquet_add_col20 """select * from test_hive_parquet_add_column where col2 - col1 = 1 order by id ;"""
        qt_parquet_add_col21 """select * from test_hive_parquet_add_column where col2 - id  = 2 order by id ;"""
        qt_parquet_add_col22 """select * from test_hive_parquet_add_column where col2 - id  = 3 order by id ;"""
        qt_parquet_add_col23 """select * from test_hive_parquet_add_column where col3 = 33 order by id ;"""
        qt_parquet_add_col24 """select * from test_hive_parquet_add_column where col3 = 330 order by id ;"""
        qt_parquet_add_col25 """select * from test_hive_parquet_add_column where col3 - col1 = 2 order by id ;"""
        qt_parquet_add_col26 """select * from test_hive_parquet_add_column where col3 - id  != 3 order by id ;"""
        qt_parquet_add_col27 """select * from test_hive_parquet_add_column where col1 + col2 + col3 = 23*3 order by id ;"""
        qt_parquet_add_col28 """select * from test_hive_parquet_add_column where col1 + col2 + col3 != 32*3 order by id ; """

        sql """drop catalog if exists ${catalog_name}"""

    } finally {
    }

}


// CREATE TABLE `test_hive_parquet_add_column`(
//   id int,
//   col1 int
// )
// stored as parquet;
// insert into  `test_hive_parquet_add_column` values(1,2);
// insert into  `test_hive_parquet_add_column` values(3,4),(4,6);
// alter table `test_hive_parquet_add_column` ADD COLUMNS(col2 int);
// insert into  `test_hive_parquet_add_column` values(7,8,9);
// insert into  `test_hive_parquet_add_column` values(10,11,null);
// insert into  `test_hive_parquet_add_column` values(12,13,null);
// insert into  `test_hive_parquet_add_column` values(14,15,16);
// alter table `test_hive_parquet_add_column` ADD COLUMNS(col3 int,col4 string);
// insert into  `test_hive_parquet_add_column` values(17,18,19,20,"hello world");
// insert into  `test_hive_parquet_add_column` values(21,22,23,24,"cywcywcyw");
// insert into  `test_hive_parquet_add_column` values(25,26,null,null,null);
// insert into  `test_hive_parquet_add_column` values(27,28,29,null,null);
// insert into  `test_hive_parquet_add_column` values(30,31,32,33,null);
