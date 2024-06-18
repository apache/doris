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

suite("test_decimalv3_where") {


    def tableName = "test_decimalv3_demo"
	sql """drop table if exists ${tableName}"""
	sql """CREATE TABLE ${tableName} ( `id` varchar(11) NULL COMMENT '唯一标识', `name` varchar(10) NULL COMMENT '采集时间', `age` int(11) NULL, `dr` decimalv3(10, 0) ) ENGINE=OLAP UNIQUE KEY(`id`) COMMENT 'test' DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2", "light_schema_change" = "true", "disable_auto_compaction" = "false" );"""
	sql """insert into ${tableName} values (1,'doris',20,324.10),(2,'spark',10,95.5),(3,'flink',9,20)"""
	qt_decimalv3 "select * from ${tableName} where dr != 1  order by age;"

    sql """drop table if exists test_sys_update_basic_test_update_decimal_tb"""
    sql """CREATE TABLE test_sys_update_basic_test_update_decimal_tb (
              k1 DECIMAL(10, 5) NULL, 
              v1 DECIMAL(10, 5) NULL
            ) UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            );"""
    sql """insert into test_sys_update_basic_test_update_decimal_tb values
                (1.001, 2.002), (1.002, 0.00000002), (1.003, 0.100000001), (1.004, 0.100044001), (1.005, 0.100045001);"""
    qt_select """select * from test_sys_update_basic_test_update_decimal_tb where k1 = 1.001;"""

    sql """
        UPDATE test_sys_update_basic_test_update_decimal_tb SET v1=0.0001 WHERE k1 = 1.001;
    """
    qt_select_after_update """
        select * from test_sys_update_basic_test_update_decimal_tb where k1 = 1.001;
    """
}
