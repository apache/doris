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

suite("fold_constant") {
    
    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                k1 int null,
                k2 date null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """
    sql "insert into d_table values(1,curdate());"
    sql "insert into d_table values(2,'2020-01-01');"
    sql "delete from d_table where k2=curdate();"
    qt_select "select * from d_table order by 1;"

    sql "insert into d_table values(4,'2020-01-01');"
    qt_select "select * from d_table order by 1;"
    sql "delete from d_table where k1=3+1;"
    qt_select "select * from d_table order by 1;"

    sql """ DROP TABLE IF EXISTS d_table2; """

    sql """
            create table d_table2(
                k1 int null,
                k2 date null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1",
            "disable_auto_compaction" = "true");
        """
    sql "insert into d_table2 values(1,curdate());"
    sql "insert into d_table2 values(2,'2020-01-01');"
    sql "delete from d_table2 where k2=curdate();"
    qt_select "select * from d_table2 order by 1;"

    sql "insert into d_table2 values(4,'2020-01-01');"
    qt_select "select * from d_table2 order by 1;"
    sql "delete from d_table2 where k1=3+1;"
    qt_select "select * from d_table2 order by 1;"

    sql """ DROP TABLE IF EXISTS delete_condition; """
    sql """
    CREATE TABLE IF NOT EXISTS delete_condition
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `create_date` DATETIMe NOT NULL COMMENT "数据灌入日期时间",
    )
    ENGINE=OLAP
    unique KEY(`user_id`, `create_date`)
    PARTITION BY RANGE(`create_date`)
    (
        PARTITION `p201701` VALUES LESS THAN ("2017-02-01 00:00:00"),
        PARTITION `p201702` VALUES LESS THAN ("2017-03-01 00:00:00"),
        PARTITION `p201703` VALUES LESS THAN ("2017-04-01 00:00:00")
    )
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
    PROPERTIES
    (
        "replication_num" = "1"
    ); 
    """
    sql """insert into delete_condition values(1,"2017-01-01 00:00:00");"""
    sql """insert into delete_condition values(2,"2017-02-02 00:00:00");"""
    sql """insert into delete_condition values(3,"2017-02-03 00:00:00");"""
    sql """insert into delete_condition values(4,"2017-03-02 00:00:00");"""
    sql """
    delete from  delete_condition PARTITION p201702  where  `create_date` > str_to_date('2017-02-02 00:00:00', '%Y-%m-%d %H:%i:%s'); 
    """
    qt_select """select * from delete_condition order by 1,2;"""
}
