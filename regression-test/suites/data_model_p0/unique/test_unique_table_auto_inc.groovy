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

suite("test_unique_table_auto_inc") {
    
    // auto-increment column is key
    def table1 = "test_unique_tab_auto_inc_col_basic_key"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql "select * from ${table1};"
    sql """ insert into ${table1} values(0, "Bob", 123), (2, "Tom", 323), (4, "Carter", 523);"""
    qt_sql "select * from ${table1} order by id"
    sql "drop table if exists ${table1};"

    // auto-increment column is value
    def table2 = "test_unique_tab_auto_inc_col_basic_value"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`, `value`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`, `value`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql "select id, name, value from ${table2} order by id;"
    sql """ insert into ${table2} values("Bob", 100, 1230), ("Tom", 300, 1232), ("Carter", 500, 1234);"""
    qt_sql "select id, name, value from ${table2} order by id;"
    sql "drop table if exists ${table2};"

    // auto inc key with null values
    def table3 = "test_unique_tab_auto_inc_col_key_with_null"
    sql "drop table if exists ${table3}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table3}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, name, value'

        file 'auto_inc_with_null.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql "select * from ${table3};"
    sql """ insert into ${table3} values(0, "Bob", 123), (2, "Tom", 323), (4, "Carter", 523);"""
    qt_sql "select * from ${table3} order by id"
    sql "drop table if exists ${table3};"

    // dircetly update rows in one batch
    def table4 = "test_unique_tab_auto_inc_col_key_with_null"
    sql "drop table if exists ${table4}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table4}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table4}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, name, value'

        file 'auto_inc_update_inplace.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_update_inplace "select * from ${table4};"
    sql "drop table if exists ${table4};"

    // test for partial update, auto inc col is key
    def table5 = "test_unique_tab_auto_inc_col_key_partial_update"
    sql "drop table if exists ${table5}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table5}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table5}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_partial_update_key "select * from ${table5} order by id;"

    streamLoad {
        table "${table5}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update1.csv'
        time 10000
    }
    sql "sync"
    qt_partial_update_key "select * from ${table5} order by id;"
    sql "drop table if exists ${table5};"

    // test for partial update, auto inc col is value, update auto inc col
    def table6 = "test_unique_tab_auto_inc_col_value_partial_update"
    sql "drop table if exists ${table6}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table6}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table6}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_partial_update_value "select * from ${table6} order by id;"

    streamLoad {
        table "${table6}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, id'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update2.csv'
        time 10000
    }
    sql "sync"
    qt_partial_update_value "select * from ${table6} order by id;"
    sql "drop table if exists ${table6};"

    // test for partial update, auto inc col is value, update other col
    def table7 = "test_unique_tab_auto_inc_col_value_partial_update"
    sql "drop table if exists ${table7}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table7}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table7}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_partial_update_value1 "select name, value from ${table7} order by value;"
    qt_partial_update_value2 "select id, count(*) from ${table7} group by id having count(*) > 1;"

    streamLoad {
        table "${table7}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update2.csv'
        time 10000
    }
    sql "sync"
    qt_partial_update_value1 "select name, value from ${table7} order by value;"
    qt_partial_update_value2 "select id, count(*) from ${table7} group by id having count(*) > 1;"

    streamLoad {
        table "${table7}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update3.csv'
        time 10000
    }
    sql "sync"
    qt_partial_update_value1 "select name, value from ${table7} order by value;"
    qt_partial_update_value2 "select id, count(*) from ${table7} group by id having count(*) > 1;"
    sql "drop table if exists ${table7};"

    def table8 = "test_auto_inc_col_create_as_select1"
    def table9 = "test_auto_inc_col_create_as_select2"
    def table10 = "test_auto_inc_col_create_as_select3"
    sql "drop table if exists ${table8}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table8}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql "drop table if exists ${table9}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table9}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """insert into ${table8}(name) values("a"), ("b"), ("c"); """
    qt_sql "select * from ${table8} order by id, name;"
    sql """insert into ${table9}(value) values(10),(20),(30); """
    qt_sql "select * from ${table9} order by id, value;"
    sql "drop table if exists ${table10}"
    sql """create table ${table10}(id,name,value) PROPERTIES("replication_num" = "1") as select A.id, A.name, B.value from ${table8} A join ${table9} B on A.id=B.id;"""
    qt_sql "select * from ${table10} order by id, name, value;"
    sql "drop table if exists ${table8};"
    sql "drop table if exists ${table9};"
    sql "drop table if exists ${table10};"


    def table11 = "test_unique_tab_auto_inc_col_insert_select"
    sql "drop table if exists ${table11}"
    sql """CREATE TABLE ${table11} (
        `r_regionkey` bigint(20) NOT NULL AUTO_INCREMENT,
        `r_name` varchar(25) NOT NULL,
        `r_comment` varchar(152) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`r_regionkey`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
    sql """ INSERT INTO ${table11} values 
 (0,'AFRICA','lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to')
,(1,'AMERICA','hs use ironic, even requests. s')
,(2,'ASIA','ges. thinly even pinto beans ca')
,(3,'EUROPE','ly final courts cajole furiously final excuse')
,(4,'MIDDLE EAST','uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl');"""
    qt_sql "select * from ${table11} order by r_regionkey;"
    sql 'set enable_nereids_planner=true'
    sql "set experimental_enable_nereids_planner=true;"
    sql 'set enable_nereids_dml=true'
    sql "update ${table11} set r_comment = 'foobar' where  r_regionkey <= 10;"
    qt_sql "select * from ${table11} order by r_regionkey;"

    sql 'set enable_nereids_planner=false'
    sql "set experimental_enable_nereids_planner=false;"
    sql 'set enable_nereids_dml=false'
    sql "update ${table11} set r_comment = 'barfoo' where  r_regionkey <= 10;"
    qt_sql "select * from ${table11} order by r_regionkey;"
    sql "drop table if exists ${table11};"


    def table12 = "test_unique_tab_auto_inc_col_insert_select2"
    sql "drop table if exists ${table12}"
    sql """CREATE TABLE ${table12} (
        `r_regionkey` bigint(20) NOT NULL AUTO_INCREMENT,
        `r_name` varchar(25) NOT NULL,
        `r_comment` varchar(152) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`r_regionkey`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); """
    sql """ INSERT INTO ${table12} values 
 (0,'AFRICA','lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to')
,(1,'AMERICA','hs use ironic, even requests. s')
,(2,'ASIA','ges. thinly even pinto beans ca')
,(3,'EUROPE','ly final courts cajole furiously final excuse')
,(4,'MIDDLE EAST','uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl');"""
    qt_sql "select * from ${table12} order by r_regionkey;"
    sql 'set enable_nereids_planner=true'
    sql "set experimental_enable_nereids_planner=true;"
    sql 'set enable_nereids_dml=true'
    sql """insert into ${table12} select r_regionkey, "test1", "test2" from ${table12} where r_regionkey=3;"""
    qt_sql "select * from ${table12} order by r_regionkey;"
    sql 'set enable_nereids_planner=false'
    sql "set experimental_enable_nereids_planner=false;"
    sql 'set enable_nereids_dml=false'
    sql """insert into ${table12} select r_regionkey, "test3", "test4" from ${table12} where r_regionkey=4;"""
    qt_sql "select * from ${table12} order by r_regionkey;"
    sql "drop table if exists ${table12};"


    def table13 = "test_unique_tab_auto_inc_col_insert_select3"
    sql "drop table if exists ${table13}"
    sql """ CREATE TABLE ${table13} (
        mykey BIGINT NOT NULL AUTO_INCREMENT,
        name VARCHAR(64)
        ) UNIQUE KEY(mykey)
        DISTRIBUTED BY HASH(mykey)
        BUCKETS AUTO PROPERTIES("replication_num" = "1"); """
    def table14 = "test_unique_tab_auto_inc_col_insert_select4"
    sql "drop table if exists ${table14}"
    sql """ CREATE TABLE ${table14} (
        mykey BIGINT NULL,
        name VARCHAR(64)
        ) DUPLICATE KEY(mykey)
        DISTRIBUTED BY HASH(mykey)
        BUCKETS AUTO PROPERTIES("replication_num" = "1"); """
    
    sql """insert into ${table13}(name) values("A"), ("B"), ("C")"""
    qt_sql "select * from ${table13} order by mykey;"
    sql """insert into ${table13}(name) select lower(name) from ${table13};"""
    qt_sql """ select count(mykey), count(distinct mykey) from ${table13}"""
    qt_sql "select name from ${table13} order by name;"
    sql """ insert into ${table13}(name) select "Jack" as name from ${table13};"""
    qt_sql """ select count(mykey), count(distinct mykey) from ${table13}"""
    qt_sql "select name from ${table13} order by name;"
    
    sql """ insert into ${table14} values
    (100,"John"), (null, "Mick"), (300, "Bob"), (null, "Alice"), (null, "Steve");"""
    qt_ql "select * from ${table14} order by mykey, name;"
    sql """ insert into ${table13} select mykey, name from ${table14};"""
    qt_sql """ select count(mykey), count(distinct mykey) from ${table13}"""
    qt_sql "select name from ${table13} order by name;"

    sql "drop table if exists ${table13};"
    sql "drop table if exists ${table14};"
    
}

