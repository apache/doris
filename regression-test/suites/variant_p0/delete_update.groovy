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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("regression_test_variant_delete_and_update", "variant_type"){
    // MOR
    def table_name = "var_delete_update"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "false");
    """
    // test mor table

    sql """insert into ${table_name} values (1, '{"a":1,"b":[1],"c":1.0}')"""
    sql """insert into ${table_name} values (2, '{"a":2,"b":[1],"c":2.0}')"""
    sql """insert into ${table_name} values (3, '{"a":3,"b":[3],"c":3.0}')"""
    sql """insert into ${table_name} values (4, '{"a":4,"b":[4],"c":4.0}')"""
    sql """insert into ${table_name} values (5, '{"a":5,"b":[5],"c":5.0}')"""

    sql "delete from ${table_name} where k = 1"
    sql """update ${table_name} set v = '{"updated_value":123}' where k = 2"""
    qt_sql "select * from ${table_name} order by k"

    // MOW
    table_name = "var_delete_update_mow"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v  variant,
            vs string 
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "true", "store_row_column" = "true");
    """
    sql "insert into var_delete_update_mow select k, cast(v as string), cast(v as string) from var_delete_update"
    sql "delete from ${table_name} where k = 1"
    sql "delete from ${table_name} where k in (select k from var_delete_update_mow where k in (3, 4, 5))"

    sql """insert into var_delete_update_mow values (6, '{"a":4,"b":[4],"c":4.1}', 'xxx')"""
    sql """insert into var_delete_update_mow values (7, '{"a":4,"b":[4],"c":4.1}', 'yyy')"""
    sql """update var_delete_update_mow set vs = '{"updated_value" : 123}' where k = 6"""
    sql """update var_delete_update_mow set v = '{"updated_value":1111}' where k = 7"""
    qt_sql "select * from var_delete_update_mow order by k"

    sql """delete from ${table_name} where v = 'xxx' or vs = 'yyy'"""
    sql """delete from ${table_name} where vs = 'xxx' or vs = 'yyy'"""
    qt_sql "select * from ${table_name} order by k"

    // delete & insert concurrently
    sql "set enable_unique_key_partial_update=true;"
    sql "sync"
    def t1 = Thread.startDaemon {
        for (int k = 1; k <= 60; k++) {
            int x = new Random().nextInt(61) % 10;
            sql """insert into ${table_name}(k,vs) values(${x}, '{"k${x}" : ${x}}'),(${x+1}, '{"k${x+1}" : ${x+1}}'),(${x+2}, '{"k${x+2}" : ${x+2}}'),(${x+3}, '{"k${x+3}" : ${x+3}}')"""
        } 
    }
    def t2 = Thread.startDaemon {
        for (int k = 1; k <= 60; k++) {
            int x = new Random().nextInt(61) % 10;
            sql """insert into ${table_name}(k,v) values(${x}, '{"k${x}" : ${x}}'),(${x+1}, '{"k${x+1}" : ${x+1}}'),(${x+2}, '{"k${x+2}" : ${x+2}}'),(${x+3}, '{"k${x+3}" : ${x+3}}')"""
        } 
    }
    def t3 = Thread.startDaemon {
        for (int k = 1; k <= 60; k++) {
            int x = new Random().nextInt(61) % 10;
            sql """insert into ${table_name}(k,v) values(${x}, '{"k${x}" : ${x}}'),(${x+1}, '{"k${x+1}" : ${x+1}}'),(${x+2}, '{"k${x+2}" : ${x+2}}'),(${x+3}, '{"k${x+3}" : ${x+3}}')"""
        } 
    }
    t1.join()
    t2.join()
    t3.join()
    sql "sync"

    sql "set enable_unique_key_partial_update=false;"
     // case 1: concurrent partial update
    def tableName = "test_primary_key_partial_update_parallel"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321",
                `var` variant NULL)
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true", "disable_auto_compaction" = "true", "store_row_column" = "true")
    """

    sql """insert into ${tableName} values
        (2, "doris2", 2000, 223, 2, '{"id":2, "name":"doris2","score":2000,"test":223,"dft":2}'),
        (1, "doris", 1000, 123, 1, '{"id":1, "name":"doris","score":1000,"test":123,"dft":1}'),
        (5, "doris5", 5000, 523, 5, '{"id":5, "name":"doris5","score":5000,"test":523,"dft":5}'),
        (4, "doris4", 4000, 423, 4, '{"id":4, "name":"doris4","score":4000,"test":423,"dft":4}'),
        (3, "doris3", 3000, 323, 3, '{"id":3, "name":"doris3","score":3000,"test":323,"dft":3}');"""

    t1 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,name'

            file 'partial_update_parallel1.csv'
            time 10000 // limit inflight 10s
        }
    }

    t2 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score,test'

            file 'partial_update_parallel2.csv'
            time 10000 // limit inflight 10s
        }
    }

    t3 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,dft,var'

            file 'partial_update_parallel3.csv'
            time 10000 // limit inflight 10s
        }
    }

    t1.join()
    t2.join()
    t3.join()

    sql "sync"

    if (!isCloudMode()) {
        qt_sql """ select * from ${tableName} order by id;"""
    }
}