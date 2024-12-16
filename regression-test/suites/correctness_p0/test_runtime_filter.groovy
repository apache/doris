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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_runtime_filter") {
    sql "set enable_runtime_filter_prune=false;"
    sql """ DROP TABLE IF EXISTS rf_tblA """
    sql """
            CREATE TABLE IF NOT EXISTS rf_tblA (
                a int
            )
            DUPLICATE KEY(a)
            DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    
    sql """ DROP TABLE IF EXISTS rf_tblB """
    sql """
            CREATE TABLE IF NOT EXISTS rf_tblB (
                b int
            )
            DUPLICATE KEY(b)
            DISTRIBUTED BY HASH(b) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    sql """
        CREATE TABLE IF NOT EXISTS rf_tblC (
                c int
            )
            DUPLICATE KEY(c)
            DISTRIBUTED BY HASH(c) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """

    sql "set enable_pipeline_engine=true;"
    sql "set runtime_filter_type=4"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "set ignore_storage_data_distribution=false"
    explain{
        sql ("""select * from rf_tblA join rf_tblB on a < b""")
        contains "runtime filters: RF000[max] -> a"
        contains "runtime filters: RF000[max] <- b"
    } 

    explain{
        sql ("""select * from rf_tblA join rf_tblB on a > b""")
        contains "runtime filters: RF000[min] -> a"
        contains "runtime filters: RF000[min] <- b"
    } 

    explain{
        sql ("""select * from rf_tblA join rf_tblB on b < a""")
        contains "runtime filters: RF000[min] -> a"
        contains "runtime filters: RF000[min] <- b"
    } 

    explain{
        sql ("""select * from rf_tblA right outer join rf_tblB on a < b""")
        contains "runtime filters: RF000[max] <- b"
        contains "runtime filters: RF000[max] -> a"
    }

    explain{
        sql ("""select * from rf_tblA left join rf_tblB on a < b; """)
        notContains "runtime filters"
    }

    explain{
        sql ("""select * from rf_tblA full outer join rf_tblB on a = b; """)
        notContains "runtime filters"
    }

    explain{
        sql ("""
            with x as (select * from rf_tblA join rf_tblB on a=b)
            select * from x join rf_tblC on x.b <= rf_tblC.c
            union 
            select * from x join rf_tblC on x.b <= rf_tblC.c
            """)
        contains "runtime filters: RF001[max] -> b"
        contains "runtime filters: RF002[max] -> b"
        contains "runtime filters: RF001[max] <- c"
        contains "runtime filters: RF002[max] <- c"

    }

    sql """ DROP TABLE IF EXISTS v_table """
    sql """
            create table v_table (
                kc char(100),
                kv varchar(100)
            )
            duplicate key (kc)
            distributed BY hash(kv) buckets 1
            properties("replication_num" = "1");
        """

    sql """ DROP TABLE IF EXISTS c_table """
    sql """
            create table c_table (
                kc char(100),
                kv varchar(100)
            )
            duplicate key (kc)
            distributed BY hash(kv) buckets 1
            properties("replication_num" = "1");
        """
    sql """
    insert into c_table values ('a','a'),('b','b');
    """
    sql """
    insert into v_table values ('a','a'),('b','b'),('c','c');
    """
    sql "set runtime_filter_type='1';"
    qt_test "select * from c_table,v_table where c_table.kc=v_table.kv;"
    qt_test "select * from c_table,v_table where c_table.kv=v_table.kc;"
    sql "set runtime_filter_type='2';"
    qt_test "select * from c_table,v_table where c_table.kc=v_table.kv;"
    qt_test "select * from c_table,v_table where c_table.kv=v_table.kc;"
    sql "set runtime_filter_type='4';"
    qt_test "select * from c_table,v_table where c_table.kc=v_table.kv;"
    qt_test "select * from c_table,v_table where c_table.kv=v_table.kc;"
    sql "set runtime_filter_type='8';"
    qt_test "select * from c_table,v_table where c_table.kc=v_table.kv;"
    qt_test "select * from c_table,v_table where c_table.kv=v_table.kc;"
}
