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

suite("test_insert_overwrite_generated_column") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    multi_sql """
        drop table if exists gen_col_insert_overwrite_src;
        CREATE TABLE gen_col_insert_overwrite_src(a int, b int, c int,d int) ENGINE=OLAP
        UNIQUE KEY(`a`, `b`, `c`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 10 properties("replication_num"="1");
        insert into gen_col_insert_overwrite_src values(1,2,3,5),(3,23,5,1);
        
        drop table if exists gen_col_insert_overwrite;
        CREATE TABLE gen_col_insert_overwrite(a int, b int, c int AS(a+b), d int AS (c+1))
        ENGINE=OLAP
        UNIQUE KEY(`a`, `b`, `c`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 10 properties("replication_num"="1");
        INSERT into gen_col_insert_overwrite values(1,2,DEFAULT,default),(3,4,DEFAULT,default);
    """

    qt_overwrite_value_partial_column "INSERT overwrite TABLE gen_col_insert_overwrite(a,b) values(3,4)"
    qt_overwrite_value "INSERT overwrite TABLE gen_col_insert_overwrite values(1,2,DEFAULT,default),(3,4,DEFAULT,default);"
    qt_overwrite_value_select "select * from gen_col_insert_overwrite order by 1,2,3,4;"
    qt_overwrite_multi_value "INSERT overwrite TABLE gen_col_insert_overwrite values(1,2,DEFAULT,default),(3,4,DEFAULT,default);"
    qt_overwrite_multi_value_select "select * from gen_col_insert_overwrite order by 1,2,3,4;"
    qt_overwrite_multi_value_partial_column "INSERT overwrite TABLE gen_col_insert_overwrite(a,b) values(1,2),(3,4);"
    qt_overwrite_multi_value_partial_column_select "select * from gen_col_insert_overwrite order by 1,2,3,4;"
    qt_overwrite_select_value_partial_column "INSERT overwrite TABLE gen_col_insert_overwrite(a,b) select 1,2;"
    qt_overwrite_select_value_partial_column_select "select * from gen_col_insert_overwrite order by 1,2,3,4;"
    qt_overwrite_select_table_partial_column "INSERT overwrite TABLE gen_col_insert_overwrite(a,b) select a,b from gen_col_insert_overwrite_src;"
    qt_overwrite_select_table_partial_column_select "select * from gen_col_insert_overwrite order by 1,2,3,4;"

    multi_sql """
        drop table if exists gen_col_insert_overwrite_par;
        CREATE TABLE IF NOT EXISTS gen_col_insert_overwrite_par (
                `c1` int NOT NULL DEFAULT "1",
                `c2` int NOT NULL DEFAULT "4",
                `c3` int as(c1+c2)
        ) ENGINE=OLAP
        UNIQUE KEY(`c1`)
        PARTITION BY LIST (`c1`)
        (
                PARTITION p1 VALUES IN ("1","2","3"),
                        PARTITION p2 VALUES IN ("4","5","6")
        )
        DISTRIBUTED BY HASH(`c1`) BUCKETS 3
        properties("replication_num"="1");
    """

    sql "insert into gen_col_insert_overwrite_par values(1,5,default),(4,3,default);"
    sql "INSERT OVERWRITE table gen_col_insert_overwrite_par PARTITION(p1,p2) (c1,c2) VALUES (1, 2);"
    qt_overwrite_partition_single_value "select * from gen_col_insert_overwrite_par order by 1,2,3;"
    sql "insert into gen_col_insert_overwrite_par values(1,5,default),(4,3,default);"
    sql "INSERT OVERWRITE table gen_col_insert_overwrite_par PARTITION(p1) (c1,c2) VALUES (1, 2);"
    qt_overwrite_partition_partial_single_value "select * from gen_col_insert_overwrite_par order by 1,2,3;"

    sql "insert into gen_col_insert_overwrite_par values(1,5,default),(4,3,default);"
    sql "INSERT OVERWRITE table gen_col_insert_overwrite_par PARTITION(p1,p2) (c1,c2) VALUES (1, 2), (4, 2 * 2), (5, DEFAULT);"
    qt_overwrite_partition_multi_value "select * from gen_col_insert_overwrite_par order by 1,2,3;"
    sql "insert into gen_col_insert_overwrite_par values(1,5,default),(4,3,default);"
    sql "INSERT OVERWRITE table gen_col_insert_overwrite_par PARTITION(p1) (c1) VALUES (1),(2),(3);"
    qt_overwrite_partition_partial_multi_value "select * from gen_col_insert_overwrite_par order by 1,2,3;"

    sql "insert into gen_col_insert_overwrite_par values(1,5,default),(4,3,default);"
    sql "INSERT OVERWRITE table gen_col_insert_overwrite_par PARTITION(*) (c1) VALUES (1), (2),(3);"
    qt_auto_partition_overwrite "select * from gen_col_insert_overwrite_par order by 1,2,3;"

    test {
        sql "insert into gen_col_insert_overwrite_par values(1,5,2),(4,3,3);"
        exception "The value specified for generated column 'c3' in table 'gen_col_insert_overwrite_par' is not allowed."
    }
    test {
        sql "insert into gen_col_insert_overwrite_par values(1,5,2);"
        exception "The value specified for generated column 'c3' in table 'gen_col_insert_overwrite_par' is not allowed."
    }
    test {
        sql "insert into gen_col_insert_overwrite_par(c1,c2,c3) values(1,5,2),(4,3,3);"
        exception "The value specified for generated column 'c3' in table 'gen_col_insert_overwrite_par' is not allowed."
    }
    test {
        sql "insert into gen_col_insert_overwrite_par(c1,c2,c3) values(1,5,2);"
        exception "The value specified for generated column 'c3' in table 'gen_col_insert_overwrite_par' is not allowed."
    }
    test{
        sql "INSERT overwrite TABLE gen_col_insert_overwrite values(1,2,5,default),(3,4,DEFAULT,3);"
        exception "The value specified for generated column 'c' in table 'gen_col_insert_overwrite' is not allowed."
    }
    test {
        sql "insert into gen_col_insert_overwrite_par select 1,2,3;"
        exception "The value specified for generated column 'c3' in table 'gen_col_insert_overwrite_par' is not allowed."
    }
    test {
        sql "insert into gen_col_insert_overwrite_par select * from gen_col_insert_overwrite_src"
        exception "The value specified for generated column 'c3' in table 'gen_col_insert_overwrite_par' is not allowed."
    }
    test {
        sql "insert into gen_col_insert_overwrite_par(c1,c2,c3) select * from gen_col_insert_overwrite_src;"
        exception "The value specified for generated column 'c3' in table 'gen_col_insert_overwrite_par' is not allowed."
    }

}