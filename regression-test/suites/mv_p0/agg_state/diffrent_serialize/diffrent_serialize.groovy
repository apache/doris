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

suite ("diffrent_serialize") {

    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,3,null,'c';"

    createMV("create materialized view mv1_1 as select k1,bitmap_intersect(to_bitmap(k2)) from d_table group by k1;")
    createMV("create materialized view mv1 as select k1,bitmap_agg(k2) from d_table group by k1;")
    createMV("create materialized view mv1_2 as select k1, multi_distinct_group_concat(k4) from d_table group by k1 order by k1;")
    createMV("create materialized view mv1_3 as select k1, multi_distinct_sum(k3) from d_table group by k1 order by k1;")
    /*
    createMV("create materialized view mv2 as select k1,map_agg(k2,k3) from d_table group by k1;")
    createMV("create materialized view mv3 as select k1,array_agg(k2) from d_table group by k1;")
    createMV("create materialized view mv4 as select k1,collect_list(k2,3) from d_table group by k1;")
    createMV("create materialized view mv5 as select k1,collect_set(k2,3) from d_table group by k1;")
    */

    sql "insert into d_table select -4,4,-4,'d';"
    sql "insert into d_table(k4,k2) values('d',4);"

    qt_select_star "select * from d_table order by k1;"

    explain {
        sql("select k1,bitmap_to_string(bitmap_agg(k2)) from d_table group by k1 order by 1;")
        contains "(mv1)"
    }
    qt_select_mv "select k1,bitmap_to_string(bitmap_agg(k2)) from d_table group by k1 order by 1;"

    explain {
        sql("select k1,bitmap_to_string(bitmap_intersect(to_bitmap(k2))) from d_table group by k1 order by 1;")
        contains "(mv1_1)"
    }
    qt_select_mv "select k1,bitmap_to_string(bitmap_intersect(to_bitmap(k2))) from d_table group by k1 order by 1;"

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,2,1,'a';"

    explain {
        sql("select k1,bitmap_count(bitmap_agg(k2)) from d_table group by k1 order by 1;")
        contains "(mv1)"
    }
    qt_select_mv "select k1,bitmap_count(bitmap_agg(k2)) from d_table group by k1 order by 1;"

    explain {
        sql("select k1, multi_distinct_sum(k3) from d_table group by k1 order by k1;")
        contains "(mv1_3)"
    }
    qt_select_mv "select k1, multi_distinct_sum(k3) from d_table group by k1 order by k1;"

    explain {
        sql("select k1, multi_distinct_group_concat(k4) from d_table group by k1 order by k1;")
        contains "(mv1_2)"
    }
    qt_select_mv "select k1, multi_distinct_group_concat(k4) from d_table group by k1 order by k1;"


/*
    explain {
        sql("select k1,map_agg(k2,k3) from d_table group by k1 order by 1;")
        contains "(mv2)"
    }
    qt_select_mv "select k1,map_agg(k2,k3) from d_table group by k1 order by 1;"

    explain {
        sql("select k1,array_agg(k2) from d_table group by k1 order by 1;")
        contains "(mv3)"
    }
    qt_select_mv "select k1,array_agg(k2) from d_table group by k1 order by 1;"

    explain {
        sql("select k1,collect_list(k2,3) from d_table group by k1 order by 1;")
        contains "(mv4)"
    }
    qt_select_mv "select k1,collect_list(k2,3) from d_table group by k1 order by 1;"

    explain {
        sql("select k1,collect_set(k2,3) from d_table group by k1 order by 1;")
        contains "(mv5)"
    }
    qt_select_mv "select k1,collect_set(k2,3) from d_table group by k1 order by 1;"
    */
}
