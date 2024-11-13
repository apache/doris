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

// nereids_testIncorrectRewriteCountDistinct
suite ("incRewriteCD") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS incRewriteCD; """

    sql """ create table incRewriteCD (
                time_col dateV2, 
                user_id int, 
                user_name varchar(20), 
                tag_id int) 
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """


    sql """insert into incRewriteCD values("2020-01-01",1,"a",1);"""
    sql """insert into incRewriteCD values("2020-01-02",2,"b",2);"""

    createMV("create materialized view incRewriteCD_mv as select user_id, bitmap_union(to_bitmap(tag_id)) from incRewriteCD group by user_id;")

    sleep(3000)

    sql """insert into incRewriteCD values("2020-01-01",1,"a",2);"""

    sql "analyze table incRewriteCD with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_fail("select * from incRewriteCD order by time_col;", "incRewriteCD_mv")
    order_qt_select_star "select * from incRewriteCD order by time_col,tag_id;"

    mv_rewrite_fail("select user_name, count(distinct tag_id) from incRewriteCD group by user_name;", "incRewriteCD_mv")
    order_qt_select_mv "select user_name, count(distinct tag_id) from incRewriteCD group by user_name order by user_name;"

    sql """set enable_stats=true;"""
    sql """alter table incRewriteCD modify column time_col set stats ('row_count'='3');"""

    mv_rewrite_fail("select * from incRewriteCD order by time_col;", "incRewriteCD_mv")

    mv_rewrite_fail("select user_name, count(distinct tag_id) from incRewriteCD group by user_name;", "incRewriteCD_mv")
}
