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

suite ("bitmapUnionIn") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS bitmapUnionIn; """

    sql """ create table bitmapUnionIn (
                time_col dateV2, 
                user_id int, 
                user_name varchar(20), 
                tag_id int) 
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into bitmapUnionIn values("2020-01-01",1,"a",1);"""
    sql """insert into bitmapUnionIn values("2020-01-02",2,"b",2);"""

    createMV("create materialized view bitmapUnionIn_mv as select user_id, bitmap_union(to_bitmap(tag_id)) from bitmapUnionIn group by user_id;")

    sleep(3000)

    sql """insert into bitmapUnionIn values("2020-01-01",1,"a",2);"""

    sql "analyze table bitmapUnionIn with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("select * from bitmapUnionIn order by time_col;")
        contains "(bitmapUnionIn)"
    }
    order_qt_select_star "select * from bitmapUnionIn order by time_col,tag_id;"

    explain {
        sql("select user_id, bitmap_union_count(to_bitmap(tag_id)) a from bitmapUnionIn group by user_id having a>1 order by a;")
        contains "(bitmapUnionIn_mv)"
    }
    order_qt_select_mv "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from bitmapUnionIn group by user_id having a>1 order by a;"

    sql """set enable_stats=true;"""
    explain {
        sql("select * from bitmapUnionIn order by time_col;")
        contains "(bitmapUnionIn)"
    }

    explain {
        sql("select user_id, bitmap_union_count(to_bitmap(tag_id)) a from bitmapUnionIn group by user_id having a>1 order by a;")
        contains "(bitmapUnionIn_mv)"
    }
}
