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

suite ("testBitmapUnionInQuery") {

    sql """ DROP TABLE IF EXISTS user_tags; """

    sql """ create table user_tags (
                time_col date, 
                user_id int, 
                user_name varchar(20), 
                tag_id int) 
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into user_tags values("2020-01-01",1,"a",1);"""
    sql """insert into user_tags values("2020-01-02",2,"b",2);"""

    createMV("create materialized view user_tags_mv as select user_id, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id;")

    sql """insert into user_tags values("2020-01-01",1,"a",2);"""

    sql "analyze table user_tags with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("select * from user_tags order by time_col;")
        contains "(user_tags)"
    }
    qt_select_star "select * from user_tags order by time_col,tag_id;"

    explain {
        sql("select user_id, bitmap_union_count(to_bitmap(tag_id)) a from user_tags group by user_id having a>1 order by a;")
        contains "(user_tags_mv)"
    }
    qt_select_mv "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from user_tags group by user_id having a>1 order by a;"

    explain {
        sql("select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) a from user_tags group by user_id having a>1 order by a;")
        contains "(user_tags_mv)"
    }
    qt_select_mv "select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) a from user_tags group by user_id having a>1 order by a;"

    sql """set enable_stats=true;"""

    explain {
        sql("select * from user_tags order by time_col;")
        contains "(user_tags)"
    }

    explain {
        sql("select user_id, bitmap_union_count(to_bitmap(tag_id)) a from user_tags group by user_id having a>1 order by a;")
        contains "(user_tags_mv)"
    }

    explain {
        sql("select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) a from user_tags group by user_id having a>1 order by a;")
        contains "(user_tags_mv)"
    }
}
