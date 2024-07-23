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

// testAggTableCountDistinctInBitmapType
suite ("aggCDInBitmap") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS aggCDInBitmap; """

    sql """
            CREATE TABLE aggCDInBitmap (k1 int, v1 bitmap bitmap_union) Aggregate KEY (k1) DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');
        """

    sql """insert into aggCDInBitmap values(1,to_bitmap(1));"""
    sql """insert into aggCDInBitmap values(2,to_bitmap(2));"""
    sql """insert into aggCDInBitmap values(3,to_bitmap(3));"""

    sql "analyze table aggCDInBitmap with sync;"
    sql """set enable_stats=false;"""


    order_qt_select_star "select * from aggCDInBitmap order by 1;"


    explain {
        sql("select k1, count(distinct v1) from aggCDInBitmap group by k1;")
        contains "bitmap_union_count"
    }
    order_qt_select_mv "select k1, count(distinct v1) from aggCDInBitmap group by k1 order by k1;"

    sql """set enable_stats=true;"""
    explain {
        sql("select k1, count(distinct v1) from aggCDInBitmap group by k1;")
        contains "bitmap_union_count"
    }

}
