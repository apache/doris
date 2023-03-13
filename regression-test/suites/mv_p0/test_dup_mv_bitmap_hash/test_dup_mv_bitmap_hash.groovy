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

suite ("test_dup_mv_bitmap_hash") {
    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                k1 int null,
                k2 int null,
                k3 varchar(100) null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,'a';"
    sql "insert into d_table select 2,2,'b';"
    sql "insert into d_table select 3,3,'c';"

    createMV( "create materialized view k1g2bm as select k1,bitmap_union(to_bitmap(k2)) from d_table group by k1;")

    explain {
        sql("select bitmap_union_count(to_bitmap(k2)) from d_table group by k1 order by k1;")
        contains "(k1g2bm)"
    }
    qt_select_mv "select bitmap_union_count(to_bitmap(k2)) from d_table group by k1 order by k1;"

    result = "null"
    sql "create materialized view k1g3bm as select k1,bitmap_union(bitmap_hash(k3)) from d_table group by k1;"
    while (!result.contains("FINISHED")){
        result = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='d_table' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("CANCELLED")){
            return 
        }
        Thread.sleep(1000)
    }

    sql "insert into d_table select 2,2,'bb';"
    sql "insert into d_table select 3,3,'c';"

    qt_select_k1 "select k1 from d_table order by k1;"

    qt_select_star "select * from d_table order by k1,k2,k3;"

    explain {
        sql("select k1,bitmap_union_count(bitmap_hash(k3)) from d_table group by k1;")
        contains "(k1g3bm)"
    }
    qt_select_mv_sub "select k1,bitmap_union_count(bitmap_hash(k3)) from d_table group by k1 order by k1;"
}
