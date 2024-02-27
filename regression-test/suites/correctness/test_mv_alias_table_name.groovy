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


/*
exception throw before bug fix:  Unknown column 'mv_bitmap_union_mh' in 'test.original_table'
*/
suite("test_mv_alias_table_name") {
    sql """
        DROP TABLE IF EXISTS original_table;
    """
    sql """
        CREATE TABLE original_table
        (
            day date,
            aid bigint,
            lid bigint,
            yid bigint,
            mh bigint,
            my bigint
        )
        DUPLICATE KEY(`day`, `aid`, `lid`)
        DISTRIBUTED BY HASH(aid)
        PROPERTIES ("replication_num" = "1" );
    """

    sql """
        create materialized view mv_table as
        select day,aid,lid,
            bitmap_union(to_bitmap(mh)) as wu,     
            bitmap_union(to_bitmap(my)) as mu 
        from original_table 
        group by day, aid, lid;
    """

    sql """
        insert into original_table values('2022-10-16', 1665710553, 1665710553, 1665710553, 1665700553, 1665700553);
    """

    sleep(2000)

    sql """
        select t0.aid, t0.lid, count(distinct mh), count(distinct my) 
        from original_table t0 
        where t0.day = '2022-10-16' and t0.lid > 0 group by t0.aid, t0.lid;
    """

 }

