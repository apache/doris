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

suite("colocate_agg_join") {
    multi_sql """
        drop table if exists colocate_agg_join1;
        drop table if exists colocate_agg_join2;

        create table colocate_agg_join1 (
          col_int_undef_signed_not_null int  not null ,
          col_date_undef_signed_not_null date  not null ,
          pk int,
          col_int_undef_signed int  null ,
          col_date_undef_signed date  null ,
          col_varchar_10__undef_signed varchar(10)  null ,
          col_varchar_10__undef_signed_not_null varchar(10)  not null ,
          col_varchar_1024__undef_signed varchar(1024)  null ,
          col_varchar_1024__undef_signed_not_null varchar(1024)  not null
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed_not_null, col_date_undef_signed_not_null, pk)
        PARTITION BY RANGE(col_int_undef_signed_not_null, col_date_undef_signed_not_null) (PARTITION p0 VALUES [('-10000', '2023-12-01'), ('3', '2023-12-10')), PARTITION p1 VALUES [('3', '2023-12-10'), ('6', '2023-12-15')), PARTITION p2 VALUES [('6', '2023-12-15'), ('10000', '2023-12-21')))
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into colocate_agg_join1(pk,col_int_undef_signed,col_int_undef_signed_not_null,col_date_undef_signed,col_date_undef_signed_not_null,col_varchar_10__undef_signed,col_varchar_10__undef_signed_not_null,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_not_null) values (0,null,6,'2023-12-18','2023-12-20','b','g','were','as'),(1,4,0,'2023-12-16','2023-12-16','have','l','be','do'),(2,null,3,'2023-12-14','2023-12-14','there','why','were','s'),(3,null,2,'2023-12-20','2023-12-16','i','y','at','v'),(4,null,1,'2023-12-20','2023-12-09','x','some','why','e'),(5,null,0,'2023-12-13','2023-12-13','n','didn''t','get','z'),(6,2,7,'2023-12-11','2023-12-12','want','h','you','m'),(7,6,6,'2023-12-18','2023-12-19','your','look','e','i'),(8,8,1,'2023-12-11','2023-12-19','j','a','when','i'),(9,2,8,'2023-12-13','2023-12-16','v','his','v','had'),(10,null,2,'2023-12-17','2023-12-14','who','z','were','d'),(11,5,6,'2023-12-12','2023-12-15','z','been','z','he'),(12,2,0,'2023-12-16','2023-12-13','no','a','not','been'),(13,4,7,'2023-12-16','2023-12-18','o','about','z','it'),(14,6,2,'2023-12-10','2023-12-09','he','ok','n','about'),(15,2,1,'2023-12-15','2023-12-19','y','e','l','his'),(16,3,0,'2023-12-16','2023-12-18','b','that','for','yes'),(17,3,5,'2023-12-15','2023-12-15','e','I''m','h','could'),(18,null,9,'2023-12-15','2023-12-16','j','n','about','on'),(19,1,3,'2023-12-15','2023-12-15','had','would','in','no');

        create table colocate_agg_join2 (
          pk int,
          col_int_undef_signed int  null ,
          col_int_undef_signed_not_null int  not null ,
          col_date_undef_signed date  null ,
          col_date_undef_signed_not_null date  not null ,
          col_varchar_10__undef_signed varchar(10)  null ,
          col_varchar_10__undef_signed_not_null varchar(10)  not null ,
          col_varchar_1024__undef_signed varchar(1024)  null ,
          col_varchar_1024__undef_signed_not_null varchar(1024)  not null
        ) engine=olap
        DUPLICATE KEY(pk)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into colocate_agg_join2(pk,col_int_undef_signed,col_int_undef_signed_not_null,col_date_undef_signed,col_date_undef_signed_not_null,col_varchar_10__undef_signed,col_varchar_10__undef_signed_not_null,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_not_null) values (0,4,0,'2023-12-18','2023-12-11','to','see','he''s','r'),(1,4,5,'2023-12-10','2023-12-09','him','s','didn''t','k');

        set disable_join_reorder=true;
        set enable_local_shuffle=true;
        set runtime_filter_mode=off;
    
    """

    for (def i in 1..10) {
        sql "set parallel_pipeline_task_num=${i}"
        test {
            sql """
                select avg_pk
                from (
                   select t1.*
                   from colocate_agg_join1 AS alias1
                   right anti join (
                     select pk, avg(distinct pk) avg_pk
                     from colocate_agg_join2
                     group by pk
                   ) t1
                   ON t1.`pk` = alias1.`pk`
                )a
                order by pk
                limit 1000
                """
            rowNum 0
        }
    }
}