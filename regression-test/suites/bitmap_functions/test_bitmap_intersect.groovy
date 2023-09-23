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

suite("test_bitmap_intersect", "p0") {

    def tbName = "test_bitmap_intersect"
    sql """ DROP TABLE IF EXISTS ${tbName} """
    sql """ create table if not exists ${tbName} (tag varchar(20),user_ids bitmap bitmap_union) aggregate key (tag) distributed by hash (tag) PROPERTIES("replication_num" = "1"); """
    sql """ insert into ${tbName} values('A', to_bitmap(1)); """
    sql """ insert into ${tbName} values('A', to_bitmap(2)); """
    sql """ insert into ${tbName} values('A', to_bitmap(3)); """
    sql """ insert into ${tbName} values('B', to_bitmap(1)); """
    sql """ insert into ${tbName} values('B', to_bitmap(2)); """


   qt_sql  """ select
       bitmap_to_string(bitmap_intersect(user_ids))
       from
       (
           select
             tag,
             bitmap_union(user_ids) user_ids
           from
             ${tbName}
           group by
             tag
       ) t
       """
   qt_sql  """ select
       bitmap_to_string(bitmap_intersect(user_ids))
       from
       (
           select
             tag,
             bitmap_union(user_ids) user_ids
           from
             ${tbName}
           group by
             tag having tag not in ("A","B")
       ) t
       """
    qt_sql  """ select
       bitmap_to_string(bitmap_intersect(user_ids))
       from
       (
           select
             tag,
             bitmap_union(user_ids) user_ids
           from
             ${tbName}
           group by
             tag 
       ) t
       """
    qt_sql  """ select
       bitmap_to_string(bitmap_intersect(user_ids))
       from
       (
           select
             tag,
             bitmap_union(user_ids) user_ids
           from
             ${tbName}
           group by
             tag having tag not in ("A","B")
       ) t
       """

    sql """ DROP TABLE  ${tbName} """
}
