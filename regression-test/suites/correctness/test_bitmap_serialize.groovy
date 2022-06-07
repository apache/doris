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

 suite("test_bitmap_serialize") {
     def tableName = "test_bitmap"

     sql """ DROP TABLE IF EXISTS ${tableName} """
     sql """
        create table ${tableName} (tag varchar(20),user_ids bitmap bitmap_union) aggregate key (tag) 
        distributed by hash (tag) PROPERTIES("replication_num" = "1"); 
     """

     sql " insert into ${tableName} values('A', to_bitmap(1)); "
     sql " insert into ${tableName} values('A', to_bitmap(2)); "
     sql " insert into ${tableName} values('A', to_bitmap(3)); "
     sql " insert into ${tableName} values('B', to_bitmap(1)); "
     sql " insert into ${tableName} values('B', to_bitmap(2)); "

     // test no vectorized
     sql """ set enable_vectorized_engine = false; """

     qt_select_default """ 
     select bitmap_to_string(bitmap_intersect(user_ids)) from ( select tag, bitmap_union(user_ids) user_ids 
     from ${tableName} group by tag having tag not in("A","B") ) t; """
 } 