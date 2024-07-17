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

suite("create_view_star_except_and_cast_to_sql") {
    sql "SET enable_nereids_planner=false;"
    sql """
         DROP TABLE IF EXISTS mal_old_create_view
        """
    sql """
        create table mal_old_create_view(pk int, a int, b int) distributed by hash(pk) buckets 10
        properties('replication_num' = '1'); 
        """

    sql """
        insert into mal_old_create_view values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6);
     """
    sql "sync"
    sql "drop view if EXISTS v_mal_old_create_view"

    sql "create view v_mal_old_create_view as select * except(a) from mal_old_create_view"

    qt_test_select_star_except "select * from v_mal_old_create_view order by pk,b"
    qt_test_select_star_except_sql "show create view v_mal_old_create_view"

    sql "drop view if EXISTS v_mal_old_create_view2"

    sql "create view v_mal_old_create_view2 as select cast(cast(a as string) as time) from mal_old_create_view"
    qt_test_sql "show create view v_mal_old_create_view2"
    sql "select * from v_mal_old_create_view2"

}
