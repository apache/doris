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

 suite("test_filter_pushdown_set") {
     sql "SET enable_nereids_planner=true"
     sql "SET enable_fallback_to_original_planner=false"
     qt_select1 'select * from (select 1 as a, 2 as b union all select 1, 3 union all select 1, 3) t where a = 1 order by a, b;'
     qt_select2 'select * from (select 1 as a, 2 as b union select 1, 3 union select 1, 3) t where a = 1 order by a, b;'
     qt_select3 'select * from ((select 1 as a, 2 as b union all select 1, 3) intersect select 1, 2) t where a = 1 order by a, b;'
     qt_select4 'select * from ((select 1 as a, 2 as b union all select 1, 3) except select 1, 2 ) t where a = 1 order by a, b;'
     qt_select5 'select * from (select 1 as a, 2 as b union all select 1, 3 union all select 1, 2) t where b = 2 order by a, b;'
     qt_select6 'select * from (select 1 as a, 2 as b union select 1, 3 union select 1, 2) t where b = 2 order by a, b;'
     qt_select7 'select * from ((select 1 as a, 2 as b union all select 1, 3) intersect select 1, 2) t where b = 2 order by a, b;'
     qt_select8 'select * from ((select 1 as a, 2 as b union all select 1, 3) except select 1, 2 ) t where b = 2 order by a, b;'

     explain {
        sql("select * from ((select 1 as a, 2 as b union all select 1, 3) intersect select 1, 2) t where a = 1;")
        notContains "VSELECT"
     }

     explain {
        sql("select * from ((select 1 as a, 2 as b union all select 1, 3) except select 1, 2 ) t where b = 2;")
        notContains "1 | 3"
     }

     sql "SET enable_nereids_planner=false"
     qt_select22 'select * from (select 1 as a, 2 as b union all select 3, 3) t where a = 1 order by a, b;'
     qt_select23 'select * from (select 1 as a, 2 as b union select 3, 3) t where a = 1 order by a, b;'
 }