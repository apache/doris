/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("cse") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set disable_join_reorder=true;'
//
//    qt_proj_scan "select (s_suppkey + s_nationkey) as x, 1+(s_suppkey + s_nationkey), s_name from supplier order by x, s_name"
//    qt_proj_join "select (s_suppkey + n_regionkey) + 1 as x, (s_suppkey + n_regionkey) + 2 as y from supplier join nation on s_nationkey=n_nationkey"

//    explain select(s_suppkey + s_nationkey), (s_suppkey + s_nationkey) * 2, (s_suppkey + s_nationkey) * 2 + 4, abs((s_suppkey + s_nationkey) * 2 + 4) from supplier;
//    explain select s_suppkey , s_nationkey , (s_suppkey + s_nationkey) , (s_suppkey + s_nationkey) * 2 ,
//    (s_suppkey + s_nationkey) * 2 + 4 , abs((s_suppkey + s_nationkey) * 2 + 4) from supplier;
//
//    explain select(s_suppkey + s_nationkey) as x, 1 + (s_suppkey + s_nationkey), abs(s_suppkey + s_nationkey), s_name from supplier order by x
//    , s_name;
//
//    explain select abs(s_suppkey * 2 + 1) , (s_suppkey * 2 + 1) , s_name from supplier;
}