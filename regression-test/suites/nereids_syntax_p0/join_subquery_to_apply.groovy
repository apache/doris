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

suite("join_subquery_to_apply") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    qt_sql"""
        select 
        ref_25.`user_name` as c3
        from
        (select 'zhangsan' as user_name) as ref_25 
        where
        bitxor(cast(find_in_set(cast(coalesce(hex(cast((select 211985) as bigint)), ref_25.`user_name`) as varchar), 'zhangsan') as int), 1) is NULL;
    """
}
