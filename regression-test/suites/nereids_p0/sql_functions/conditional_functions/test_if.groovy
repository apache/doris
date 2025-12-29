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

suite("test_if") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "select if(id=1,count,hll_empty()) from (select 1 as id, hll_hash(1) as count) t"


    sql "set debug_skip_fold_constant = true"

    qt_sql """
    select if(jsonb_exists_path(CAST('{"a":1}' AS json), '\$.b'), 'EXISTS', 'NOT_EXISTS')
"""

    qt_sql """
select if(jsonb_exists_path(CAST(' {"a":1}' AS json), '\$.b'), 'NOT_EXISTS', 'EXISTS');
"""

    qt_sql """
select if('EXISTS', jsonb_exists_path(CAST(' {"a":1}' AS json), '\$.b'), 'NOT_EXISTS');
"""

    qt_sql """
select if('EXISTS', 'NOT_EXISTS', jsonb_exists_path(CAST(' {"a":1}' AS json), '\$.b'));
"""

    qt_sql """
select if('NOT_EXISTS', jsonb_exists_path(CAST(' {"a":1}' AS json), '\$.b'), 'EXISTS');
"""

    qt_sql """
select if('NOT_EXISTS', 'EXISTS', jsonb_exists_path(CAST(' {"a":1}' AS json), '\$.b'));
"""


}
