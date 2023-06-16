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

suite('nereids_delete_using') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'

    sql 'insert into t1(id, c1, c2, c3) select id, c1 * 2, c2, c3 from t1'
    sql 'insert into t2(id, c1, c2, c3) select id, c1, c2 * 2, c3 from t2'
    sql 'insert into t2(c1, c3) select c1 + 1, c3 + 1 from (select id, c1, c3 from t1 order by id, c1 limit 10) t1, t3'

    qt_sql 'select * from t1 order by id, id1'

    sql '''
        delete from t1
        using t2 join t3 on t2.id = t3.id
        where t1.id = t2.id;
    '''

    qt_sql 'select * from t1 order by id, id1'
}
