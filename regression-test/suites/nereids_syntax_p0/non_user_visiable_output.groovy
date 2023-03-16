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

suite("non_user_visiable_output") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql """
        drop table if exists t_del;
    """
    sql """
    create table t_del (
        id int not null
        )
        UNIQUE KEY (id)
        distributed by hash(id)
        properties(
        'replication_num' = '1'
        );
    """
    sql """insert into t_del values (1),(2),(3);"""
    sql "delete from t_del where id = 2;"
    test {
        sql "select id from t_del order by id;"
        result([[1],[3]])
    }

}
