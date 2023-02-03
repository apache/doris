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

suite("test_offset_in_subquery_with_join", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    // define a sql table
    def testTable = "test_offset_in_subquery_with_join"

    sql """
        drop table if exists ${testTable}
    """

    sql """
        create table if not exists ${testTable}(id int) distributed by hash(id) properties('replication_num'='1')
    """

    sql """
        insert into ${testTable} values (1), (1);
    """

    test {
        sql "select * from $testTable where id in (select id from $testTable order by id limit 1, 1)"
        result([
                [1],
                [1]
        ])
    }

}
