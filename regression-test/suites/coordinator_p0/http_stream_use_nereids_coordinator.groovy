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

suite('http_stream_use_nereids_coordinator') {
    multi_sql """
        set enable_nereids_distribute_planner=true;
        set enable_sql_cache=false;
        drop table if exists http_stream_tbl;
        create table if not exists http_stream_tbl(
            id int,
            value int
        )
        unique key(id)
        distributed by hash(id)
        properties('replication_num'='1');
        """

    def db = getCurDbName()
    streamLoad {
        set "version", ""
        set "sql", "insert into ${db}.http_stream_tbl(id, value) select c1 + 1, c2 + 2 from http_stream(\"format\" = \"CSV\", \"column_separator\" = \"\\t\")"
        inputIterator([[1, 2], [2, 3], [3, 4]].iterator())
    }

    test {
        sql "select * from http_stream_tbl order by id"
        result([[2, 4], [3, 5], [4, 6]])
    }
}