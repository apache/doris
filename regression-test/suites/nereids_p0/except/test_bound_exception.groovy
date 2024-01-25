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

suite("test_bound_exception") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def tbName = "test_bound_exception"

    sql """ DROP TABLE IF EXISTS ${tbName} """
    sql """
        create table if not exists ${tbName} (id int, name char(10))
        distributed by hash(id) buckets 1 properties("replication_num"="1");
    """
    test {
        sql "SELECT id FROM ${tbName} GROUP BY id ORDER BY id123"
        exception "Unknown column 'id123' in 'table list' in SORT clause"
    }
    test {
        sql "SELECT id123 FROM ${tbName} ORDER BY id"
        exception "Unknown column 'id123' in 'table list' in PROJECT clause"
    }
    test {
        sql "SELECT id123 FROM ${tbName} GROUP BY id ORDER BY id"
        exception "Unknown column 'id123' in 'table list' in AGGREGATE clause"
    }
    test {
        sql "SELECT id FROM ${tbName} GROUP BY id123 ORDER BY id"
        exception "Unknown column 'id123' in 'table list' in AGGREGATE clause"
    }
    test {
        sql "SELECT id FROM ${tbName} WHERE id = (SELECT id from ${tbName} ORDER BY id123 LIMIT 1) ORDER BY id"
        exception "Unknown column 'id123' in 'table list' in SORT clause"
    }
    test {
        sql "SELECT id FROM ${tbName} WHERE id123 = 123 ORDER BY id"
        exception "Unknown column 'id123' in 'table list' in FILTER clause"
    }
}
