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

suite("test_timeout_fallback") {
    sql "set enable_nereids_planner=true"
    sql "set enable_nereids_timeout=true"
    sql "set nereids_timeout_second=-1"

    test {
        sql "select 1"
        exception "Nereids cost too much time"
    }

    test {
        sql "explain select 1"
        exception "Nereids cost too much time"
    }

    sql "drop table if exists test_timeout_fallback"

    sql """
        create table test_timeout_fallback (id int) distributed by hash(id) properties ('replication_num'='1')
    """

    test {
        sql "insert into test_timeout_fallback values (1)"
        exception "Nereids cost too much time"
    }
}
