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

suite('hot_value_analyze_sync') {
    sql """
    drop table if exists t1025;
    create table t1025(a_1 int, b_5 int, c_10 int, d_1025 int) distributed by hash(c_10) properties('replication_num'='1');
    insert into t1025 select 1, number%5 , number%10, number from numbers("number"="100");
    """
    def passed = false
    def retry = 100
    while (!passed && retry-- > 0) {
        sql "analyze table t1025 with sample rows 4000 with sync;"
        def result = sql "show column cached stats t1025"
        log.info(result.toString())
        // check hot value for d_1025 is null
        if (!result.isEmpty()) {
            assertEquals(result[3][17].toString(), "null")
            passed = true
        }
    }
}