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

suite('show_profile') {
    sql "set enable_profile=true;"
    sql "drop table if exists show_profile"
    sql "create table show_profile (id int, name varchar(32)) distributed by hash(id) buckets 1 properties('replication_num'='1');"
    sql "insert into show_profile values(1, 'a'), (2, 'b'), (3, 'c');"

    for (int i = 0; i < 10; i++) {
        sql "select * from show_profile;"
    }

    boolean executedFailed = false
    try {
        sql "show query profile;"
        sql "show load profile;"
    } catch (Exception e) {
        logger.error("show query profile failed ", e)
        executedFailed = true
    }

    assertEquals(false, executedFailed)

    try {
        sql """show query profile "/";"""
        sql """show load profile "/";"""
        executedFailed = false
    } catch (Exception e) {
        logger.error("show profile failed: {}", e)
        executedFailed = true
    }

    assertEquals(false, executedFailed)

    try {
        sql """show query profile "/" limit 10;"""
        sql """show load profile "/" limit 10;"""
        executedFailed = false
    } catch (Exception e) {
        logger.error("show profile failed: {}", e)
        executedFailed = true
    }

    assertEquals(false, executedFailed)
}