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

suite("test_nereids_show_create_user") {
    sql "DROP USER IF EXISTS 'xxxxxxx'"
    sql "CREATE USER IF NOT EXISTS 'xxxxxxx' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

    checkNereidsExecute("SHOW ALL CREATE USER")
    checkNereidsExecute("SHOW CREATE USER xxxxxxx")

    def res1 = sql """SHOW ALL CREATE USER"""
    assertEquals(true, res1.size() > 1)

    def res2 = sql """SHOW CREATE USER xxxxxxx"""
    assertEquals('xxxxxxx', res2.get(0).get(0))
}
