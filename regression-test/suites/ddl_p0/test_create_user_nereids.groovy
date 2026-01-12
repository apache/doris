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

suite("test_create_user_nereids") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    sql "DROP USER IF EXISTS 'jack';"
    sql "DROP USER IF EXISTS jack1@'172.10.1.10';"
    sql "DROP USER IF EXISTS jack2;"
    sql "DROP USER IF EXISTS 'jack3'@'192.168.%';"
    sql "DROP USER IF EXISTS 'jack4'@('csding');"
    sql "DROP USER IF EXISTS 'jack5'@'%';"
    sql "DROP USER IF EXISTS 'jack6';"
    sql "DROP USER IF EXISTS 'jack7';"
    sql """DROP USER IF EXISTS 'jack8';"""

    sql "CREATE USER 'jack';"
    sql "CREATE USER jack1@'172.10.1.10' IDENTIFIED BY '123456';"
    sql "CREATE USER jack2 IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';"
    sql "CREATE USER 'jack3'@'192.168.%' DEFAULT ROLE 'admin';"
    sql "CREATE USER 'jack4'@('csding') IDENTIFIED BY '12345';"
    sql "CREATE USER 'jack5'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'admin';"
    sql "CREATE USER 'jack6' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
    sql "CREATE USER 'jack7' IDENTIFIED BY '12345' PASSWORD_HISTORY 8;"
    sql """CREATE USER 'jack8' COMMENT "this is my first user";"""

}
