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

suite("test_nereids_show_grants") {
    String dbName = "show_grants_db"
    sql "DROP USER IF EXISTS 'aaaaa'@'%';"
    sql "DROP USER IF EXISTS 'zzzzz'@'%';"
    sql "DROP USER IF EXISTS 'aaaaa'@'192.168.%';"

    sql "CREATE USER 'aaaaa'@'%' IDENTIFIED BY '12345';"
    sql "CREATE USER 'zzzzz'@'%' IDENTIFIED BY '12345';"
    sql "CREATE USER 'aaaaa'@'192.168.%' IDENTIFIED BY '12345';"

    checkNereidsExecute("show all grants")

    def res = sql """show all grants"""
    assertEquals("'aaaaa'@'%'", res.get(0).get(0))
    assertEquals("'aaaaa'@'192.168.%'", res.get(1).get(0))
    assertEquals("'zzzzz'@'%'", res.get(res.size() - 1).get(0))
}
