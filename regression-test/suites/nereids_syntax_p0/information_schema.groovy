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

suite("information_schema") {
    List<List<Object>> table =  sql """ select * from backends(); """
    assertTrue(table.size() > 0)
    assertTrue(table[0].size == 25)

    sql "SELECT DATABASE();"
    sql "select USER();"
    sql "SELECT CONNECTION_ID();"
    sql "SELECT CURRENT_USER();"

    // INFORMATION_SCHEMA
    sql "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema=\"test%\" and TABLE_TYPE = \"BASE TABLE\" order by table_name"
    sql "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \"test%\" AND column_name LIKE \"k%\""
}
