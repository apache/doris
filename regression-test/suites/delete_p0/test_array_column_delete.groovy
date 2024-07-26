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

suite("test_array_column_delete") {
    def tableName = "test_array_column_delete"

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (id INT NULL, c_array ARRAY<INT> NULL) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ( "replication_allocation" = "tag.location.default: 1","in_memory" = "false","storage_format" = "V2") """
    sql """ insert into ${tableName} values(1, NULL),(2,[12,3]),(3,[]),(4,NULL),(5,NULL) """
    test {
        sql """ DELETE FROM ${tableName} WHERE c_array is NULL """
        exception("Can not apply delete condition to column type: array<int>")
    }
    sql """ DELETE FROM ${tableName} WHERE id = 1; """
    qt_sql """ SELECT * FROM ${tableName} order by id """
}
