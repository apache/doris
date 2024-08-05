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

suite("test_function_conjunct") {
    def tableName = "test_function_conjunct_table"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                a varchar(10) not null, 
                b int not null
            ) ENGINE=OLAP
            DUPLICATE KEY(a)
            DISTRIBUTED BY HASH(a) BUCKETS 2
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO ${tableName} VALUES ("1","1"); """

    qt_select_default """select a, b from ${tableName} where coalesce(b, null) is NULL;"""

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
