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

suite("test_alter_view") {
    String tableName = "test_alter_view_table";
    String viewName = "test_alter_view_view";
    sql " DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            c1 BIGINT NOT NULL,
            c2 BIGINT NOT NULL,
            c3 BIGINT NOT NULL
        )
        UNIQUE KEY (`c1`, `c2`)
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    sql """
        CREATE VIEW IF NOT EXISTS ${viewName} (k1, k2)
        AS
        SELECT c1 as k1, c2 as k2 FROM ${tableName}
        """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, 1, 10),
        (1, 5, 50),
        (2, 1, 20),
        (2, 10, 50)
        """

    qt_select " SELECT * FROM ${viewName} order by k1, k2 "

    sql """
        ALTER VIEW ${viewName} (k1, k2)
        AS
        SELECT c1 as k1, sum(c3) as k2 FROM ${tableName} GROUP BY c1
    """

    qt_select " SELECT * FROM ${viewName} order by k1, k2 "

    sql "DROP VIEW ${viewName}"
    sql "DROP TABLE ${tableName}"
}

