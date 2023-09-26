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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_ip_stream_load") {
    def dbName = "test_stream_load"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"

    def tableName = "test_ip_stream_load"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
    CREATE TABLE ${tableName} (
      `id` int,
      `ip_v4` ipv4,
      `ip_v6` ipv6
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    streamLoad {
        db dbName
        table tableName

        set 'column_separator', ','

        file 'test_data/test.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
    }

    sql """sync"""
    qt_select """ SELECT * FROM ${tableName} ORDER BY id; """

    try_sql("DROP TABLE IF EXISTS ${tableName}")
}
