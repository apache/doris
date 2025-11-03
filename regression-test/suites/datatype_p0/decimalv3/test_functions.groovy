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

suite("test_functions") {
    def dbName = "test_functions"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"

    def tableName = "test_decimal_load"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
              id int(11) NULL,
              s_count int(11) NULL,
              fee DECIMALV3(15,4) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

        sql """ INSERT INTO ${tableName} VALUES(10007,26,13.3), (10001,12,13.3) """
        qt_select_default """ SELECT id, if(1 = 2,
                                     1.0*s_count - fee,
                                     fee) FROM ${tableName} t ORDER BY id; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
