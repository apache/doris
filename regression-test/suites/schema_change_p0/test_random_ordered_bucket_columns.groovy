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

suite('test_random_ordered_bucket_columns') {
    def tblName = "test_random_ordered_bucket_columns"
    sql """ DROP TABLE IF EXISTS ${tblName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tblName}
        (
            k1 DATE,
            k2 INT DEFAULT '10',
            k3 bigint,
            k4 VARCHAR(32) DEFAULT '',
            v1 BIGINT DEFAULT '0'
        )
        UNIQUE  KEY(k1, k2, k3, k4)
        DISTRIBUTED BY HASH(k3, k4, k1, k2) BUCKETS 5
        PROPERTIES("replication_num" = "1", "light_schema_change" = "true"); 
    """

    // show create table differs in local and cloud mode, this behavior is verified
    // qt_sql """ SHOW CREATE TABLE ${tblName} """

    sql """ INSERT INTO ${tblName} VALUES("2025-07-29", 0, 1, "2", 3) """

    sql """ INSERT INTO ${tblName} VALUES("2025-07-29", 0, 1, "2", 3) """

    sql """ SYNC """

    qt_sql """ SELECT * FROM ${tblName} """

    sql """ ALTER TABLE ${tblName} ADD COLUMN v2 BIGINT DEFAULT '1' """


    // sql """ SYNC """

    // show create table differs in local and cloud mode, this behavior is verified
    // qt_sql """ SHOW CREATE TABLE ${tblName} """

    30.times { index -> 
        sql """ INSERT INTO ${tblName} VALUES("2025-07-29", 0, 1, "2", 3, 0) """
    }

    sql """ SYNC """

    qt_sql """ SELECT * FROM ${tblName} """
}
