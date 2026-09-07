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

// Checklist: B09 C05 C08.
suite("test_uuid_bucket_pruning", "p0") {

    sql "DROP TABLE IF EXISTS uuid_bucket_pruning"
    sql """CREATE TABLE uuid_bucket_pruning (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(u) BUCKETS 8
           PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_bucket_pruning VALUES
           (1,'00112233445566778899AABBCCDDEEFF'),
           (2,'00112233-4455-6677-8899-aabbccddeeff'),
           (3,'80000000-0000-0000-0000-000000000000'),(4,NULL)"""
    for (String value : ["CAST('00112233445566778899AABBCCDDEEFF' AS UUID)",
                          "CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID)",
                          "CAST(CONCAT('00112233-4455-6677-','8899-aabbccddeeff') AS UUID)"]) {
        explain {
            sql "verbose SELECT id,u FROM uuid_bucket_pruning WHERE u = ${value}"
            contains "tablets=1/8"
        }
        qt_canonical_bucket "SELECT id,u FROM uuid_bucket_pruning WHERE u = ${value} ORDER BY id"
    }
    explain {
        sql "verbose SELECT id FROM uuid_bucket_pruning WHERE u IS NULL"
        contains "tablets=1/8"
    }
    qt_null_bucket "SELECT id FROM uuid_bucket_pruning WHERE u IS NULL ORDER BY id"
    sql "DROP TABLE IF EXISTS uuid_bucket_composite"
    sql """CREATE TABLE uuid_bucket_composite (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(u,id) BUCKETS 8
           PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_bucket_composite SELECT * FROM uuid_bucket_pruning"
    explain {
        sql "verbose SELECT * FROM uuid_bucket_composite WHERE u = '00112233-4455-6677-8899-aabbccddeeff' AND id = 1"
        contains "tablets=1/8"
    }
    explain {
        sql "verbose SELECT * FROM uuid_bucket_composite WHERE u = '00112233-4455-6677-8899-aabbccddeeff'"
        contains "tablets=8/8"
    }
    qt_composite_full "SELECT * FROM uuid_bucket_composite WHERE u = '00112233-4455-6677-8899-aabbccddeeff' AND id = 1 ORDER BY id"
    qt_composite_partial "SELECT * FROM uuid_bucket_composite WHERE u = '00112233-4455-6677-8899-aabbccddeeff' ORDER BY id"

}
