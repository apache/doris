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

// Checklist: B07 F06 F13.
suite("test_uuid_cluster_key", "p0") {
    sql "DROP TABLE IF EXISTS uuid_storage_cluster"
    sql """CREATE TABLE uuid_storage_cluster (id INT NOT NULL, u UUID, v INT)
           UNIQUE KEY(id) ORDER BY(u,id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='true')"""
    sql """INSERT INTO uuid_storage_cluster VALUES (1,'ffffffff-ffff-ffff-ffff-ffffffffffff',1),
           (2,'80000000-0000-0000-0000-000000000000',1), (3,NULL,1)"""
    sql "INSERT INTO uuid_storage_cluster VALUES (1,'00112233445566778899AABBCCDDEEFF',2)"
    qt_cluster "SELECT * FROM uuid_storage_cluster ORDER BY u NULLS FIRST,id"
    qt_cluster_predicate "SELECT id,v FROM uuid_storage_cluster WHERE u >= '80000000-0000-0000-0000-000000000000' ORDER BY id"
}
