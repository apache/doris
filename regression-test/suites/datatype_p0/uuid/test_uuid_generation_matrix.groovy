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

suite("test_uuid_generation_matrix", "p0") {

    sql "DROP TABLE IF EXISTS uuid_matrix_generation"
    sql """CREATE TABLE uuid_matrix_generation (id INT,u UUID,v UUID,w UUID,x UUID,y UUID,z UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        sql "TRUNCATE TABLE uuid_matrix_generation"
        sql """INSERT INTO uuid_matrix_generation SELECT number,UUID_V4(),UUID_V7(),
               GENERATEUUIDV4(),GENERATEUUIDV7(),GENERATE_UUID_V4(),GENERATE_UUID_V7()
               FROM numbers('number'='4097')"""
        for (String column : ['u','v','w','x','y','z']) {
            qt_generated """SELECT COUNT(*),COUNT(${column}),COUNT(DISTINCT ${column}),
                MIN(UUID_VERSION(${column})),MAX(UUID_VERSION(${column})),
                MIN(LENGTH(CAST(${column} AS STRING))),MAX(LENGTH(CAST(${column} AS STRING))),
                SUM(SUBSTRING(CAST(${column} AS STRING),20,1) IN ('8','9','a','b'))
                FROM uuid_matrix_generation"""
        }
    }
}
