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

// Checklist: A03 A06 C05 E02.
suite("test_uuid_is_uuid", "p0") {
    sql "SET enable_sql_cache = false"

    sql "DROP TABLE IF EXISTS test_uuid_is_uuid"
    sql """CREATE TABLE test_uuid_is_uuid (
               id INT,
               v VARCHAR(64) NOT NULL
           ) DUPLICATE KEY(id)
           DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO test_uuid_is_uuid VALUES
               (1, '000000000000000000000000000000000000'),
               (2, '00000000-0000-0000-0000-000000000000'),
               (3, '00000000000000000000000000000000'),
               (4, '{00000000-0000-0000-0000-000000000000}'),
               (5, '{000000000000000000000000000000000000}'),
               (6, 'abcdefabcdefabcdefabcdefabcdefabcdef'),
               (7, '6ccd780c-baba-1026-9564-5b8c656024db'),
               (8, '6ccd780cbaba102695645b8c656024db'),
               (9, '00000000-0000-0000-00000000000000000'),
               (10, '0000000-00000-0000-0000-000000000000'),
               (11, '00112233-4455-6677-8899-aabbccddeeff'),
               (12, '00112233445566778899aabbccddeefg')"""

    // Column path: a 36 characters long value must keep the fixed dashes of the
    // 8-4-4-4-12 layout, hex digits are not accepted at the separator positions.
    order_qt_column_path "SELECT id, is_uuid(v) AS r FROM test_uuid_is_uuid ORDER BY id"

    // Literal path, evaluated on FE (folded) and on BE (folding disabled). Both must
    // agree with the column path above.
    for (boolean skip : [false, true]) {
        sql "SET debug_skip_fold_constant = ${skip}"
        order_qt_literal_path """SELECT is_uuid('000000000000000000000000000000000000'),
                                        is_uuid('abcdefabcdefabcdefabcdefabcdefabcdef'),
                                        is_uuid('00000000-0000-0000-0000-000000000000'),
                                        is_uuid('00000000000000000000000000000000'),
                                        is_uuid('{00000000-0000-0000-0000-000000000000}'),
                                        is_uuid('{000000000000000000000000000000000000}'),
                                        is_uuid('00000000-0000-0000-00000000000000000')"""
    }
    sql "SET debug_skip_fold_constant = false"
}
