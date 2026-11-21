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

import org.apache.doris.regression.util.ExportTestHelper

suite("test_uuid_orc_varbinary_mapping") {
    def storage = new ExportTestHelper(delegate)
    String prefix = "regression/test_uuid_orc_varbinary_mapping/${UUID.randomUUID()}/"
    String root = storage.uri(prefix)
    try {
        sql "DROP TABLE IF EXISTS uuid_orc_varbinary_mapping"
        sql """CREATE TABLE uuid_orc_varbinary_mapping (
                   id INT, u UUID, a ARRAY<UUID>, m MAP<UUID,UUID>, s STRUCT<k:UUID>, b STRING)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO uuid_orc_varbinary_mapping VALUES
               (1, '00112233445566778899aabbccddeeff',
                   ['00112233445566778899aabbccddeeff', NULL],
                   {'00000000000000000000000000000000':'80000000000000000000000000000000',
                    'ffffffffffffffffffffffffffffffff':NULL},
                   {'80000000000000000000000000000000'}, UNHEX('0000ff')),
               (2, '80000000000000000000000000000000', [], {}, {NULL},
                   UNHEX('')),
               (3, 'ffffffffffffffffffffffffffffffff', NULL, NULL, NULL,
                   UNHEX('ffffffffffffffffffffffffffffffff')),
               (4, '00000000000000000000000000000000', [NULL],
                   {'00112233445566778899aabbccddeeff':'00000000000000000000000000000000'},
                   {'00000000000000000000000000000000'}, NULL),
               (5, NULL, NULL, NULL, NULL, NULL)"""
        sql """SELECT id, u, a, m, s, CAST(b AS VARBINARY) AS b
               FROM uuid_orc_varbinary_mapping ORDER BY id
               INTO OUTFILE "${root}orc_" FORMAT AS ORC PROPERTIES (${storage.storageProperties()})"""
        for (boolean scannerV2 : [false, true]) {
            sql "SET enable_file_scanner_v2 = ${scannerV2}"
            for (boolean binaryMapping : [false, true]) {
                String source = """S3("uri"="${root}orc_*",
                        "format"="orc", ${storage.storageProperties()},
                        "enable_mapping_varbinary"="${binaryMapping}")"""
                "order_qt_schema_${scannerV2}_${binaryMapping}" "DESC FUNCTION ${source}"
                // UUID annotations override physical binary mapping, including nested leaves.
                String uuidValue = "u"
                String nestedValue = "value"
                String structValue = "s.k"
                "qt_values_${scannerV2}_${binaryMapping}" """SELECT id, ${uuidValue},
                        ARRAY_MAP(value -> ${nestedValue}, a),
                        ARRAY_MAP(value -> ${nestedValue}, MAP_KEYS(m)),
                        ARRAY_MAP(value -> ${nestedValue}, MAP_VALUES(m)),
                        ${structValue}, HEX(b), LENGTH(HEX(b))
                    FROM ${source} ORDER BY id"""
                "qt_filtered_${scannerV2}_${binaryMapping}" """SELECT id, ${uuidValue}, HEX(b), LENGTH(HEX(b))
                    FROM ${source} WHERE u IS NOT NULL AND id > 1 ORDER BY id"""
            }
        }
    } finally {
        storage.deleteFiles(prefix)
    }
}
