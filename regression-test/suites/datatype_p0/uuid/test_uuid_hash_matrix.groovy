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

suite("test_uuid_hash_matrix", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "DROP TABLE IF EXISTS uuid_matrix_hash"
    sql """CREATE TABLE uuid_matrix_hash (${matrix.schema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_hash VALUES ${matrix.values()}"

    // These signatures coerce UUID to its canonical string representation in FE.
    matrix.run(delegate, 'unary', 'uuid_matrix_hash', ['u'], { u ->
        [murmur32: "MURMUR_HASH3_32(${u})", murmur64: "MURMUR_HASH3_64(${u})",
         xx32: "XXHASH_32(${u})", xx64: "XXHASH_64(${u})", crc_value: "CRC32(${u})",
         md5_value: "MD5(${u})", length_value: "LENGTH(${u})", hex_value: "HEX(${u})", valid_text: "IS_UUID(${u})", as_int: "UUID_TO_INT(${u})",
         int_roundtrip: "CAST(INT_TO_UUID(UUID_TO_INT(${u})) AS UUID)"]
    })
    matrix.run(delegate, 'binary', 'uuid_matrix_hash', ['u','v'], { u,v ->
        [murmur32: "MURMUR_HASH3_32(${u},${v})", murmur64: "MURMUR_HASH3_64(${u},${v})",
         xx32: "XXHASH_32(${u},${v})", xx64: "XXHASH_64(${u},${v})"]
    })
}
