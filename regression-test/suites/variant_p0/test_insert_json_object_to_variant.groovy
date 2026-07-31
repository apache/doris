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

suite("test_insert_json_object_to_variant", "variant_type") {
    sql """DROP TABLE IF EXISTS test_insert_json_object_to_variant_dst"""
    sql """DROP TABLE IF EXISTS test_insert_json_object_to_variant_src"""
    sql """DROP TABLE IF EXISTS test_insert_json_to_variant_dst"""
    sql """DROP TABLE IF EXISTS test_insert_json_to_variant_src"""

    sql """
        CREATE TABLE test_insert_json_object_to_variant_src (
            a bigint NOT NULL AUTO_INCREMENT(1),
            ch text NULL
        )
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """
        CREATE TABLE test_insert_json_object_to_variant_dst (
            a bigint NOT NULL AUTO_INCREMENT(1),
            ch variant NULL
        )
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """INSERT INTO test_insert_json_object_to_variant_src VALUES (17075, 'Willie "The Lion" Smith')"""
    sql """
        INSERT INTO test_insert_json_object_to_variant_dst
        SELECT a, json_object("ch", ch)
        FROM test_insert_json_object_to_variant_src
        WHERE a = 17075
    """

    order_qt_insert_json_object_to_variant """
        SELECT a, ch, CAST(ch['ch'] AS string)
        FROM test_insert_json_object_to_variant_dst
        ORDER BY a
    """

    sql """
        CREATE TABLE test_insert_json_to_variant_src (
            id int,
            j JSON
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """
        CREATE TABLE test_insert_json_to_variant_dst (
            id int,
            v VARIANT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql $/INSERT INTO test_insert_json_to_variant_src VALUES
        (1, '{"ok":"abc","msg":"he said \\\"hi\\\"","path":"C:\\\\tmp","nested":{"x":1},"arr":[1,2]}')/$

    sql """
        INSERT INTO test_insert_json_to_variant_dst
        SELECT id, j
        FROM test_insert_json_to_variant_src
        WHERE id = 1
    """

    order_qt_insert_json_to_variant """
        SELECT id,
               CAST(v['ok'] AS string),
               CAST(v['msg'] AS string),
               HEX(CAST(v['path'] AS string)),
               CAST(v['nested']['x'] AS int),
               size(CAST(v['arr'] AS array<int>))
        FROM test_insert_json_to_variant_dst
        ORDER BY id
    """
}
