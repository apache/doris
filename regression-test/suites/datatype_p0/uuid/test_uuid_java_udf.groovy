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

suite("test_uuid_java_udf", "p0") {
    String jarPath = "${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"
    // The local test cluster shares the worktree; remote CI backends need the normal UDF copy helper.
    if (context.config.otherConfigs.get("uuidSharedLocalFiles") != "true") {
        scp_udf_file_to_all_be(jarPath)
    }
    sql "DROP TABLE IF EXISTS uuid_java_paths"
    sql """CREATE TABLE uuid_java_paths (id INT, u UUID, a ARRAY<UUID>, m MAP<UUID,UUID>, s STRUCT<k:UUID>)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
           PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO uuid_java_paths VALUES
           (1, '00112233445566778899AABBCCDDEEFF', ['00112233445566778899AABBCCDDEEFF', NULL],
               {'00112233445566778899AABBCCDDEEFF':'ffffffffffffffffffffffffffffffff'},
               {'80000000-0000-0000-0000-000000000000'}),
           (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff', [], {}, {NULL}), (3, NULL, NULL, NULL, NULL)"""
    for (def entry : [scalar: ['UUID', 'Scalar'], array: ['ARRAY<UUID>', 'Array'],
                      map: ['MAP<UUID,UUID>', 'Map'], struct: ['STRUCT<k:UUID>', 'Struct']]) {
        String type = entry.value[0]
        sql "DROP FUNCTION IF EXISTS java_uuid_${entry.key}(${type})"
        sql """CREATE FUNCTION java_uuid_${entry.key}(${type}) RETURNS ${type}
               PROPERTIES("file"="file://${jarPath}", "symbol"="org.apache.doris.udf.UuidTest\$${entry.value[1]}",
                          "type"="JAVA_UDF")"""
    }
    sql "DROP FUNCTION IF EXISTS java_uuid_min(UUID)"
    sql """CREATE AGGREGATE FUNCTION java_uuid_min(UUID) RETURNS UUID
           PROPERTIES("file"="file://${jarPath}", "symbol"="org.apache.doris.udf.UuidMin", "type"="JAVA_UDF")"""
    qt_scalar "SELECT id, java_uuid_scalar(u) FROM uuid_java_paths ORDER BY id"
    qt_nested "SELECT id, java_uuid_array(a), java_uuid_map(m), java_uuid_struct(s) FROM uuid_java_paths ORDER BY id"
    qt_constant "SELECT java_uuid_scalar(CAST('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' AS UUID)), java_uuid_scalar(NULL)"
    qt_aggregate "SELECT java_uuid_min(u), MIN(u) FROM uuid_java_paths"
    qt_empty "SELECT java_uuid_min(u) FROM uuid_java_paths WHERE id < 0"
    qt_null "SELECT java_uuid_min(u) FROM uuid_java_paths WHERE u IS NULL"
    sql """INSERT INTO uuid_java_paths(id,u)
           SELECT number + 4, CAST(LPAD(HEX(number),32,'0') AS UUID) FROM numbers("number"="4097")"""
    qt_batches """SELECT COUNT(java_uuid_scalar(u)), MIN(java_uuid_scalar(u)), MAX(java_uuid_scalar(u)),
                         java_uuid_min(u), MIN(u) FROM uuid_java_paths"""
}
