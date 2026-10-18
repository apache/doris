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


suite("test_hive_uuid_binary", "p0,external,hive,external_docker,external_docker_hive") {
    if (!context.config.otherConfigs.get("enableHiveTest")?.equalsIgnoreCase("true")) {
        return
    }
    setHivePrefix("hive2")
    String host = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")
    String root = "hdfs://${host}:${hdfsPort}/tmp/test_hive_uuid_binary_${UUID.randomUUID()}"
    hive_docker "CREATE DATABASE IF NOT EXISTS test_hive_uuid_binary"
    sql "DROP CATALOG IF EXISTS hive_uuid_binary"
    sql """CREATE CATALOG hive_uuid_binary PROPERTIES (
        "type"="hms", "hive.metastore.uris"="thrift://${host}:${hmsPort}",
        "hadoop.username"="doris", "enable.mapping.varbinary"="true")"""
    sql "DROP TABLE IF EXISTS hive_uuid_binary_source"
    sql """CREATE TABLE hive_uuid_binary_source (
        id INT, u UUID, a ARRAY<UUID>, m MAP<UUID,UUID>, s STRUCT<k:UUID>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO hive_uuid_binary_source VALUES
        (1, '00112233445566778899aabbccddeeff',
            ['00112233445566778899aabbccddeeff', NULL],
            {'00000000000000000000000000000000':'80000000000000000000000000000000'},
            {'80000000000000000000000000000000'}),
        (2, '80000000000000000000000000000000', [], {}, {NULL}),
        (3, 'ffffffffffffffffffffffffffffffff', NULL, NULL, NULL),
        (4, '00000000000000000000000000000000', [NULL],
            {'ffffffffffffffffffffffffffffffff':NULL}, {'00000000000000000000000000000000'}),
        (5, NULL, NULL, NULL, NULL)"""
    for (String format : ["parquet", "orc"]) {
        sql """SELECT * FROM hive_uuid_binary_source ORDER BY id
            INTO OUTFILE "${root}/${format}/part_" FORMAT AS ${format}
            PROPERTIES("fs.defaultFS"="hdfs://${host}:${hdfsPort}", "hadoop.username"="doris")"""
        hive_docker "DROP TABLE IF EXISTS test_hive_uuid_binary.uuid_${format}"
        // The file carries UUID annotations, but Hive's declared BINARY contract is the raw bytes.
        hive_docker """CREATE EXTERNAL TABLE test_hive_uuid_binary.uuid_${format} (
            id INT, u BINARY, a ARRAY<BINARY>, m MAP<BINARY,BINARY>, s STRUCT<k:BINARY>)
            STORED AS ${format} LOCATION '${root}/${format}'"""
        sql "REFRESH CATALOG hive_uuid_binary"
        for (boolean scannerV2 : [false, true]) {
            sql "SET enable_file_scanner_v2 = ${scannerV2}"
            for (boolean strict : [false, true]) {
                sql "SET enable_strict_cast = ${strict}"
                String table = "hive_uuid_binary.test_hive_uuid_binary.uuid_${format}"
                "qt_values_${format}_${scannerV2}_${strict}" """SELECT id, HEX(u), LENGTH(u),
                    ARRAY_MAP(x -> HEX(x), a), ARRAY_MAP(x -> HEX(x), MAP_KEYS(m)),
                    ARRAY_MAP(x -> HEX(x), MAP_VALUES(m)), HEX(s.k)
                    FROM ${table} ORDER BY id"""
                "qt_filtered_${format}_${scannerV2}_${strict}" """SELECT id, HEX(u), HEX(a[1]), HEX(s.k)
                    FROM ${table} WHERE HEX(u) = '00112233445566778899AABBCCDDEEFF'
                    ORDER BY id"""
            }
        }
    }
}
