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

// Checklist: A03 A06 C01 E02 G01 G05 H01 H02 H03.
suite("test_uuid_v7_time_functions", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "SET time_zone='UTC'"
    List<String> times = [null,'0000-01-01 00:00:00','1969-12-31 23:59:59.999999',
        '1970-01-01 00:00:00','1970-01-01 00:00:00.001999','2000-02-29 12:34:56.123456',
        '2026-09-10 12:34:56.789','9999-12-31 23:59:59.999999']
    List<Map> rows = times.collect { t -> [d: t == null ? 'CAST(NULL AS DATETIME(6))' : "CAST('${t}' AS DATETIME(6))"] }
    sql "DROP TABLE IF EXISTS uuid_v7_time_inputs"
    sql """CREATE TABLE uuid_v7_time_inputs (id INT,d DATETIME(6))
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_v7_time_inputs VALUES ${rows.withIndex().collect { r,i -> "(${i},${r.d})" }.join(',')}"
    for (String timezone : ['UTC','Asia/Shanghai','America/New_York','+14:00','-12:00']) {
        sql "SET time_zone='${timezone}'"
        matrix.run(delegate, "generate_${timezone.replaceAll('[^a-zA-Z0-9]','_')}", 'uuid_v7_time_inputs', ['d'], { d ->
            [version: "UUID_VERSION(dateTimeToUUIDv7(${d}))",
             restored: "UUIDv7ToDateTime(dateTimeToUUIDv7(${d}),'${timezone}')"]
        }, [rows:rows])
    }
    List<String> uuids = [null,'00000000000000000000000000000000',
        'ffffffffffffffffffffffffffffffff','550e8400e29b41d4a716446655440000',
        '00000000000170000000000000000000','018f0f5910107abc9234001122334455',
        'ffffffffffff70008000000000000000','e677d21fdbff70008000000000000000']
    List<Map> decodeRows = uuids.collect { u -> [u:u == null ? 'CAST(NULL AS UUID)' : "CAST('${u}' AS UUID)"] }
    sql "DROP TABLE IF EXISTS uuid_v7_decode_inputs"
    sql """CREATE TABLE uuid_v7_decode_inputs (id INT,u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_v7_decode_inputs VALUES ${decodeRows.withIndex().collect { r,i -> "(${i},${r.u})" }.join(',')}"
    for (String timezone : ['UTC','Asia/Shanghai','America/New_York','+14:00','-12:00']) {
        sql "SET time_zone='${timezone}'"
        matrix.run(delegate, "decode_${timezone.replaceAll('[^a-zA-Z0-9]','_')}", 'uuid_v7_decode_inputs', ['u'], { u ->
            [session_time: "UUIDv7ToDateTime(${u})", explicit_time: "UUIDv7ToDateTime(${u},'${timezone}')"]
        }, [rows:decodeRows])
    }
    sql "SET time_zone='UTC'"
    sql "DROP TABLE IF EXISTS uuid_v7_generated_values"
    sql """CREATE TABLE uuid_v7_generated_values (id INT,u UUID,v UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        sql "TRUNCATE TABLE uuid_v7_generated_values"
        sql """INSERT INTO uuid_v7_generated_values SELECT number,
            dateTimeToUUIDv7(CAST('2026-09-10 12:34:56.789' AS DATETIME(3))),
            datetime_to_uuid_v7(CAST('2026-09-10 12:34:56.789' AS DATETIME(3)))
            FROM numbers('number'='4097')"""
        qt_unique """SELECT COUNT(*),COUNT(DISTINCT u),COUNT(DISTINCT v),SUM(u=v),
            MIN(UUID_VERSION(u)),MAX(UUID_VERSION(v)),
            MIN(UUIDv7ToDateTime(u)),MAX(uuid_v7_to_datetime(v)),
            SUM(SUBSTRING(CAST(u AS STRING),20,1) IN ('8','9','a','b'))
            FROM uuid_v7_generated_values"""
        test {
            sql "SELECT UUIDv7ToDateTime(u,'Invalid/Timezone') FROM uuid_v7_decode_inputs"
            exception 'timezone'
        }
        qt_null_timezone "SELECT UUIDv7ToDateTime(CAST('00000000000170008000000000000000' AS UUID),NULL)"
    }
    test {
        sql "SELECT UUIDv7ToDateTime(u,CAST(id AS STRING)) FROM uuid_v7_decode_inputs"
        exception 'timezone must be constant'
    }
}
