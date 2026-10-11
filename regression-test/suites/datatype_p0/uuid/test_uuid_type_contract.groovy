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

suite("test_uuid_type_contract", "p0") {
    for (def invalid : [
        ["u UUID NOT NULL AUTO_INCREMENT", "", "auto increment must be BIGINT"],
        ["u UUID, INDEX ngram_u(u) USING NGRAM_BF PROPERTIES('gram_size'='3','bf_size'='256')",
         "", "not supported in ngram_bf"],
        ["u UUID NOT NULL, INDEX ann_u(u) USING ANN PROPERTIES('index_type'='hnsw','metric_type'='l2_distance','dim'='16')",
         "", "must be array type"],
        ["u UUID", ", 'function_column.sequence_type'='UUID'", "sequence type only support integer types and date types"]
    ]) {
        test {
            sql """CREATE TABLE uuid_invalid_contract (id INT, ${invalid[0]})
                   UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES('replication_num'='1' ${invalid[1]})"""
            exception invalid[2]
        }
    }
    test {
        sql """CREATE TABLE uuid_invalid_contract (id INT, u UUID SUM)
               AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
        exception "SUM is not compatible with primitive type UUID"
    }
    for (String target : ["BIGINT", "DOUBLE", "DATE", "IPV6"]) {
        test {
            sql "SELECT CAST(CAST('00112233445566778899AABBCCDDEEFF' AS UUID) AS ${target})"
            exception "cannot cast"
        }
    }
    sql "DROP TABLE IF EXISTS uuid_contract_paths"
    sql """CREATE TABLE uuid_contract_paths (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_contract_paths VALUES (1,'00112233445566778899AABBCCDDEEFF'), (2,NULL)"
    test {
        sql "ALTER TABLE uuid_contract_paths MODIFY COLUMN u BIGINT"
        exception "Can not change UUID to BIGINT"
    }
    for (String timezone : ["+00:00", "+08:00"]) {
        sql "SET time_zone = '${timezone}'"
        qt_timezone "SELECT * FROM uuid_contract_paths ORDER BY id"
    }
    qt_map_keys """SELECT MAP(CAST('00112233445566778899AABBCCDDEEFF' AS UUID), 1,
                             CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID), 2),
                         MAP(CAST(NULL AS UUID), 1)"""
    qt_struct_cast """SELECT CAST('{"k":null}' AS STRUCT<k:UUID>),
                            CAST('{}' AS STRUCT<k:UUID>), CAST(NULL AS ARRAY<UUID>)"""
    String deepType = "ARRAY<" * 10 + "UUID" + ">" * 10
    test {
        sql "CREATE TABLE uuid_invalid_contract (id INT,u ${deepType}) DISTRIBUTED BY HASH(id) PROPERTIES('replication_num'='1')"
        exception "maximum nesting depth"
    }
}
