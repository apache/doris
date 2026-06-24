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

suite("test_stream_load_with_aes_encrypt", "p0") {
    def tableName = "test_stream_load_with_aes_encrypt"
    def dbName = context.dbName
    def encryptKeyName = "my_stream_load_key"
    def aesKey = "F3229A0B371ED2D9441B830D21A390C3"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
                id int,
                name string,
                encrypted_data string
            ) ENGINE=OLAP
            DUPLICATE KEY (`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
    """

    // Test 1: Stream load with AES_ENCRYPT using direct key string
    log.info("Test 1: Stream load with AES_ENCRYPT using direct key string")
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', """ id, name, tmp_data, encrypted_data=TO_BASE64(AES_ENCRYPT(tmp_data, '${aesKey}')) """
        file 'test_stream_load_aes_encrypt.csv'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(3, json.NumberLoadedRows)
        }
    }

    sql """sync"""

    // Verify data was loaded
    qt_sql_count """ SELECT count(*) FROM ${tableName} """

    // Verify encrypted data can be decrypted back to original using direct key
    qt_sql_decrypt_direct """
        SELECT id, name, AES_DECRYPT(FROM_BASE64(encrypted_data), '${aesKey}') as decrypted_data
        FROM ${tableName}
        ORDER BY id
    """

    // Clean table for next test
    sql """ TRUNCATE TABLE ${tableName} """

    // Test 2: Stream load with AES_ENCRYPT using ENCRYPTKEY (KEY syntax)
    log.info("Test 2: Stream load with AES_ENCRYPT using ENCRYPTKEY (KEY syntax)")

    // Create encryptkey with the same key value
    try_sql """ DROP ENCRYPTKEY IF EXISTS ${encryptKeyName} """
    sql """ CREATE ENCRYPTKEY ${encryptKeyName} AS "${aesKey}" """

    // Verify encryptkey was created
    def keyRes = sql """ SHOW ENCRYPTKEYS FROM ${dbName} """
    log.info("Encryptkeys: ${keyRes}")
    assertTrue(keyRes.size() >= 1, "Encryptkey should be created")

    // Stream load using KEY syntax to reference encryptkey
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', """ id, name, tmp_data, encrypted_data=TO_BASE64(AES_ENCRYPT(tmp_data, KEY ${dbName}.${encryptKeyName})) """
        file 'test_stream_load_aes_encrypt.csv'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result with ENCRYPTKEY: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(3, json.NumberLoadedRows)
        }
    }

    sql """sync"""

    // Verify data count
    qt_sql_count_key """ SELECT count(*) FROM ${tableName} """

    // Verify encrypted data can be decrypted using ENCRYPTKEY
    qt_sql_decrypt_with_key """
        SELECT id, name, AES_DECRYPT(FROM_BASE64(encrypted_data), KEY ${dbName}.${encryptKeyName}) as decrypted_data
        FROM ${tableName}
        ORDER BY id
    """

    // Cleanup
    sql """ DROP TABLE IF EXISTS ${tableName} """
    try_sql """ DROP ENCRYPTKEY IF EXISTS ${encryptKeyName} """
}
