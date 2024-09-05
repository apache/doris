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

suite("test_encryption_function") {
    sql "DROP TABLE IF EXISTS dwd_candidates"
    sql """
        CREATE TABLE IF NOT EXISTS dwd_candidates (
          c_int INT,
          `name` varchar(65530) NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """

    sql "set block_encryption_mode=\"AES_128_ECB\";"
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));" // 'wr2JEDVXzL9+2XtRhgIloA=='
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));" // wr2JEDVXzL9+2XtRhgIloA==
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');" // text
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('mvZT1KJw7N0RJf27aipUpg=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');" // NULL

    sql "set block_encryption_mode=\"AES_256_ECB\";"
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));" // 'wr2JEDVXzL9+2XtRhgIloA=='
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));" // 'BO2vxHeUcw5BQQalSBbo1w=='
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');" // text
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('BO2vxHeUcw5BQQalSBbo1w=='),'F3229A0B371ED2D9441B830D21A390C3');" // NULL
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('BO2vxHeUcw5BQQalSBbo1w=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');" // text

    sql "set block_encryption_mode=\"AES_256_CBC\";"
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));" // 'wr2JEDVXzL9+2XtRhgIloA=='
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));" // tsmK1HzbpnEdR2//WhO+MA==
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789ff'));" // ciacXDLHMNG7CD9Kws8png==
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');" // text
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('mvZT1KJw7N0RJf27aipUpg=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');" // NULL
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('tsmK1HzbpnEdR2//WhO+MA=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');" // text
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('ciacXDLHMNG7CD9Kws8png=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789ff');" // text
    explain {
        sql "SELECT AES_DECRYPT(UNHEX(r_2_3.`name`), 'namePnhe3E0MWyfZivUnVzDy12caymnrKp', '0123456789') AS x0 FROM dwd_candidates AS r_2_3\n" +
                "GROUP BY x0;"
    }

    qt_sql "SELECT SM3(\"abc\");"
    qt_sql "select sm3(\"abcd\");"
    qt_sql "select sm3sum(\"ab\",\"cd\");"

    qt_sql_gcm_1 "SELECT TO_BASE64(AES_ENCRYPT('Spark SQL', '1234567890abcdef', '123456789012', 'aes_128_gcm', 'Some AAD'))"
    qt_sql_gcm_2 "SELECT AES_DECRYPT(FROM_BASE64('MTIzNDU2Nzg5MDEyMdXvR41sJqwZ6hnTU8FRTTtXbL8yeChIZA=='), '1234567890abcdef', '', 'aes_128_gcm', 'Some AAD')"

    qt_sql_gcm_3 "select to_base64(aes_encrypt('Spark','abcdefghijklmnop12345678ABCDEFGH',unhex('000000000000000000000000'),'aes_256_gcm', 'This is an AAD mixed into the input'));"
    qt_sql_gcm_4 "SELECT AES_DECRYPT(FROM_BASE64('AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4'), 'abcdefghijklmnop12345678ABCDEFGH', '', 'aes_256_gcm', 'This is an AAD mixed into the input');"

    sql "DROP TABLE IF EXISTS aes_encrypt_decrypt_tbl"
    sql """
        CREATE TABLE IF NOT EXISTS aes_encrypt_decrypt_tbl (
          id int,
          plain_txt varchar(255),
          enc_txt varchar(255),
          k varchar(255),
          iv varchar(255),
          mode varchar(255),
          aad varchar(255)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """
    sql """ insert into aes_encrypt_decrypt_tbl values(1,'Spark SQL','MTIzNDU2Nzg5MDEyMdXvR41sJqwZ6hnTU8FRTTtXbL8yeChIZA==','1234567890abcdef','123456789012','aes_128_gcm','Some AAD');"""
    sql """ insert into aes_encrypt_decrypt_tbl values(2,'Spark','AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4','abcdefghijklmnop12345678ABCDEFGH',unhex('000000000000000000000000'),'aes_256_gcm','This is an AAD mixed into the input');"""
    sql """ sync """

    qt_sql_gcm_5 "SELECT id,TO_BASE64(AES_ENCRYPT(plain_txt,k,iv,mode,aad)) from aes_encrypt_decrypt_tbl order by id;"
    qt_sql_gcm_6 "SELECT id,AES_DECRYPT(FROM_BASE64(enc_txt),k,'',mode,aad) from aes_encrypt_decrypt_tbl order by id;"

    // test for const opt branch, only first column is not const
    qt_sql_gcm_7 "SELECT id,TO_BASE64(AES_ENCRYPT(plain_txt, '1234567890abcdef', '123456789012', 'aes_128_gcm', 'Some AAD')) from aes_encrypt_decrypt_tbl where id=1"
    qt_sql_gcm_8 "SELECT AES_DECRYPT(FROM_BASE64(enc_txt), '1234567890abcdef', '', 'aes_128_gcm', 'Some AAD') from aes_encrypt_decrypt_tbl where id=1"
}
