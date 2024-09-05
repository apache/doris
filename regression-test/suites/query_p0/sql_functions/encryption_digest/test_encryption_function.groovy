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
    test {
        sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));"
        exception "session variable block_encryption_mode is invalid with sm4"
    }

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

    sql "set block_encryption_mode=\"SM4_128_CBC\";"
    qt_sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));" // aDjwRflBrDjhBZIOFNw3Tg==
    qt_sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));" // 1Y4NGIukSbv9OrkZnRD1bQ==
    qt_sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789ff'));" // G5POcFAJwiZHeTtN6DjInQ==
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('aDjwRflBrDjhBZIOFNw3Tg=='),'F3229A0B371ED2D9441B830D21A390C3');" // text
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('1Y4NGIukSbv9OrkZnRD1bQ=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');" // text
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('G5POcFAJwiZHeTtN6DjInQ=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');" // NULL
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('G5POcFAJwiZHeTtN6DjInQ=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789ff');" // text
    test {
        sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));"
        exception "session variable block_encryption_mode is invalid with aes"
    }

    qt_sql "SELECT SM3(\"abc\");"
    qt_sql "select sm3(\"abcd\");"
    qt_sql "select sm3sum(\"ab\",\"cd\");"
    sql "DROP TABLE IF EXISTS quantile_table"
    sql"""
        CREATE TABLE quantile_table
        (
            id int,
            k string
        )
        ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1"
        );
    """
    sql""" insert into quantile_table values(1,"aaaaaa");"""
    qt_sql """ select sm4_decrypt(sm4_encrypt(k,"doris","0123456789abcdef"),"doris","0123456789abcdef") from quantile_table; """
}
