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

    // sm4_encrypt sm4_decrypt
    // aes_encrypt aes_decrypt
    //two arg (column/const)
    sql "set enable_fold_constant_by_be = false;"
    sql """ set block_encryption_mode=""; """ // SM4_128_ECB
    qt_sql1 """ select sm4_decrypt(sm4_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql2 """ select sm4_decrypt(sm4_encrypt(k,k),k) from quantile_table; """
    qt_sql3 """ select sm4_decrypt(sm4_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql4 """ select sm4_decrypt(sm4_encrypt("zhang",k),k) from quantile_table; """

    sql """ set block_encryption_mode="SM4_128_CBC"; """
    qt_sql5 """ select sm4_decrypt(sm4_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql6 """ select sm4_decrypt(sm4_encrypt(k,k),k) from quantile_table; """
    qt_sql7 """ select sm4_decrypt(sm4_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql8 """ select sm4_decrypt(sm4_encrypt("zhang",k),k) from quantile_table; """

    sql """ set block_encryption_mode="SM4_128_OFB"; """
    qt_sql9 """ select sm4_decrypt(sm4_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql10 """ select sm4_decrypt(sm4_encrypt(k,k),k) from quantile_table; """
    qt_sql11 """ select sm4_decrypt(sm4_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql12 """ select sm4_decrypt(sm4_encrypt("zhang",k),k) from quantile_table; """

    sql """ set block_encryption_mode="SM4_128_CTR"; """
    qt_sql9 """ select sm4_decrypt(sm4_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql10 """ select sm4_decrypt(sm4_encrypt(k,k),k) from quantile_table; """
    qt_sql11 """ select sm4_decrypt(sm4_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql12 """ select sm4_decrypt(sm4_encrypt("zhang",k),k) from quantile_table; """

    sql """ set block_encryption_mode=""; """ // AES_128_ECB
    qt_sql13 """ select aes_decrypt(aes_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql14 """ select aes_decrypt(aes_encrypt(k,k),k) from quantile_table; """
    qt_sql15 """ select aes_decrypt(aes_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql16 """ select aes_decrypt(aes_encrypt("zhang",k),k) from quantile_table; """

    sql """ set block_encryption_mode="AES_256_CBC"; """
    qt_sql17 """ select aes_decrypt(aes_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql18 """ select aes_decrypt(aes_encrypt(k,k),k) from quantile_table; """
    qt_sql19 """ select aes_decrypt(aes_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql20 """ select aes_decrypt(aes_encrypt("zhang",k),k) from quantile_table; """


    sql """ set block_encryption_mode="AES_128_CTR"; """
    qt_sql21 """ select aes_decrypt(aes_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql22 """ select aes_decrypt(aes_encrypt(k,k),k) from quantile_table; """
    qt_sql23 """ select aes_decrypt(aes_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql24 """ select aes_decrypt(aes_encrypt("zhang",k),k) from quantile_table; """


    sql """ set block_encryption_mode="AES_256_OFB"; """
    qt_sql25 """ select aes_decrypt(aes_encrypt(k,"doris"),"doris") from quantile_table; """
    qt_sql26 """ select aes_decrypt(aes_encrypt(k,k),k) from quantile_table; """
    qt_sql27 """ select aes_decrypt(aes_encrypt("zhang","doris"),"doris") from quantile_table; """
    qt_sql28 """ select aes_decrypt(aes_encrypt("zhang",k),k) from quantile_table; """

    sql """ set block_encryption_mode=""; """

    sql """ select to_base64(aes_encrypt(k,"doris")) from quantile_table;""" // 3A7GoWeuMNEBWzJx+YefZw==
    qt_sql29 """ select aes_decrypt(FROM_BASE64("3A7GoWeuMNEBWzJx+YefZw=="),"doris") from quantile_table; """

    sql """ select to_base64(aes_encrypt(k,k)) from quantile_table;""" //ADnRqPtFBjreZu06UTD64g==
    qt_sql30 """ select aes_decrypt(FROM_BASE64("ADnRqPtFBjreZu06UTD64g=="),k) from quantile_table; """

    sql """ select to_base64(aes_encrypt("zhang","doris")) from quantile_table;""" //fLhlYvn/yZhqd2LTRHImrw==
    qt_sql31 """ select aes_decrypt(FROM_BASE64("fLhlYvn/yZhqd2LTRHImrw=="),"doris") from quantile_table; """

    sql """ select to_base64(aes_encrypt("zhang",k)) from quantile_table;""" //2C8acACKfoRwHZS5B4juNw==
    qt_sql32 """ select aes_decrypt(FROM_BASE64("2C8acACKfoRwHZS5B4juNw=="),k) from quantile_table; """



    sql """ select to_base64(sm4_encrypt(k,"doris")) from quantile_table;""" // 7vSaqYqMl9no8trrzbdAEw==
    qt_sql29 """ select sm4_decrypt(FROM_BASE64("7vSaqYqMl9no8trrzbdAEw=="),"doris") from quantile_table; """

    sql """ select to_base64(sm4_encrypt(k,k)) from quantile_table;""" // PcPR18T6lhMuFTqQtymb8w==
    qt_sql30 """ select sm4_decrypt(FROM_BASE64("PcPR18T6lhMuFTqQtymb8w=="),k) from quantile_table; """

    sql """ select to_base64(sm4_encrypt("zhang","doris")) from quantile_table;""" // WY+4o1/cZwAFQ0F6dlyEqQ==
    qt_sql31 """ select sm4_decrypt(FROM_BASE64("WY+4o1/cZwAFQ0F6dlyEqQ=="),"doris") from quantile_table; """

    sql """ select to_base64(sm4_encrypt("zhang",k)) from quantile_table;""" // lhDiiEnRn3PvY6v4sHES0A==
    qt_sql32 """ select sm4_decrypt(FROM_BASE64("lhDiiEnRn3PvY6v4sHES0A=="),k) from quantile_table; """


    sql "DROP TABLE IF EXISTS quantile_table2"
    sql"""
        CREATE TABLE quantile_table2
        (
            id int,
            k string,
            k1 string,
            k2 string
        )
        ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1"
        );
    """
    sql""" insert into quantile_table2 values(1,"aaaaaa", "key_word", "init_word");"""

    //four arg (column/const)
    sql """ set block_encryption_mode=""; """ // SM4_128_ECB
    qt_sql33 """ select sm4_decrypt(sm4_encrypt(k,"doris","abcdefghij", "SM4_128_CBC"),"doris","abcdefghij","SM4_128_CBC") from quantile_table2; """
    qt_sql34 """ select sm4_decrypt(sm4_encrypt(k,k,"abcdefghij", "SM4_128_CBC"),k,"abcdefghij", "SM4_128_CBC") from quantile_table2; """
    qt_sql35 """ select sm4_decrypt(sm4_encrypt("zhang","doris","abcdefghij", "SM4_128_CBC"),"doris","abcdefghij", "SM4_128_CBC") from quantile_table2; """
    qt_sql36 """ select sm4_decrypt(sm4_encrypt("zhang",k,"abcdefghij", "SM4_128_CBC"),k,"abcdefghij", "SM4_128_CBC") from quantile_table2; """
    
    qt_sql37 """ select sm4_decrypt(sm4_encrypt(k,"doris",k2, "SM4_128_CBC"),"doris",k2,"SM4_128_CBC") from quantile_table2; """
    qt_sql38 """ select sm4_decrypt(sm4_encrypt(k,k,k2, "SM4_128_CBC"),k,k2, "SM4_128_CBC") from quantile_table2; """
    qt_sql39 """ select sm4_decrypt(sm4_encrypt("zhang","doris",k2, "SM4_128_CBC"),"doris",k2, "SM4_128_CBC") from quantile_table2; """
    qt_sql40 """ select sm4_decrypt(sm4_encrypt("zhang",k,k2, "SM4_128_CBC"),k,k2, "SM4_128_CBC") from quantile_table2; """
    
    qt_sql41 """ select sm4_decrypt(sm4_encrypt(k,k1,k2, "SM4_128_CBC"),k1,k2,"SM4_128_CBC") from quantile_table2; """
    qt_sql42 """ select sm4_decrypt(sm4_encrypt(k,k1,k2, "SM4_128_CBC"),k1,k2, "SM4_128_CBC") from quantile_table2; """
    qt_sql43 """ select sm4_decrypt(sm4_encrypt("zhang",k1,k2, "SM4_128_CBC"),k1,k2, "SM4_128_CBC") from quantile_table2; """
    qt_sql44 """ select sm4_decrypt(sm4_encrypt("zhang",k1,k2, "SM4_128_CBC"),k1,k2, "SM4_128_CBC") from quantile_table2; """
    

    qt_sql45 """ select aes_decrypt(aes_encrypt(k,"doris","abcdefghij", "AES_256_CFB"),"doris","abcdefghij","AES_256_CFB") from quantile_table2; """
    qt_sql46 """ select aes_decrypt(aes_encrypt(k,k,"abcdefghij", "AES_256_CFB"),k,"abcdefghij", "AES_256_CFB") from quantile_table2; """
    qt_sql47 """ select aes_decrypt(aes_encrypt("zhang","doris","abcdefghij", "AES_256_CFB"),"doris","abcdefghij", "AES_256_CFB") from quantile_table2; """
    qt_sql48 """ select aes_decrypt(aes_encrypt("zhang",k,"abcdefghij", "AES_256_CFB"),k,"abcdefghij", "AES_256_CFB") from quantile_table2; """
    
    qt_sql49 """ select aes_decrypt(aes_encrypt(k,"doris",k2, "AES_256_CFB"),"doris",k2,"AES_256_CFB") from quantile_table2; """
    qt_sql50 """ select aes_decrypt(aes_encrypt(k,k,k2, "AES_256_CFB"),k,k2, "AES_256_CFB") from quantile_table2; """
    qt_sql51 """ select aes_decrypt(aes_encrypt("zhang","doris",k2, "AES_256_CFB"),"doris",k2, "AES_256_CFB") from quantile_table2; """
    qt_sql52 """ select aes_decrypt(aes_encrypt("zhang",k,k2, "AES_256_CFB"),k,k2, "AES_256_CFB") from quantile_table2; """
    
    qt_sql53 """ select aes_decrypt(aes_encrypt(k,k1,k2, "AES_256_CFB"),k1,k2,"AES_256_CFB") from quantile_table2; """
    qt_sql54 """ select aes_decrypt(aes_encrypt(k,k1,k2, "AES_256_CFB"),k1,k2, "AES_256_CFB") from quantile_table2; """
    qt_sql55 """ select aes_decrypt(aes_encrypt("zhang",k1,k2, "AES_256_CFB"),k1,k2, "AES_256_CFB") from quantile_table2; """
    qt_sql56 """ select aes_decrypt(aes_encrypt("zhang",k1,k2, "AES_256_CFB"),k1,k2, "AES_256_CFB") from quantile_table2; """

    //four arg (column/const) with wrong mode
    qt_sql57 """ select sm4_decrypt(sm4_encrypt(k,"doris","abcdefghij", "SM4_128_CBC"),"doris","abcdefghij","SM4_555_CBC") from quantile_table2; """
}
