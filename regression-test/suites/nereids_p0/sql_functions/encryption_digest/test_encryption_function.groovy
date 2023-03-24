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
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "set batch_size = 4096;"

    sql "set block_encryption_mode=\"AES_256_CBC\";"
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));"
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));"
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');"
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('mvZT1KJw7N0RJf27aipUpg=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');"

    qt_sql "SELECT MD5(\"abc\");"

    qt_sql "SELECT MD5(\"abcd\");"
    qt_sql "SELECT MD5SUM(\"ab\",\"cd\");"

    sql "set block_encryption_mode=\"SM4_128_CBC\";"
    qt_sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));"
    qt_sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));"
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('aDjwRflBrDjhBZIOFNw3Tg=='),'F3229A0B371ED2D9441B830D21A390C3');"
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('G7yqOKfEyxdagboz6Qf01A=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');"

    qt_sql "SELECT SM3(\"abc\");"
    qt_sql "select sm3(\"abcd\");"
    qt_sql "select sm3sum(\"ab\",\"cd\");"


    sql "set block_encryption_mode=\"AES_256_CBC\";"
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));"
    qt_sql "SELECT TO_BASE64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));"
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');"
    qt_sql "SELECT AES_DECRYPT(FROM_BASE64('mvZT1KJw7N0RJf27aipUpg=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');"

    qt_sql "SELECT MD5(\"abc\");"

    qt_sql "SELECT MD5(\"abcd\");"
    qt_sql "SELECT MD5SUM(\"ab\",\"cd\");"

    sql "set block_encryption_mode=\"SM4_128_CBC\";"
    qt_sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));"
    qt_sql "SELECT TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));"
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('aDjwRflBrDjhBZIOFNw3Tg=='),'F3229A0B371ED2D9441B830D21A390C3');"
    qt_sql "SELECT SM4_DECRYPT(FROM_BASE64('G7yqOKfEyxdagboz6Qf01A=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');"

    qt_sql "SELECT SM3(\"abc\");"
    qt_sql "select sm3(\"abcd\");"
    qt_sql "select sm3sum(\"ab\",\"cd\");"
}
