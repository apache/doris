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

import static java.util.concurrent.TimeUnit.SECONDS

import org.awaitility.Awaitility

suite("test_dict_nullable_key") {
    sql "drop database if exists test_dict_nullable_key_db"
    sql "create database test_dict_nullable_key_db"
    sql "use test_dict_nullable_key_db"

    sql """
        create table tmp_table_no_null(
            k0 int null,
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into tmp_table_no_null values(1, 'abc');"""
    sql """insert into tmp_table_no_null values(2, 'def');"""

    sql """
        create dictionary dc_tmp_table_no_null using tmp_table_no_null
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    sql """
        refresh dictionary dc_tmp_table_no_null
    """
    qt_sql_constant"""
        select dict_get("test_dict_nullable_key_db.dc_tmp_table_no_null", "k1", 1)  , dict_get("test_dict_nullable_key_db.dc_tmp_table_no_null", "k1", 2)  ;
    """

    sql """
        create table tmp_table_null(
            k0 int null,    
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into tmp_table_null values(1, 'abc');"""
    sql """insert into tmp_table_null values(null, 'def');"""

    sql """
        create dictionary tmp_table_null using tmp_table_null
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    for (int _ = 0; _ < 30 ; _++)
    {
        try {
            sql "refresh dictionary tmp_table_null"
            assert false
        } catch (Exception e) {
            if (e.getMessage().contains("key column k0 has null value")) {
                break;
            } else {
                logger.info("refresh dictionary tmp_table_null failed: " + e.getMessage())
            }
        }
        assertTrue(_ < 30, "refresh dictionary tmp_table_null failed")
        sleep(1000)
    }

    sql """
        drop dictionary tmp_table_null
    """
    sql """
        create dictionary tmp_table_null using tmp_table_null
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600','skip_null_key'='true');
    """
    waitAllDictionariesReady()

    sql """
        refresh dictionary tmp_table_null
    """
    qt_sql_constant"""
        select dict_get("test_dict_nullable_key_db.tmp_table_null", "k1", 1)  , dict_get("test_dict_nullable_key_db.tmp_table_null", "k1", 2)  ;
    """
}