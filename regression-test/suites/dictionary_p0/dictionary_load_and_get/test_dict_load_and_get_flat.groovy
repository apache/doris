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

suite("test_dict_load_and_get_flat") {
    sql "drop database if exists test_dict_load_and_get_flat_db"
    sql "create database test_dict_load_and_get_flat_db"
    sql "use test_dict_load_and_get_flat_db"

    // ---- duplicate key rejection ----
    sql """
        create table flat_single_key_with_duplicate(
            k0 int not null,
            v0 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """

    sql """insert into flat_single_key_with_duplicate values(1, 'abc');"""
    sql """insert into flat_single_key_with_duplicate values(1, 'def');"""

    sql """
        create dictionary dc_flat_with_duplicate using flat_single_key_with_duplicate
        (
            k0 KEY,
            v0 VALUE
        )
        LAYOUT(FLAT)
        properties('data_lifetime'='600');
    """

    boolean sawDuplicateError = false
    for (int i = 0; i < 30; i++) {
        try {
            sql "refresh dictionary dc_flat_with_duplicate"
            assertTrue(false, "refresh should fail on duplicate key")
        } catch (Exception e) {
            if (e.getMessage().contains("The key has duplicate data in FlatDictionary")) {
                sawDuplicateError = true
                break
            } else {
                logger.info("refresh dc_flat_with_duplicate failed: " + e.getMessage())
            }
        }
        sleep(1000)
    }
    assertTrue(sawDuplicateError, "refresh dc_flat_with_duplicate did not report duplicate error")

    // ---- happy path: dense keys 0/1, sparse key 100, plus a missing key ----
    sql """
        create table flat_single_key_without_duplicate(
            k0 int not null,
            str_not_null string not null,
            str_null string null,
            int_not_null int not null,
            int_null int null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """

    sql """insert into flat_single_key_without_duplicate values(0, 'abc', 'def', 100, 10000);"""
    sql """insert into flat_single_key_without_duplicate values(1, 'ABC', null, 200, null);"""
    sql """insert into flat_single_key_without_duplicate values(100, 'sparse', 'S', 300, 30000);"""

    sql """
        create dictionary dc_flat_without_duplicate using flat_single_key_without_duplicate
        (
            k0 KEY,
            str_not_null VALUE,
            str_null VALUE,
            int_not_null VALUE,
            int_null VALUE
        )
        LAYOUT(FLAT)
        properties('data_lifetime'='600');
    """
    waitDictionaryReady("dc_flat_without_duplicate")

    // present keys (0, 1, 100) return their values; missing key (5) returns null
    order_qt_sql_hit """
        select dict_get("test_dict_load_and_get_flat_db.dc_flat_without_duplicate", "str_not_null", k0) as v
        from flat_single_key_without_duplicate order by k0;
    """

    // constant lookups: 0 hit, 1 hit, 100 hit sparse, 5 miss -> null
    order_qt_sql_constant """
        select dict_get("test_dict_load_and_get_flat_db.dc_flat_without_duplicate", "str_not_null", 0),
               dict_get("test_dict_load_and_get_flat_db.dc_flat_without_duplicate", "str_not_null", 1),
               dict_get("test_dict_load_and_get_flat_db.dc_flat_without_duplicate", "str_not_null", 100),
               dict_get("test_dict_load_and_get_flat_db.dc_flat_without_duplicate", "str_not_null", 5);
    """

    // nullable value column: key 1 has null str_null -> null
    order_qt_sql_nullable_value """
        select dict_get("test_dict_load_and_get_flat_db.dc_flat_without_duplicate", "str_null", 0),
               dict_get("test_dict_load_and_get_flat_db.dc_flat_without_duplicate", "str_null", 1);
    """

    // dict_get_many with a single-field key on a FLAT dictionary
    order_qt_sql_get_many """
        select dict_get_many("test_dict_load_and_get_flat_db.dc_flat_without_duplicate",
                             ["str_not_null", "int_not_null"], struct(0));
    """

    // ---- key over MAX_ARRAY_SIZE (500000) is rejected at load ----
    sql """
        create table flat_over_max(
            k0 bigint not null,
            v0 int not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into flat_over_max values(500000, 1);"""

    sql """
        create dictionary dc_flat_over_max using flat_over_max
        (
            k0 KEY,
            v0 VALUE
        )
        LAYOUT(FLAT)
        properties('data_lifetime'='600');
    """

    boolean sawOverMaxError = false
    for (int i = 0; i < 30; i++) {
        try {
            sql "refresh dictionary dc_flat_over_max"
            assertTrue(false, "refresh should fail on over-max key")
        } catch (Exception e) {
            if (e.getMessage().contains("exceeds max array size")) {
                sawOverMaxError = true
                break
            } else {
                logger.info("refresh dc_flat_over_max failed: " + e.getMessage())
            }
        }
        sleep(1000)
    }
    assertTrue(sawOverMaxError, "refresh dc_flat_over_max did not report over-max error")
}
