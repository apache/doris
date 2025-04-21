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

suite("regression_test_variant_var_index", "p0, nonConcurrent"){
    def table_name = "var_index"
    sql "DROP TABLE IF EXISTS var_index"
    sql """
        CREATE TABLE IF NOT EXISTS var_index (
            k bigint,
            v variant<'timestamp' : double, 'a' : int, 'b' : string, 'c' : int>,
            INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """insert into var_index values(1, '{"a" : 123, "b" : "xxxyyy", "c" : 111999111}')"""
    sql """insert into var_index values(2, '{"a" : 18811, "b" : "hello world", "c" : 1181111}')"""
    sql """insert into var_index values(3, '{"a" : 18811, "b" : "hello wworld", "c" : 11111}')"""
    sql """insert into var_index values(4, '{"a" : 1234, "b" : "hello xxx world", "c" : 8181111}')"""
    trigger_and_wait_compaction(table_name, "full")
    sql """ set enable_common_expr_pushdown = true """
    sql """set enable_match_without_inverted_index = false""" 
    qt_sql """select * from var_index where cast(v["a"] as smallint) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 1024 order by k"""
    sql """set enable_match_without_inverted_index = true""" 
    sql """insert into var_index values(5, '{"a" : 123456789, "b" : 123456, "c" : 8181111}')"""
    qt_sql """select * from var_index where cast(v["a"] as int) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 11111 order by k"""
    // insert double/float/array/json
    sql """insert into var_index values(6, '{"timestamp": 1713283200.060359}')"""
    sql """insert into var_index values(7, '{"timestamp": 17.0}')"""
    sql """insert into var_index values(8, '{"arr": [123]}')"""
    sql """insert into var_index values(9, '{"timestamp": 17.0}'),(10, '{"timestamp": "17.0"}')"""
    sql """insert into var_index values(11, '{"nested": [{"a" : 1}]}'),(11, '{"nested": [{"b" : "1024"}]}')"""
    trigger_and_wait_compaction(table_name, "full")
    qt_sql "select * from var_index order by k limit 15"

    sql """ set disable_inverted_index_v1_for_variant = true """
    sql "DROP TABLE IF EXISTS var_index"
    boolean findException = false
    try {
        sql """
            CREATE TABLE IF NOT EXISTS var_index (
                k bigint,
                v variant,
                INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT ''
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1 
            properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
        """
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("not supported in inverted index format V1"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    sql """
        CREATE TABLE IF NOT EXISTS var_index (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
    """

    try {
        sql """ALTER TABLE var_index ADD INDEX idx_var(v) USING INVERTED"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("not supported in inverted index format V1"))
        findException = true
    }
    assertTrue(findException)

    sql """ set disable_inverted_index_v1_for_variant = false """
    sql """
        CREATE TABLE IF NOT EXISTS var_index (
            k bigint,
            v variant,
            INDEX idx_var(v) USING INVERTED  PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
    """

    sql "DROP TABLE IF EXISTS var_index"
    sql """
        CREATE TABLE IF NOT EXISTS var_index (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
    """
    sql """ALTER TABLE var_index ADD INDEX idx_var(v) USING INVERTED"""
    sql """ set disable_inverted_index_v1_for_variant = true """
    sql "DROP TABLE IF EXISTS objects"
    sql """
         CREATE TABLE `objects` (
          `id` int NOT NULL,
          `overflow_properties` variant NULL,
          INDEX idx1 (`overflow_properties`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "english", "lower_case" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
         "variant_max_subcolumns_count" = "0"
        );
    """ 
    sql """
        INSERT INTO objects (id, overflow_properties)
        VALUES 
        (6, '{"color":"Bright Red","description":"A bright red circular object with a metallic shine","shape":"Large Circle","tags":["metallic","reflective"]}'),
        (7, '{"color":"Deep Blue","description":"Opaque square made of plastic in deep blue","shape":"Small Square","tags":["opaque","plastic"]}'),
        (8, '{"color":"Green","description":"Tall green triangle carved from wood","shape":"Tall Triangle","tags":["matte","wood"]}'),
        (9, '{"color":"Reddish Orange","description":"Glossy ceramic hexagon with reddish orange tint","shape":"Flat Hexagon","tags":["glossy","ceramic"]}'),
        (10, '{"color":"Yellow","description":"Shiny yellow circular badge","shape":"Wide Circle","tags":["shiny","plastic"]}');    
    """
    sql "set enable_match_without_inverted_index = false"
    qt_sql "select count() from objects where (overflow_properties['color'] MATCH_PHRASE 'Blue')"    
}