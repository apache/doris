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

suite("regression_test_variant_with_index", "p0"){
    def timeout = 60000

    sql "set enable_add_index_for_new_data = true"

    def table_name = "var_with_index"
    sql "DROP TABLE IF EXISTS var_with_index"
    sql """
        CREATE TABLE IF NOT EXISTS var_with_index (
            k bigint,
            v variant,
            inv string,
            INDEX idx(inv) USING INVERTED PROPERTIES("parser"="standard")  COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """insert into var_with_index values(1, '{"a" : 0, "b": 3}', 'hello world'), (2, '{"a" : 123}', 'world'),(3, '{"a" : 123}', 'hello world')"""
    sql """set enable_match_without_inverted_index = false"""
    sql """ set enable_common_expr_pushdown = true """
    qt_sql_inv_1 """select v["a"] from var_with_index where inv match 'hello' order by k"""
    qt_sql_inv_2 """select v["a"] from var_with_index where inv match 'hello' and cast(v['a'] as int) > 0 order by k"""
    qt_sql_inv_3 """select * from var_with_index where inv match 'hello' and cast(v["a"] as int) > 0 order by k"""
    sql "truncate table var_with_index"
    // set back configs
    // sql "truncate table ${table_name}"
    sql """insert into var_with_index values(1, '{"a1" : 0, "b1": 3}', 'hello world'), (2, '{"a2" : 123}', 'world'),(3, '{"a3" : 123}', 'hello world')"""
    sql """insert into var_with_index values(4, '{"b1" : 0, "b2": 3}', 'hello world'), (5, '{"b2" : 123}', 'world'),(6, '{"b3" : 123}', 'hello world')"""
    def drop_result = sql """
                      ALTER TABLE var_with_index 
                          drop index idx
                  """
    logger.info("drop index " + "${table_name}" +  "; result: " + drop_result)
    wait_for_last_schema_change_finish(table_name, timeout)
    def show_result = sql "show index from ${table_name}"
    assertEquals(show_result.size(), 0)
    qt_sql_inv4 """select v["a1"] from ${table_name} where cast(v['a1'] as int) = 0"""
    qt_sql_inv5 """select * from ${table_name} order by k"""
    sql "create index inv_idx on ${table_name}(`inv`) using inverted"

    build_index_on_table("inv_idx", table_name)

    wait_for_last_schema_change_finish(table_name, timeout)
    show_result = sql "show index from ${table_name}"
    assertEquals(show_result.size(), 1)
    sql """insert into var_with_index values(7, '{"a1" : 0, "b1": 3}', 'hello world'), (8, '{"a2" : 123}', 'world'),(9, '{"a3" : 123}', 'hello world')"""
    qt_sql_inv6 """select * from ${table_name} order by k desc limit 4"""
    
    sql """insert into var_with_index values(1, '{"a" : 0, "b": 3}', 'hello world'), (2, '{"a" : 123}', 'world'),(3, '{"a" : 123}', 'hello world')"""

    sql """insert into var_with_index values(1, '{"a" : 0, "b": 3}', 'hello world'), (2, '{"a" : 123}', 'world'),(3, '{"a" : 123}', 'hello world')"""
    sql "select * from var_with_index order by k limit 4"

    sql """insert into var_with_index values(1, '{"a" : 0, "b": 3}', 'hello world'), (2, '{"a" : 123}', 'world'),(3, '{"a" : 123}', 'hello world')"""
    sql "select * from var_with_index order by k limit 4"

}