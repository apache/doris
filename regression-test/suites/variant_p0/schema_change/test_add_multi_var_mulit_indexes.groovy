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


suite("regression_test_variant_add_multi_var_mulit_indexes", "variant_type"){


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { tableName, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(tableName + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }
    def table_name = "variant_add_multi_var_mulit_indexes"
    int count = new Random().nextInt(10) + 3
    sql "set default_variant_max_subcolumns_count = ${count}"
    sql "set default_variant_enable_typed_paths_to_sparse = false"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "${count}")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """insert into  ${table_name} values (0, '{"a" : 12345,"b" : 2}')"""
    test {
        sql """alter table  ${table_name} add column var2 variant<properties("variant_max_subcolumns_count" = "0")> NULL"""
        exception("The variant_max_subcolumns_count must either be 0 in all columns or greater than 0 in all columns")
    }
    
    sql """ alter table  ${table_name} add column v2 variant<'a': string, 'b': string> NULL"""

    sql """insert into  ${table_name} values (1, '{"a" : 12345,"b" : 2}', '{"a" : 12345,"b" : 3}')"""

    sql """alter table  ${table_name} add column v3 variant NULL"""

    sql """insert into  ${table_name} values (2, '{"a" : 12345,"b" : 2}', '{"a" : 56789,"b" : 3}', '{"a" : 12345,"b" : 2}')"""

    sql """alter table ${table_name} add index idx_v2(v2) using inverted"""
    wait_for_latest_op_on_table_finish(table_name, timeout)

    sql """alter table  ${table_name} add index idx_v3(v3) using inverted"""
    wait_for_latest_op_on_table_finish(table_name, timeout)

    sql """alter table  ${table_name} add index idx_v4(v2) using inverted properties("parser" = "unicode")"""
    wait_for_latest_op_on_table_finish(table_name, timeout)

    sql """alter table  ${table_name} add index idx_v5(v3) using inverted properties("parser" = "unicode", "support_phrase" = "true")"""
    wait_for_latest_op_on_table_finish(table_name, timeout)

    sql """insert into  ${table_name} values (3, '{"a" : 12345,"b" : 2}', '{"a" : 12345,"b" : 2}', '{"a" : 56789,"b" : 2}')"""

    sql """insert into  ${table_name} values (4, '{"a" : 12345,"b" : 2}', '{"a" : 56789,"b" : 2}', '{"a" : 12345,"b" : 3}')"""

    trigger_and_wait_compaction("${table_name}", "full")

    qt_sql "select * from  ${table_name} order by k"

    sql """ set enable_match_without_inverted_index = false"""
    sql """ set enable_inverted_index_query = true"""
    sql """ set enable_common_expr_pushdown = true"""
    sql """ set enable_common_expr_pushdown_for_inverted_index = true"""
    
    qt_sql "select * from  ${table_name} where cast(v2['a'] as string) match '12345' order by k"
    qt_sql "select * from  ${table_name} where cast(v2['b'] as string) match '2' order by k"
    qt_sql "select * from  ${table_name} where cast(v3['b'] as int) = 2 order by k"
    
    
}