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

suite("regression_test_variant_schema_change", "variant_type"){
    def table_name = "variant_schema_change"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1");
    """
    def timeout = 60000
    def delta_time = 1000
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

    // sql "set experimental_enable_nereids_planner = true"
    // add, drop columns
    sql """INSERT INTO ${table_name} SELECT *, '{"k1":1, "k2": "hello world", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "4096")"""
    sql "alter table ${table_name} add column v2 variant default null"
    sql """INSERT INTO ${table_name} SELECT k, v, v from ${table_name}"""
    sql "alter table ${table_name} drop column v2"
    sql """INSERT INTO ${table_name} SELECT k, v from ${table_name}"""
    qt_sql """select v['k1'] from ${table_name} order by k limit 10"""
    sql "alter table ${table_name} add column vs string default null"
    sql """INSERT INTO ${table_name} SELECT k, v, v from ${table_name}"""
    qt_sql """select v['k1'] from ${table_name} order by k desc limit 10"""
    qt_sql """select v['k1'], cast(v['k2'] as string) from ${table_name} order by k desc limit 10"""

    // sql "set experimental_enable_nereids_planner = true"
    // add, drop index
    sql "alter table ${table_name} add index btm_idxk (k) using bitmap ;"
    sql """INSERT INTO ${table_name} SELECT k, v, v from ${table_name}"""
    wait_for_latest_op_on_table_finish(table_name, timeout)
    // drop column is linked schema change
    sql "drop index btm_idxk on ${table_name};"
    sql """INSERT INTO ${table_name} SELECT k, v, v from ${table_name} limit 1024"""
    wait_for_latest_op_on_table_finish(table_name, timeout)
    qt_sql """select v['k1'] from ${table_name} order by k desc limit 10"""
    qt_sql """select v['k1'], cast(v['k2'] as string) from ${table_name} order by k desc limit 10"""

    // add, drop materialized view
    createMV("""create materialized view var_order as select vs, k, v from ${table_name} order by vs""")    
    sql """INSERT INTO ${table_name} SELECT k, v, v from ${table_name} limit 4096"""
    createMV("""create materialized view var_cnt as select k, count(k) from ${table_name} group by k""")    
    sql """INSERT INTO ${table_name} SELECT k, v, v from ${table_name} limit 8101"""
    sql """DROP MATERIALIZED VIEW var_cnt ON ${table_name}"""
    sql """INSERT INTO ${table_name} SELECT k, v,v  from ${table_name} limit 1111"""
    // select from mv
    qt_sql """select v['k1'], cast(v['k2'] as string) from ${table_name} order by k desc limit 10"""
}