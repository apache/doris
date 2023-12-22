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

suite('variant_p0_decimal', "nonConcurrent") {
    def table_name = "var_decimal"
    def create_table = { table, buckets="auto", key_type="DUPLICATE" ->
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table} (
                k bigint,
                v variant
            )
            ${key_type} KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS ${buckets}
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }
    create_table.call(table_name, "1")
    sql "set describe_extend_variant_column = true"
    // insert values
    sql """insert into ${table_name} values (1, '{"decimal1" : 1.2211}'),(2, '{"decimal1" : 11120192919}');"""
    sql """insert into ${table_name} values (3, '{"int64_overflow" : 9223372036854775809}')"""
    sql """insert into ${table_name} values (4, '{"int64_overflow" : 1024}')"""
    sql """insert into ${table_name} values (6, '{"decimal1" : 9223372036854775808.1011}'),(7, '{"decimal1" : 210101111.111111}');"""
    qt_sql1 "desc ${table_name}"
    qt_sql2 "select v from ${table_name} order by k limit 10";
    qt_sql3 """select k, cast(v["decimal1"] as decimal(38, 10)), cast(v["int64_overflow"] as largeint) from ${table_name} order by k limit 10"""
    qt_sql4 """select  /*+ SET_VAR(enable_two_phase_read_opt=false) */ k, cast(v["decimal1"] as decimal(38, 10)), cast(v["int64_overflow"] as largeint) from ${table_name} order by k limit 10"""
    qt_sql5 """select  cast(v["int64_overflow"] as largeint) from  ${table_name} where cast(v["int64_overflow"] as largeint) = 9223372036854775809"""
    qt_sql6 """select  cast(v["decimal1"] as decimal(38, 10)) from ${table_name} where cast(v["decimal1"] as decimal(38, 10)) = 1.2211000000"""
}