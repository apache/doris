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

suite("distinct_mixed", "agg_shuffle_prune_func") {
    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """set enable_sql_cache=false;"""
    sql """set enable_query_cache=false;"""
    sql """set agg_phase=0;"""

    def dbName = context.config.getDbNameByFile(context.file)
    sql "create database if not exists ${dbName};"
    sql "use ${dbName};"

    def tptn = 8
    sql "set parallel_fragment_exec_instance_num=1;"
    sql "set parallel_pipeline_task_num=${tptn};"
    sql "set be_number_for_test=1;"

    def rowCount = "480000"
    def highNdv = (tptn * 512 + tptn * 64).toString()
    def lowNdv = "4"
    def esc = { Object v -> v == null ? "null" : v.toString().replace("\\", "\\\\").replace("'", "\\'") }
    def norm = { rows -> rows.collect { r -> r.collect { v -> v == null ? "NULL" : v.toString() }.join("||") }.sort() }
    def explainText = { String q -> (sql "explain physical plan " + q).collect { it[0].toString() }.join("\n") }
    def shuffleSigns = { String e ->
        def signs = []
        def matcher = (e =~ /orderedShuffledColumns=\[([0-9,\s-]*)\]/)
        while (matcher.find()) {
            signs << "[" + matcher.group(1).replaceAll("\\s+", "") + "]"
        }
        signs
    }
    def setStats = { String t, String c, String n, String nn, String ds, String mi, String ma, String hv ->
        def hotValues = hv == null ? "" : ", 'hot_values'='${esc(hv)}'"
        sql """alter table ${t} modify column ${c} set stats (
                'row_count'='${rowCount}',
                'ndv'='${esc(n)}',
                'num_nulls'='${esc(nn)}',
                'data_size'='${esc(ds)}',
                'min_value'='${esc(mi)}',
                'max_value'='${esc(ma)}'${hotValues});"""
    }
    def checkPlanChange = { String id, String q, boolean expectChange ->
        sql "set enable_shuffle_key_prune=false;"
        def explainOff = explainText(q)
        sql "set enable_shuffle_key_prune=true;"
        def explainOn = explainText(q)
        if (expectChange) {
            assertTrue(shuffleSigns(explainOff).toString() != shuffleSigns(explainOn).toString(),
                "${id}: expected plan change")
        } else {
            assertTrue(shuffleSigns(explainOff).toString() == shuffleSigns(explainOn).toString(),
                "${id}: plan should stay unchanged")
        }
        sql "set enable_shuffle_key_prune=false;"
        def resultOff = sql q
        sql "set enable_shuffle_key_prune=true;"
        def resultOn = sql q
        assertTrue(norm(resultOff) == norm(resultOn), "${id}: results differ")
    }

    sql "drop table if exists t_08_distinct;"
    sql """create table t_08_distinct (
            id bigint, a bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, v bigint
        ) duplicate key(id) distributed by hash(id) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_08_distinct
           select number, number % 5000, number % 4800, number % 4600, number % 4400,
                  number % 4200, number % 4000, number % 3800, number
           from numbers("number"="2000");"""

    def distinctStats = { String ndv ->
        setStats("t_08_distinct", "id", "60000", "0", "1600000", "0", "1999", "")
        ["a", "b", "c", "d", "e", "f", "g", "v"].each { col ->
            setStats("t_08_distinct", col, ndv, "0", "1600000", "0", "9999", "")
        }
    }

    distinctStats(highNdv)
    sql "set agg_phase=2;"
    checkPlanChange("distinct_basic",
        "select a,b,c,d,e,f,count(distinct g) from t_08_distinct group by a,b,c,d,e,f",
        true)

    distinctStats(lowNdv)
    checkPlanChange("distinct_low_ndv",
        "select a,b,c,d,e,f,count(distinct g) from t_08_distinct group by a,b,c,d,e,f",
        false)

    distinctStats(highNdv)
    checkPlanChange("distinct_avg_mixed",
        "select a,b,c,d,e,f,count(distinct g),avg(v) from t_08_distinct group by a,b,c,d,e,f",
        true)

    distinctStats(highNdv)
    checkPlanChange("distinct_skew_hint",
        "select a,count(distinct [skew] g) from t_08_distinct group by a",
        false)

    sql "drop table if exists t_08_mix_agg;"
    sql "drop table if exists t_08_mix_probe;"
    sql """create table t_08_mix_agg (
            x bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, v bigint
        ) duplicate key(x) distributed by hash(x) buckets 4 properties ("replication_num"="1");"""
    sql """create table t_08_mix_probe (
            b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, v bigint
        ) duplicate key(b) distributed by hash(c) buckets 4 properties ("replication_num"="1");"""
    sql """insert into t_08_mix_agg
           select number, number % 50, number % 800, number % 700, number % 600, number % 500, number % 400, number
           from numbers("number"="1000");"""
    sql "insert into t_08_mix_probe select b, c, d, e, f, g, v from t_08_mix_agg;"

    setStats("t_08_mix_agg", "x", "1000", "0", "800000", "0", "999", "")
    setStats("t_08_mix_agg", "b", "50", "0", "800000", "0", "49", "")
    setStats("t_08_mix_agg", "c", "10000", "0", "800000", "0", "799", "")
    setStats("t_08_mix_agg", "d", "10000", "0", "800000", "0", "699", "")
    setStats("t_08_mix_agg", "e", "10000", "0", "800000", "0", "599", "")
    setStats("t_08_mix_agg", "f", "500", "0", "800000", "0", "499", "")
    setStats("t_08_mix_agg", "g", "400", "0", "800000", "0", "399", "")
    setStats("t_08_mix_agg", "v", "1000", "0", "800000", "0", "999", "")

    setStats("t_08_mix_probe", "b", "50", "0", "800000", "0", "49", null)
    setStats("t_08_mix_probe", "c", "800", "0", "800000", "0", "799", null)
    setStats("t_08_mix_probe", "d", "700", "0", "800000", "0", "699", null)
    setStats("t_08_mix_probe", "e", "600", "0", "800000", "0", "599", null)
    setStats("t_08_mix_probe", "f", "500", "0", "800000", "0", "499", null)
    setStats("t_08_mix_probe", "g", "400", "0", "800000", "0", "399", null)
    setStats("t_08_mix_probe", "v", "1000", "0", "800000", "0", "999", null)

    sql "set agg_phase=0;"
    checkPlanChange("mixed_join_conservative",
        """select s.b, s.c, s.v, a.sv
           from t_08_mix_probe s
           join (select b, c, d, e, f, g, sum(v) as sv from t_08_mix_agg group by b, c, d, e, f, g) a
             on s.b = a.b and s.c = a.c and s.d = a.d and s.e = a.e and s.f = a.f and s.g = a.g""",
        false)

    sql "set agg_phase=0;"
    sql "set enable_shuffle_key_prune=true;"
}
