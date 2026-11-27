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

import org.apache.doris.regression.action.ProfileAction

suite("test_runtime_filter_on_exchange", "p0") {
    sql "set disable_join_reorder = true"
    sql "set enable_runtime_filter_prune = false"
    sql "set enable_runtime_filter_partition_prune = false"
    sql "set enable_runtime_filter_bucket_prune = false"
    sql "set runtime_filter_wait_time_ms = 0"
    sql "set runtime_filter_wait_infinitely = false"
    sql "set parallel_pipeline_task_num = 3"
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
    sql "set enable_condition_cache = false"

    sql "drop table if exists rf_exchange_fact"
    sql """create table rf_exchange_fact (id int not null, k bigint null, v bigint not null)
           duplicate key(id) distributed by hash(id) buckets 3
           properties("replication_num" = "1")"""
    sql """insert into rf_exchange_fact
           select cast(number as int), if(number % 17 = 0, null, number % 1024), number * 3
           from numbers("number" = "32768")"""
    sql "drop table if exists rf_exchange_dim"
    sql """create table rf_exchange_dim (k bigint not null)
           duplicate key(k) distributed by hash(k) buckets 1
           properties("replication_num" = "1")"""
    sql "insert into rf_exchange_dim values (7)"

    for (boolean enabled : [false, true]) {
        sql "set enable_runtime_filter_on_exchange = ${enabled}"
        for (String type : ['IN', 'BLOOM_FILTER', 'MIN_MAX', 'IN_OR_BLOOM_FILTER']) {
            sql "set runtime_filter_type = '${type}'"
            order_qt_direct """select count(*), sum(f.id), sum(f.v)
                               from rf_exchange_fact f join [shuffle] rf_exchange_dim d on f.k = d.k"""
            order_qt_alias """select count(*), sum(p.id), sum(p.v)
                              from (select id, k + 1 as x, v from rf_exchange_fact) p
                              join [shuffle] (select k + 1 as x from rf_exchange_dim) d on p.x = d.x"""
            order_qt_union """select count(*), sum(p.id), sum(p.v)
                              from (select id, k as x, v from rf_exchange_fact where id < 16384
                                    union all
                                    select id, k as x, v from rf_exchange_fact where id >= 16384) p
                              join [shuffle] rf_exchange_dim d on p.x = d.k"""
            order_qt_cte """with p as (select id, k, v from rf_exchange_fact)
                            select count(*), sum(a.id), sum(a.v)
                            from p a join [shuffle] p b on a.id = b.id
                            join [shuffle] rf_exchange_dim d on a.k = d.k"""
            order_qt_grouped """select count(*), sum(p.total)
                                from (select k, sum(v) as total from rf_exchange_fact group by k) p
                                join [shuffle] rf_exchange_dim d on p.k = d.k"""
            order_qt_empty """select count(*) from rf_exchange_fact f
                              join [shuffle] (select k from rf_exchange_dim where k < 0) d on f.k = d.k"""
            order_qt_outer """select count(*), count(d.k), sum(f.id)
                              from rf_exchange_fact f left join [shuffle] rf_exchange_dim d on f.k = d.k"""
            order_qt_limit """select count(*), sum(p.id), sum(p.v)
                              from (select id, k, v from rf_exchange_fact order by id limit 20 offset 5) p
                              join [shuffle] rf_exchange_dim d on p.k = d.k"""
        }
    }

    // Delay the one-row build input while the probe scan fills the exchange queues.
    // Scan RFs remain enabled: positive receiver counters prove that rows which escaped
    // scan filtering are pruned at the exchange, rather than only checking the final join.
    String delayedBuildQuery = """select count(*), sum(f.id), sum(f.v)
            from rf_exchange_fact f join [shuffle]
                 (select k + cast(sleep(2) as bigint) as k from rf_exchange_dim) d on f.k = d.k"""
    sql "set enable_profile = true"
    sql "set profile_level = 2"
    sql "set runtime_filter_type = 'IN'"
    for (boolean enabled : [false, true]) {
        sql "set enable_runtime_filter_on_exchange = ${enabled}"
        String token = "rf_exchange_${enabled}_${UUID.randomUUID()}"
        order_qt_delayed "/* ${token} */ ${delayedBuildQuery}"
        String profile = new ProfileAction(context).getProfileBySql(token, ['EXCHANGE_OPERATOR'])
        boolean receiver = false
        List<BigDecimal> inputRows = []
        List<BigDecimal> filteredRows = []
        profile.eachLine { String line ->
            String text = line.trim()
            if (text ==~ /^[A-Z][A-Z0-9_]*_OPERATOR\b.*/) {
                receiver = text.startsWith('EXCHANGE_OPERATOR')
            }
            if (receiver) {
                def input = (text =~ /^- RF[0-9]+ InputRows:\s*([0-9,.]+)/)
                def filtered = (text =~ /^- RF[0-9]+ FilterRows:\s*([0-9,.]+)/)
                if (input.find()) {
                    inputRows.add(new BigDecimal(input.group(1).replace(',', '')))
                }
                if (filtered.find()) {
                    filteredRows.add(new BigDecimal(filtered.group(1).replace(',', '')))
                }
            }
        }
        if (enabled) {
            if (!inputRows.any { it > 0 } || !filteredRows.any { it > 0 }) {
                throw new IllegalStateException("Receiver RF did not prune received rows: ${profile}")
            }
        } else if (!inputRows.isEmpty() || !filteredRows.isEmpty()) {
            throw new IllegalStateException("Receiver RF counters exist with the option disabled: ${profile}")
        }
    }
}
