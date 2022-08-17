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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("aggregate", "query,p0") {
    def tableName = "datetype"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                c_bigint bigint,
                c_double double,
                c_string string,
                c_date date,
                c_timestamp datetime,
                c_boolean boolean,
                c_short_decimal decimal(5,2),
                c_long_decimal decimal(27,9)
            )
            DUPLICATE KEY(c_bigint)
            DISTRIBUTED BY HASH(c_bigint) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """

    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '|'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'datetype.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    qt_aggregate """ select max(upper(c_string)), min(upper(c_string)) from ${tableName} """
    qt_aggregate """ select avg(c_bigint), avg(c_double) from ${tableName} """
    qt_aggregate """ select avg(distinct c_bigint), avg(distinct c_double) from ${tableName} """
    qt_aggregate """ select count(c_bigint),count(c_double),count(c_string),count(c_date),count(c_timestamp),count(c_boolean) from ${tableName} """
    qt_aggregate """ select count(distinct c_bigint),count(distinct c_double),count(distinct c_string),count(distinct c_date),count(distinct c_timestamp),count(distinct c_boolean) from ${tableName} """
    qt_aggregate """ select max(c_bigint), max(c_double),max(c_string), max(c_date), max(c_timestamp) from ${tableName} """
    qt_aggregate """ select min(c_bigint), min(c_double), min(c_string), min(c_date), min(c_timestamp) from ${tableName} """
    qt_aggregate """ select count(c_string), max(c_double), avg(c_bigint) from ${tableName} """
    qt_aggregate """ select stddev_pop(c_bigint), stddev_pop(c_double) from ${tableName} """
    qt_aggregate """ select stddev_pop(distinct c_bigint), stddev_pop(c_double) from ${tableName} """
    qt_aggregate """ select stddev_pop(c_bigint), stddev_pop(distinct c_double) from ${tableName} """
    qt_aggregate """ select stddev_samp(c_bigint), stddev_samp(c_double) from ${tableName} """
    qt_aggregate """ select stddev_samp(distinct c_bigint), stddev_samp(c_double) from ${tableName} """
    qt_aggregate """ select stddev_samp(c_bigint), stddev_samp(distinct c_double) from ${tableName} """
    qt_aggregate """ select sum(c_bigint), sum(c_double) from ${tableName} """
    qt_aggregate """ select sum(distinct c_bigint), sum(distinct c_double) from ${tableName} """
    qt_aggregate """ select var_pop(c_bigint), var_pop(c_double) from ${tableName} """
    qt_aggregate """ select var_pop(distinct c_bigint), var_pop(c_double) from ${tableName} """
    qt_aggregate """ select var_pop(c_bigint), var_pop(distinct c_double) from ${tableName} """
    qt_aggregate """ select var_samp(c_bigint), var_samp(c_double) from ${tableName} """
    qt_aggregate """ select var_samp(distinct c_bigint), var_samp(c_double) from ${tableName} """
    qt_aggregate """ select var_samp(c_bigint), var_samp(distinct c_double) from ${tableName} """
    qt_aggregate """ select variance(c_bigint), variance(c_double) from ${tableName}  """
    qt_aggregate """ select variance(distinct c_bigint), variance(c_double) from ${tableName}  """
    qt_aggregate """ select variance(c_bigint), variance(distinct c_double) from ${tableName}  """
    qt_aggregate """ select 1 k1, 2 k2, c_bigint k3, sum(c_double) from ${tableName} group by 1, k2, k3 order by k1, k2, k3 """
    qt_aggregate """ select (k1 + k2) * k3 k4 from (select 1 k1, 2 k2, c_bigint k3, sum(c_double) from ${tableName} group by 1, k2, k3) t order by k4 """

    sql "use test_query_db"
    List<String> fields = ["k1", "k2", "k3", "k4", "k5", "k6", "k10", "k11", "k7", "k8", "k9"]
    // test_query_normal_aggression
    String k1 = fields[1]
    String k2 = fields[2]
    qt_aggregate1"select ${k1}, sum(${k2}) over (partition by ${k1}) as wj from baseall order by ${k1}, wj"
    qt_aggregate2"""
                select t1.${k1}, t2.mysum from baseall t1,
                (select ${k1}, sum(${k2}) as mysum from baseall 
                group by ${k1}) t2 where t1.${k1}=t2.${k1} 
                order by t1.${k1}, t2.mysum
                """

    qt_aggregate3"select * from (select ${k1}, sum(${k2}) over (partition by ${k1}) as wj from baseall) b order by ${k1}, wj"
    order_qt_aggregate4"select ${k1}, min(${k2}) over (partition by ${k1}) as wj from baseall order by ${k1}, wj"
    qt_aggregate5"""
                    select t1.${k1}, t2.mysum from baseall t1,
                    (select ${k1}, min(${k2}) as mysum from baseall 
                    group by ${k1}) t2 where t1.${k1}=t2.${k1} 
                    order by t1.${k1}, t2.mysum
                """
    qt_aggregate6"select ${k1}, max(${k2}) over (partition by ${k1}) as wj from baseall order by ${k1}, wj"
    qt_aggregate7"""
                    select t1.${k1}, t2.mysum from baseall t1,
                    (select ${k1}, max(${k2}) as mysum from baseall 
                    group by ${k1}) t2 where t1.${k1}=t2.${k1} 
                    order by t1.${k1}, t2.mysum
                """
    qt_aggregate8"select ${k1}, count(${k2}) over (partition by ${k1}) as wj from baseall order by ${k1}, wj"
    qt_aggregate9"""select t1.${k1}, t2.mysum from baseall t1,
                (select ${k1}, count(${k2}) as mysum from baseall
                group by ${k1}) t2 where t1.${k1}=t2.${k1}
                order by t1.${k1}, t2.mysum
                """

    // test_query_normal_order_aggression
    String k3 = fields[8]
    qt_aggregate10"select ${k1}, ${k3}, count(${k2}) over (partition by ${k1}, ${k3} order by ${k3}) as wj from baseall order by ${k1}, ${k3}, wj"
    qt_aggregate11"""select ${k1}, count(${k2}) over (partition by ${k1} order by ${k3}
             range between unbounded preceding and unbounded following)
             as wj from baseall order by ${k1}, wj"""
    qt_aggregate12"""
            select ${k1}, count(${k2}) over (partition by ${k1} order by ${k3}
            rows between unbounded preceding and unbounded following)
            as wj from baseall order by ${k1}, wj
            """
    qt_aggregate13"""
            select t1.${k1}, t2.mysum from baseall t1,
            (select ${k1}, count(${k2}) as mysum from baseall 
            group by ${k1}) t2 where t1.${k1}=t2.${k1} 
            order by t1.${k1}, t2.mysum
            """
    qt_aggregate14"""
            select t1.${k1}, t1.${k3}, t2.mysum from baseall t1,
            (select ${k1}, ${k3}, count(${k2}) as mysum from baseall
            group by ${k1}, ${k3}) t2 where t1.${k1}=t2.${k1} and t1.${k3}=t2.${k3}
            order by t1.${k1}, t1.${k3}, t2.mysum
            """
    qt_aggregate15"""select ${k1}, ${k3}, max(${k2}) over (partition by ${k1}, ${k3} order by ${k3})
             as wj from baseall order by ${k1}, ${k3}, wj"""
    qt_aggregate16"""select ${k1}, max(${k2}) over (partition by ${k1} order by ${k3}
             range between unbounded preceding and unbounded following)
             as wj from baseall order by ${k1}, wj"""
    qt_aggregate17"""select ${k1}, max(${k2}) over (partition by ${k1} order by ${k3}
             rows between unbounded preceding and unbounded following)
             as wj from baseall order by ${k1}, wj"""
    qt_aggregate18"""select t1.${k1}, t2.mysum from baseall t1,
             (select ${k1}, max(${k2}) as mysum from baseall
             group by ${k1}) t2 where t1.${k1}=t2.${k1}
             order by t1.${k1}, t2.mysum"""
    qt_aggregate19"""select t1.${k1}, t1.${k3}, t2.mysum from baseall t1,
             (select ${k1}, ${k3}, max(${k2}) as mysum from baseall
             group by ${k1}, ${k3}) t2 where t1.${k1}=t2.${k1} and t1.${k3}=t2.${k3}
             order by t1.${k1}, t1.${k3}, t2.mysum"""
    qt_aggregate20"""select ${k1}, ${k3}, min(${k2}) over (partition by ${k1}, ${k3} order by ${k3})
             as wj from baseall order by ${k1}, ${k3}, wj"""
    qt_aggregate21"""select ${k1}, min(${k2}) over (partition by ${k1} order by ${k3} 
             range between unbounded preceding and unbounded following) 
             as wj from baseall order by ${k1}, wj"""
    qt_aggregate22"""select ${k1}, min(${k2}) over (partition by ${k1} order by ${k3} 
             rows between unbounded preceding and unbounded following) 
             as wj from baseall order by ${k1}, wj"""
    qt_aggregate23"""select t1.${k1}, t2.mysum from baseall t1,
             (select ${k1}, min(${k2}) as mysum from baseall 
             group by ${k1}) t2 where t1.${k1}=t2.${k1} 
             order by t1.${k1}, t2.mysum"""
    qt_aggregate24"""select t1.${k1}, t1.${k3}, t2.mysum from baseall t1,
             (select ${k1}, ${k3}, min(${k2}) as mysum from baseall 
             group by ${k1}, ${k3}) t2 where t1.${k1}=t2.${k1} and t1.${k3}=t2.${k3}
             order by t1.${k1}, t1.${k3}, t2.mysum"""
    qt_aggregate25"""select ${k1}, ${k3}, sum(${k2}) over (partition by ${k1}, ${k3} order by ${k3})
             as wj from baseall order by ${k1}, ${k3}, wj
            """
    qt_aggregate26"""select ${k1}, sum(${k2}) over (partition by ${k1} order by ${k3} 
             range between unbounded preceding and unbounded following) 
             as wj from baseall order by ${k1}, wj
            """
    qt_aggregate27"""select ${k1}, sum(${k2}) over (partition by ${k1} order by ${k3} 
             rows between unbounded preceding and unbounded following) 
             as wj from baseall order by ${k1}, wj
            """
    qt_aggregate28"""select t1.${k1}, t2.mysum from baseall t1,
             (select ${k1}, sum(${k2}) as mysum from baseall 
             group by ${k1}) t2 where t1.${k1}=t2.${k1} 
             order by t1.${k1}, t2.mysum"""
    qt_aggregate29"""select t1.${k1}, t1.${k3}, t2.mysum from baseall t1,
             (select ${k1}, ${k3}, sum(${k2}) as mysum from baseall 
             group by ${k1}, ${k3}) t2 where t1.${k1}=t2.${k1} and t1.${k3}=t2.${k3}
             order by t1.${k1}, t1.${k3}, t2.mysum"""
}
