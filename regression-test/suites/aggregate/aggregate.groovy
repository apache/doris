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

def tableName = "datetype"

sql """ DROP TABLE IF EXISTS ${tableName} """
sql """
    CREATE TABLE ${tableName} (
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

qt_aggregate_min_max """ select max(upper(c_string)), min(upper(c_string)) from ${tableName} """

qt_aggregate_avg """ select avg(c_bigint), avg(c_double) from ${tableName} """
qt_aggregate_avg2 """ select avg(distinct c_bigint), avg(distinct c_double) from ${tableName} """

qt_aggregate_multi_count """ select count(c_bigint),count(c_double),count(c_string),count(c_date),count(c_timestamp),count(c_boolean) from ${tableName} """

qt_aggregate_multi_distinct_count """ select count(distinct c_bigint),count(distinct c_double),count(distinct c_string),count(distinct c_date),count(distinct c_timestamp),count(distinct c_boolean) from ${tableName} """

qt_aggregate_multi_max """ select max(c_bigint), max(c_double),max(c_string), max(c_date), max(c_timestamp) from ${tableName} """

qt_aggregate_multi_min """ select min(c_bigint), min(c_double), min(c_string), min(c_date), min(c_timestamp) from ${tableName} """

qt_aggregate_mix """ select count(c_string), max(c_double), avg(c_bigint) from ${tableName} """

qt_aggregate_stddev1 """ select stddev_pop(c_bigint), stddev_pop(c_double) from ${tableName} """
qt_aggregate_stddev2 """ select stddev_pop(distinct c_bigint), stddev_pop(c_double) from ${tableName} """
qt_aggregate_stddev3 """ select stddev_pop(c_bigint), stddev_pop(distinct c_double) from ${tableName} """
qt_aggregate_stddev4 """ select stddev_samp(c_bigint), stddev_samp(c_double) from ${tableName} """
qt_aggregate_stddev5 """ select stddev_samp(distinct c_bigint), stddev_samp(c_double) from ${tableName} """
qt_aggregate_stddev6 """ select stddev_samp(c_bigint), stddev_samp(distinct c_double) from ${tableName} """

qt_aggregate_sum """ select sum(c_bigint), sum(c_double) from ${tableName} """

qt_aggregate_distinct_sum """ select sum(distinct c_bigint), sum(distinct c_double) from ${tableName} """

qt_aggregate_variance """ select var_pop(c_bigint), var_pop(c_double) from ${tableName} """
qt_aggregate_variance2 """ select var_pop(distinct c_bigint), var_pop(c_double) from ${tableName} """
qt_aggregate_variance3 """ select var_pop(c_bigint), var_pop(distinct c_double) from ${tableName} """
qt_aggregate_variance4 """ select var_samp(c_bigint), var_samp(c_double) from ${tableName} """
qt_aggregate_variance5 """ select var_samp(distinct c_bigint), var_samp(c_double) from ${tableName} """
qt_aggregate_variance6 """ select var_samp(c_bigint), var_samp(distinct c_double) from ${tableName} """
qt_aggregate_variance7 """ select variance(c_bigint), variance(c_double) from ${tableName}  """
qt_aggregate_variance8 """ select variance(distinct c_bigint), variance(c_double) from ${tableName}  """
qt_aggregate_variance9 """ select variance(c_bigint), variance(distinct c_double) from ${tableName}  """
