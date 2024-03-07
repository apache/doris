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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.

suite("load") {
    def tables = ["logs1","logs2","logs3"]
    def columnsMap = [
        "logs1":"log_time,machine_name,machine_group,cpu_idle,cpu_nice,cpu_system,cpu_user,cpu_wio,disk_free,disk_total,part_max_used,load_fifteen,load_five,load_one,mem_buffers,mem_cached,mem_free,mem_shared,swap_free,bytes_in,bytes_out",
        "logs2":"log_time,client_ip,request,status_code,object_size",
        "logs3":"log_time,device_id,device_name,device_type,device_floor,event_type,event_unit,event_value"
    ]
 
    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String tableName in tables) {
        streamLoad {
            // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', ','
            set 'compress_type', 'GZ'
            set "columns", columnsMap[tableName]
            set 'timeout', '72000'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url() + '/regression/clickhouse/brown/' + tableName}.gz"""

            time 0


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
    }

    Thread.sleep(70000) // wait for row count report of the tables just loaded
    for (String tableName in tables) {
        sql """ ANALYZE TABLE $tableName WITH SYNC """
    }
}
