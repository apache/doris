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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_json_load_double", "p0") {

    def srcTable = "stringTable"
    def dstTable = "jsonTable"
    def dataFile = "test_json_double.csv"

    sql """ DROP TABLE IF EXISTS ${srcTable} """ 
    sql """ DROP TABLE IF EXISTS ${dstTable} """

    sql """
        CREATE TABLE IF NOT EXISTS ${srcTable} (
            id INT not null,
            v STRING not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${dstTable} (
            id INT not null,
            j JSON not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        insert into ${srcTable}  values(1,'{"rebookProfit":3.729672759600005773616970827788463793694972991943359375}');
    """

    sql """
        insert into ${srcTable}  values(1,'3.729672759600005773616970827788463793694972991943359375');
    """

    sql """ insert into ${dstTable} select * from ${srcTable} """

    // load the json data from csv file
    streamLoad {
        table dstTable
        
        file dataFile // import csv file
        time 10000 // limit inflight 10s
        set 'strict_mode', 'true'

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)

            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(2, json.NumberLoadedRows)
            assertTrue(json.LoadBytes > 0)
            log.info("url: " + json.ErrorURL)
        }
    }

    qt_sql_select_src """  select jsonb_extract(v, '\$.rebookProfit') from ${srcTable} """
    qt_sql_select_dst """  select * from ${dstTable} """

    qt_json_contains """
        select /*+ set_var(enable_fold_constant_by_be=0) */
            json_contains(v, '3.729672759600005773616970827788463793694972991943359375', '\$.rebookProfit')
        from ${srcTable} order by 1 desc;
    """

    qt_json_contains_fold """
        select /*+ set_var(enable_fold_constant_by_be=1) */
            json_contains(v, '3.729672759600005773616970827788463793694972991943359375', '\$.rebookProfit')
        from ${srcTable} order by 1 desc;
    """
}
