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

suite("test_stream_load_with_udf", "p0") {
    def tableName = "test_stream_load_with_udf"
	sql """ set enable_fallback_to_original_planner=false;"""
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
                id int,
                v1 string
                ) ENGINE=OLAP
                duplicate key (`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1"
                );
    """
    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)
    log.info("Jar path: ${jarPath}".toString())

    sql """ ADMIN SET FRONTEND CONFIG ("enable_udf_in_load" = "true"); """
    try_sql("DROP GLOBAL FUNCTION IF EXISTS java_udf_int_load_global(int);")
    sql """ CREATE GLOBAL FUNCTION java_udf_int_load_global(int) RETURNS int PROPERTIES (
        "file"="file://${jarPath}",
        "symbol"="org.apache.doris.udf.IntLoadTest",
        "type"="JAVA_UDF"
    ); """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', """ tmp,v1, id=java_udf_int_load_global(tmp) """
        file 'test_stream_load_udf.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(3, json.NumberLoadedRows)
            }
    }

    sql """sync"""

    qt_sql """select * from ${tableName} order by id;"""

}
