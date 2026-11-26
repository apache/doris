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

suite("test_datev1_minmax_runtime_filter", "nonConcurrent") {
    def disableDatev1 = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'disable_datev1'")[0][1]
    def enableDateConversion = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'enable_date_conversion'")[0][1]
    def sessionVars = [
            "runtime_filter_type", "runtime_filter_mode", "runtime_filter_wait_infinitely",
            "enable_runtime_filter_prune", "disable_join_reorder", "parallel_pipeline_task_num"
    ]
    def previous = sessionVars.collectEntries { name -> [(name): sql("SELECT @@${name}")[0][0]] }

    try {
        sql 'ADMIN SET FRONTEND CONFIG ("disable_datev1" = "false")'
        sql 'ADMIN SET FRONTEND CONFIG ("enable_date_conversion" = "false")'

        sql "DROP TABLE IF EXISTS test_datev1_minmax_runtime_filter"
        sql """
            CREATE TABLE test_datev1_minmax_runtime_filter (
                id INT,
                d DATEV1 NULL
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO test_datev1_minmax_runtime_filter VALUES
                (1, '2012-01-02'), (2, '2012-02-02'), (3, '2012-03-02'),
                (4, '2013-01-01'), (5, NULL)
        """

        sql "SET runtime_filter_type = 4"
        sql "SET runtime_filter_mode = 'GLOBAL'"
        sql "SET runtime_filter_wait_infinitely = true"
        sql "SET enable_runtime_filter_prune = false"
        sql "SET disable_join_reorder = true"
        sql "SET parallel_pipeline_task_num = 2"

        def query = """
            SELECT a.id, a.d, b.id, b.d
            FROM test_datev1_minmax_runtime_filter a
            INNER JOIN [shuffle] test_datev1_minmax_runtime_filter b ON a.d = b.d
            WHERE b.id IN (2, 3)
            ORDER BY a.id, a.d, b.id, b.d
        """
        explain {
            sql query
            contains "HASH JOIN"
            contains "PARTITIONED"
            contains "VEXCHANGE"
            contains "[min_max] -> d"
        }
        qt_with_minmax query

        sql "SET runtime_filter_type = 0"
        qt_without_runtime_filter query
    } finally {
        previous.each { name, value ->
            if (name == "runtime_filter_type" || name == "runtime_filter_mode") {
                sql "SET ${name} = '${value}'"
            } else {
                sql "SET ${name} = ${value}"
            }
        }
        sql "ADMIN SET FRONTEND CONFIG (\"enable_date_conversion\" = \"${enableDateConversion}\")"
        sql "ADMIN SET FRONTEND CONFIG (\"disable_datev1\" = \"${disableDatev1}\")"
    }
}
