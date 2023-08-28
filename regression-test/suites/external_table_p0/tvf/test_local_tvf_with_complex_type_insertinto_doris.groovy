import org.junit.Assert

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

// This suit test the `backends` tvf
suite("test_local_tvf_with_complex_type_insertinto_doris", "p0") {
    List<List<Object>> backends =  sql """ select * from backends(); """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/"
    def table_name = "comp"

        // cluster mode need to make sure all be has this data
        def outFilePath="/"
        def transFile01="${dataFilePath}/comp.orc"
        def transFile02="${dataFilePath}/comp.parquet"
        for (List<Object> backend : backends) {
            def be_host = backend[1]
            scpFiles ("root", be_host, transFile01, outFilePath, false);
            scpFiles ("root", be_host, transFile02, outFilePath, false);
        }

    qt_sql """ADMIN SET FRONTEND CONFIG ('disable_nested_complex_type' = 'false')"""

    // create doris table
    qt_sql """
             CREATE TABLE IF NOT EXISTS ${table_name} (
              `id` int(11) NULL,
              `m1` MAP<int(11),array<double>> NULL,
              `m2` MAP<text,MAP<text,double>> NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            ); """

    qt_sql """
        select * from local(
            "file_path" = "${outFilePath}/comp.orc",
            "backend_id" = "${be_id}",
            "format" = "orc");"""

    qt_sql """
        insert into ${table_name} select * from local (
            "file_path" = "${outFilePath}/comp.orc",
            "backend_id" = "${be_id}",
             "format" = "orc");"""

    qt_sql """ select * from ${table_name} order by id; """

    qt_sql """
        select * from local(
            "file_path" = "${outFilePath}/comp.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"); """

    qt_sql """
        insert into ${table_name} select * from local(
            "file_path" = "${outFilePath}/comp.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"); """

    qt_sql_count """ select count(*) from ${table_name} """

    qt_sql """ select * from ${table_name} order by id"""

    qt_sql """ drop table ${table_name} """

}
