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

// Regression test for: LogicalView.computeOutput() IndexOutOfBoundsException when
// an underlying Hive table gains new columns (schema drift) after the view was created.
//
// Repro:
//   1. Create Hive table with N columns; create a Doris view (SELECT *) on it.
//   2. ADD COLUMN to the Hive table.
//   3. REFRESH TABLE base_table in Doris (view schema not refreshed yet).
//   4. Query the view → used to crash with "Index 3 out of bounds for length 3"
//      because LogicalView.computeOutput() iterated childOutput (4 slots from
//      re-analyzed view body) but called view.getFullSchema().get(i) on a
//      3-element list (view's schema at creation time).

suite("test_hive_view_schema_drift", "p0,external,hive_docker") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "test_${hivePrefix}_view_schema_drift"
        String db = "test_view_schema_drift_db"
        String base_table = "test_view_schema_drift_base"
        String view_name = "test_view_schema_drift_view"

        try {
            // ---- Setup Hive catalog ----
            sql """drop catalog if exists ${catalog_name}"""
            sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'hadoop.username' = 'hive'
            )"""

            // ---- Create Hive database and base table with 3 columns ----
            hive_docker """drop database if exists ${db} cascade"""
            hive_docker """create database ${db}"""
            hive_docker """
                create table ${db}.${base_table} (
                    id     bigint,
                    name   string,
                    age    string
                )
                partitioned by (dt string)
                stored as parquet
            """

            // ---- Create Doris view on the Hive table ----
            sql """switch ${catalog_name}"""
            sql """use ${db}"""
            sql """create view ${view_name} as select * from ${base_table} where 1=1"""

            // ---- Baseline: view query must succeed (3 non-partition columns) ----
            def beforeDrift = sql """select * from ${view_name} where 1=0"""
            assertTrue(beforeDrift.isEmpty())

            // ---- Schema drift: add a column to the Hive base table ----
            hive_docker """alter table ${db}.${base_table} add columns (score string comment 'new col')"""

            // ---- Refresh the base table metadata in Doris (view schema NOT refreshed) ----
            sql """refresh table ${base_table}"""

            // ---- Base table now has 4 columns ----
            def descBase = sql """desc ${base_table}"""
            assertEquals(4, descBase.size())

            // ---- Querying the view must NOT throw IndexOutOfBoundsException ----
            // Before the fix: errCode = 2, detailMessage = Index 3 out of bounds for length 3
            def afterDrift = sql """select * from ${view_name} where 1=0"""
            assertTrue(afterDrift.isEmpty())

        } finally {
            try {
                sql """switch ${catalog_name}"""
                sql """drop view if exists ${catalog_name}.${db}.${view_name}"""
            } catch (Exception ignored) {}
            try {
                hive_docker """drop database if exists ${db} cascade"""
            } catch (Exception ignored) {}
            sql """drop catalog if exists ${catalog_name}"""
        }
    }
}
