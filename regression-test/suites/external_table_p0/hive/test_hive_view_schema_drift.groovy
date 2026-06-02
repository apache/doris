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
// an underlying Hive table gains new columns (schema drift) after the Hive view was created.
//
// Repro:
//   1. Create a Hive base table (3 cols) and a native Hive VIEW on it.
//   2. In Doris, register the Hive catalog and query the external view — OK (3 cols).
//   3. ADD COLUMN to the Hive base table via hive_docker.
//   4. REFRESH TABLE <base_table> in Doris (view HMS schema NOT refreshed).
//   5. Query the external view again — used to crash:
//        errCode = 2, detailMessage = Index 3 out of bounds for length 3
//      because LogicalView.computeOutput() iterated childOutput (4 slots from the
//      re-analyzed view body) but called view.getFullSchema().get(i) on a 3-element
//      list (the Hive view's HMS schema at creation time).
//
// The fix: use Math.min(childOutput.size(), fullSchema.size()) as the loop bound,
// preserving the view's declared output contract while preventing the crash.

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
        String hive_view = "test_view_schema_drift_view"

        try {
            // ---- Register Hive catalog in Doris ----
            sql """drop catalog if exists ${catalog_name}"""
            sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'hadoop.username' = 'hive'
            )"""

            // ---- Create Hive database, base table (3 cols), and a native Hive VIEW ----
            // The view is created through hive_docker so it is a native Hive view
            // (ExternalView in Doris). Its HMS schema records exactly 3 columns.
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
            hive_docker """
                create view ${db}.${hive_view} as
                    select id, name, age from ${db}.${base_table}
            """

            sql """switch ${catalog_name}"""
            sql """use ${db}"""

            // ---- Baseline: query the Hive view (3 columns) ----
            def beforeDrift = sql """select * from ${hive_view} where 1=0"""
            assertTrue(beforeDrift.isEmpty(), "Expected empty result before schema drift")

            // ---- Schema drift: add a column to the Hive base table ----
            hive_docker """alter table ${db}.${base_table} add columns (score string comment 'new col')"""

            // ---- Refresh only the base table (view HMS schema is NOT refreshed) ----
            // After this, Doris re-analyzes the view body against the 4-column base table,
            // producing childOutput with 4 slots, while ExternalView.getFullSchema() still
            // returns 3 columns from the Hive metastore → IndexOutOfBoundsException before fix.
            sql """refresh table ${base_table}"""

            // ---- Base table now exposes 4 columns ----
            def descBase = sql """desc ${base_table}"""
            assertEquals(4, descBase.size(),
                    "Base table should have 4 columns after ADD COLUMN + REFRESH")

            // ---- Querying the external Hive view must NOT throw IndexOutOfBoundsException ----
            // The view's HMS schema still has 3 cols (view not refreshed), so the output
            // is truncated to the view's declared width (3 cols) — the new 'score' column
            // is not visible until the view itself is refreshed.
            def afterDrift = sql """select * from ${hive_view} where 1=0"""
            assertTrue(afterDrift.isEmpty(), "Expected empty result after schema drift (WHERE 1=0)")

        } finally {
            try {
                hive_docker """drop database if exists ${db} cascade"""
            } catch (Exception ignored) {}
            sql """drop catalog if exists ${catalog_name}"""
        }
    }
}
