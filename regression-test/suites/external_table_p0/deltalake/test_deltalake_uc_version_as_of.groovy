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

import groovy.json.JsonSlurper
import org.apache.doris.regression.util.JdbcUtils

import java.sql.Types

suite("test_deltalake_uc_version_as_of", "p0,external,deltalake,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeUcTest"))) {
        return
    }
    // uc-managed.py --history prepares the Spark reference on the shared file:// warehouse.
    String uri = context.config.otherConfigs.get("deltalakeUcUri")
    String api = uri.replaceAll('/+$', '') + "/api/2.1/unity-catalog"
    def readUc = { String path ->
        new JsonSlurper().parse(new URL(api + path), [connectTimeout: 10000, readTimeout: 10000])
    }
    def checkBackfill = {
        def state = readUc("/delta/v1/catalogs/doris_delta/schemas/m1/tables/uc_managed_history")
        assertEquals(4, state["latest-table-version"])
        File root = new File(new URI(state.metadata.location as String))
        File log = new File(root, "_delta_log")
        assertFalse(new File(log, "_last_checkpoint").exists(), "Expected a managed table without checkpoints")
        for (int version : [3, 4]) {
            def commit = state.commits.find { it.version == version }
            assertNotNull(commit, "UC must retain staged version ${version}")
            File staged = new File(log, "_staged_commits/${commit['file-name']}")
            assertTrue(staged.isFile(), "Missing staged commit: ${staged}")
            String prefix = String.format("%020d", version)
            assertFalse(new File(log, "${prefix}.json").exists(), "Version ${version} was backfilled")
            assertFalse(new File(log, "${prefix}.crc").exists(), "Found a published CRC for version ${version}")
        }
        return root
    }
    assertEquals("MANAGED", readUc("/tables/doris_delta.m1.uc_managed_history").table_type)
    def expected = new JsonSlurper().parse(new File(checkBackfill(), "_doris_history_expected.json"))

    sql "set enable_sql_cache=false"
    sql "set num_files_in_batch_mode=1"
    for (boolean streaming : [false, true]) {
        String mode = streaming ? "stream" : "sync"
        String catalog = "test_deltalake_uc_version_as_of_${mode}"
        String table = "${catalog}.m1.uc_managed_history"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='uc',
            'deltalake.uc.uri'='${uri}', 'deltalake.uc.catalog'='doris_delta')"""
        sql "set enable_external_table_batch_mode=${streaming}"
        explain {
            sql "select id, label from ${table} for version as of 3 order by id"
            contains "Delta snapshot: 3"
            if (streaming) {
                contains "(approximate)inputSplitNum="
            } else {
                notContains "(approximate)inputSplitNum="
            }
        }

        for (String tag : ["3", "latest", "4", "2", "1", "0"]) {
            String relation = tag == "latest" ? table : "${table} for version as of ${tag}"
            String statement = "select * from ${relation} order by id"
            def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), statement)
            def schema = (1..meta.columnCount).collect { i ->
                String label = meta.getColumnLabel(i).tokenize('.').last()
                String type = meta.getColumnType(i) in [Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR]
                        ? "string" : meta.getColumnTypeName(i).toLowerCase(Locale.ROOT)
                [label, type]
            }
            def version = expected.versions[tag]
            assertEquals(version.schema, schema)
            assertSparkDorisResultEquals(version.rows, rows)
            assertEquals(version.count.toLong(), sql("select count(*) from ${relation}")[0][0].toLong())
            quickTest("${mode}_v_${tag}", statement)
        }
        String join = """select n.id from ${table} for version as of 4 n
            left join ${table} for version as of 3 o on n.id=o.id where o.id is null order by n.id"""
        assertSparkDorisResultEquals(expected.only_in_v4, sql(join))
        quickTest("${mode}_only_in_v4", join)
        test {
            sql "select * from ${table} for version as of 5"
            exception "Cannot load table version 5 as the latest version ratified by UC is 4"
        }
    }
    checkBackfill()
}
