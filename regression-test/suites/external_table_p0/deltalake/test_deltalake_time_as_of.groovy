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

suite("test_deltalake_time_as_of", "p0,external,deltalake,external_docker") {
    // Run uc-managed.py --time-travel-edges NEW_DIR on the shared local filesystem first.
    String directory = context.config.otherConfigs.get("deltalakeTimeTravelFixture")
    if (!directory) {
        return
    }
    File root = new File(directory)
    def fixture = new JsonSlurper().parse(new File(root, "expected.json"))
    String uri = context.config.otherConfigs.get("deltalakeUcUri")
    String api = uri.replaceAll('/+$', '') + "/api/2.1/unity-catalog"
    def readUc = { String path ->
        new JsonSlurper().parse(new URL(api + path), [connectTimeout: 10000, readTimeout: 10000])
    }
    String name = fixture.uc.table
    assertTrue(name ==~ /uc_staged_schema_[0-9a-f]{12}/)
    def checkSources = {
        assertEquals("MANAGED", readUc("/tables/doris_delta.m1.${name}").table_type)
        def state = readUc("/delta/v1/catalogs/doris_delta/schemas/m1/tables/${name}")
        assertEquals(4, state["latest-table-version"])
        File log = new File(new File(new URI(state.metadata.location as String)), "_delta_log")
        assertFalse(new File(log, "_last_checkpoint").exists())
        [3, 4].each { v ->
            def commit = state.commits.find { it.version == v }
            assertNotNull(commit, "Missing UC commit ${v}")
            assertEquals(fixture.uc.staged_files[v.toString()], commit["file-name"])
            assertTrue(new File(log, "_staged_commits/${commit['file-name']}").isFile())
            String prefix = String.format("%020d", v)
            assertFalse(new File(log, "${prefix}.json").exists(), "Version ${v} was backfilled")
            assertFalse(new File(log, "${prefix}.crc").exists())
        }
        File data = new File(root, "warehouse/edges/retention")
        assertFalse(new File(data, fixture.retention.retired as String).exists())
        assertTrue(new File(data, fixture.retention.retained as String).isFile())
        assertTrue(new File(data, "_delta_log/00000000000000000001.json").isFile())
        File checkpoint = new File(root, "warehouse/edges/checkpoint/_delta_log")
        assertTrue(new File(checkpoint, "00000000000000000002.checkpoint.parquet").isFile())
        assertFalse(new File(checkpoint, "00000000000000000001.json").exists())
        File transition = new File(root, "warehouse/edges/transition/_delta_log")
        fixture.transition.file_mtime_ms.each { ver, ms ->
            File numbered = new File(transition, String.format("%020d.json", ver.toLong()))
            assertEquals(ms.toLong(), numbered.lastModified(), numbered.name)
        }
    }
    def at = { one ->
        if (one.kind == "version") { return "for version as of ${one.value.toLong()}" }
        if (one.kind == "millis") { return "for time as of ${one.value.toLong()}" }
        assertEquals("literal", one.kind)
        return "for time as of '" + one.value.replace("'", "''") + "'"
    }
    def schemaOf = { meta ->
        (1..meta.columnCount).collect { i ->
            String type = meta.getColumnType(i) in [Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR]
                    ? "string" : meta.getColumnTypeName(i).toLowerCase(Locale.ROOT)
            [meta.getColumnLabel(i).tokenize('.').last(), type]
        }
    }
    checkSources()
    assertEquals(["uc", "transition", "dst", "checkpoint"] as Set, fixture.cases*.source as Set)
    sql "set enable_sql_cache=false"
    sql "set num_files_in_batch_mode=1"
    for (boolean streaming : [false, true]) {
        String mode = streaming ? "stream" : "sync"
        String fs = "test_delta_time_fs_${mode}"
        String uc = "test_delta_time_uc_${mode}"
        sql "drop catalog if exists ${fs}"
        sql "drop catalog if exists ${uc}"
        sql """create catalog ${fs} properties('type'='deltalake', 'deltalake.catalog.type'='filesystem',
            'warehouse'='${new File(root, "warehouse").toURI()}')"""
        sql """create catalog ${uc} properties('type'='deltalake', 'deltalake.catalog.type'='uc',
            'deltalake.uc.uri'='${uri}', 'deltalake.uc.catalog'='doris_delta')"""
        String managed = "${uc}.m1.${name}"
        sql "set enable_external_table_batch_mode=${streaming}"
        fixture.cases.groupBy { it.zone }.each { zone, cases ->
            sql "set time_zone='${zone}'"
            cases.each { one ->
                String table = one.source == "uc" ? managed : "${fs}.edges.`${one.source}`"
                String relation = "${table} ${at(one)}"
                String columns = one.nested ? "id, payload.renamed as val" : "*"
                String query = "select ${columns} from ${relation} order by id"
                def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), query)
                assertEquals(one.schema, schemaOf(meta), "${one.source}/${one.name}")
                assertSparkDorisResultEquals(one.rows, rows)
                // Enabling in-commit timestamps changes the snapshot even when no rows change.
                explain {
                    sql query
                    contains "Delta snapshot: ${one.version}"
                    if (one.source == "uc" && one.name == "old_version") {
                        if (streaming) { contains "(approximate)inputSplitNum=" }
                        else { notContains "(approximate)inputSplitNum=" }
                    }
                }
                if (one.name in ["old_version", "new_version", "before_switch", "at_switch", "spring_gap",
                                 "autumn_fold", "readded_field"]) {
                    assertEquals(one.rows.size().toLong(), sql("select count(*) from ${relation}")[0][0].toLong())
                    quickTest("${mode}_${one.source}_${one.name}", query)
                }
            }
        }
        // An explicit offset names an instant. Spark 4.0.1 / Delta 4.4.1 can collapse the
        // second 01:30 into the first in a New York session; compare to Spark's UTC epoch read.
        sql "set time_zone='America/New_York'"
        String second = "select id, label from ${fs}.edges.dst for time as of '${fixture.dst.second_occurrence_literal}' order by id"
        assertSparkDorisResultEquals(fixture.dst.spark_utc, sql(second))
        assertSparkDorisResultEquals(fixture.dst.spark_utc, sql("""select id, label from ${fs}.edges.dst
            for time as of ${fixture.dst.second_occurrence_epoch} order by id"""))
        quickTest("${mode}_second_fold_offset", second)

        sql "set time_zone='UTC'"
        [["time_old", "for time as of ${fixture.uc.mid}", "for version as of 4"],
         ["time_new", "for version as of 3", "for time as of ${fixture.uc.new_time}"]].each { tag, oldPin, newPin ->
            String joined = """select o.id, o.label, n.renamed_label from ${managed} ${oldPin} o
                join ${managed} ${newPin} n on n.id=o.id order by o.id"""
            assertSparkDorisResultEquals(fixture.uc.mixed[tag], sql(joined))
            quickTest("${mode}_join_${tag}", joined)
        }
        test {
            sql "select renamed_label from ${managed} for version as of 3"
            exception "Unknown column 'renamed_label'"
        }
        test {
            sql "select label from ${managed} for version as of 4"
            exception "Unknown column 'label'"
        }
        test {
            sql "select * from ${managed} for time as of '2026-02-30'"
            exception "2026-02-30"
        }
        test {
            sql "select * from ${fs}.edges.transition for time as of ${fixture.transition.before_earliest}"
            exception "before the earliest"
        }
        test {
            sql "select * from ${fs}.edges.transition for time as of ${fixture.transition.after_latest}"
            exception "after the latest"
        }
        test {
            sql "select id from ${fs}.edges.checkpoint for version as of 1"
            exception "The earliest available version is 2"
        }
        String nested = """select o.id, o.payload.renamed as old_val, n.payload.renamed as new_val
            from ${fs}.edges.checkpoint for time as of ${fixture.checkpoint.t3} o
            left join ${fs}.edges.checkpoint for version as of 6 n on n.id=o.id order by o.id"""
        def (nestedRows, nestedMeta) = JdbcUtils.executeToList(context.getConnection(), nested)
        assertEquals(fixture.checkpoint.mixed.schema, schemaOf(nestedMeta))
        assertSparkDorisResultEquals(fixture.checkpoint.mixed.rows, nestedRows)
        quickTest("${mode}_nested_mixed", nested)
        for (String pin : ["for version as of 1", "for time as of ${fixture.retention.v1_ms}"]) {
            test {
                sql "select id, label from ${fs}.edges.retention ${pin} order by id"
                exception fixture.retention.retired
            }
        }
        // Spark's default age guard rejects v2. Kernel can still scan the retained v2 file.
        for (String pin : ["for version as of 2", "for time as of ${fixture.retention.v2_ms}"]) {
            def rows = sql("select id, label from ${fs}.edges.retention ${pin} order by id")
            assertSparkDorisResultEquals(fixture.retention.retained_rows, rows)
        }
        String latest = "select id, label from ${fs}.edges.retention order by id"
        assertSparkDorisResultEquals(fixture.retention.latest.rows, sql(latest))
        quickTest("${mode}_after_vacuum", latest)
        logger.info("Compared ${fixture.cases.size()} historical selectors in ${mode} mode")
    }
    checkSources()
}
