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

import org.apache.doris.regression.util.JdbcUtils

import java.sql.DriverManager

suite("test_deltalake_column_statistics", "p0,external,deltalake,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    sql "set time_zone = 'Asia/Shanghai'"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("deltalakeMinioPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    String catalog = "test_deltalake_column_statistics"
    sql "drop catalog if exists ${catalog}"
    sql """create catalog ${catalog} properties (
        'type'='deltalake', 'warehouse'='${warehouse}', 'enable.mapping.timestamp_tz'='true',
        'enable.mapping.varbinary'='true', 's3.endpoint'='http://${host}:${port}',
        's3.region'='us-east-1', 's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
    )"""

    def catalogId = get_catalog_id(catalog)
    def statsKey = { String db, String name ->
        def dbId = get_database_id(catalog, db)
        def id = get_table_id(catalog, db, name)
        return "catalog_id='${catalogId}' and db_id='${dbId}' and tbl_id='${id}' and idx_id='-1' and part_id is null"
    }
    def collect = { String db, String name, String tag ->
        String table = "${catalog}.${db}.${name}"
        sql "analyze table ${table} with sync"
        // Render instant bounds in the session zone, regardless of the statistics worker's timezone.
        def bound = { String col ->
            name == "types" ? "if(col_id='ts', cast(cast(${col} as timestamptz(6)) as string), ${col})" : col
        }
        String stored = """select col_id, count, ndv, null_count, ${bound('min')}, ${bound('max')}
            from internal.__internal_schema.column_statistics where ${statsKey(db, name)} order by col_id"""
        quickTest(tag, stored)

        // SHOW includes job timestamps and query counters. Compare its logical counts to the persisted rows.
        def persisted = sql(stored).collectEntries { row -> [(row[0]): row[1..3].collect { it as long }] }
        def shown = sql("show column stats ${table}").collectEntries { row ->
            [(row[0]): row[2..4].collect { it as double as long }]
        }
        assertEquals(persisted, shown)
        def tableStats = sql("show table stats ${table}").first()
        assertEquals("MANUAL", tableStats[5])
        assertTrue(persisted.keySet().every { tableStats[4].contains("${name}:${it}") })
    }

    qt_scalar_schema "desc ${catalog}.delta_m1.types"
    qt_widened_schema "desc ${catalog}.delta_m1.widening"
    for (String name : ["types", "basic", "empty", "mapping_name", "mapping_id", "widening"]) {
        collect("delta_m1", name, name)
    }

    String golden = "${catalog}.delta_golden.dv_partitioned_with_checkpoint"
    sql "drop stats ${golden}"
    def estimates = {
        def plan = sql("explain optimized plan select col1 from ${golden} where col1=13")
                .collect { it[0] }.join("\n")
        def nodeRows = { String node ->
            def match = plan =~ /(?m)${node}[^\n]*?\bstats=([0-9.,]+)/
            assertTrue(match.find(), plan)
            return match.group(1).replace(",", "").toDouble()
        }
        [filter: nodeRows("PhysicalFilter"), scan: nodeRows("PhysicalFileScan")]
    }
    def before = estimates()
    collect("delta_golden", "dv_partitioned_with_checkpoint", "golden_dv")
    def after = estimates()
    def exact = sql("select count(*), count(distinct col1) from ${golden}").first()
    assertEquals(exact[0] as double, after.scan, 0.001d)
    assertEquals(after.scan / (exact[1] as double), after.filter, 0.001d)
    assertTrue(before.filter / before.scan > after.filter / after.scan,
            "Filter/scan row estimates before ${before}, after ${after}")
    logger.info("Delta equality filter/scan estimates before ${before}, after ${after}")

    String sparkPort = context.config.otherConfigs.get("deltalakeSparkPort")
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    def spark = DriverManager.getConnection("jdbc:hive2://${host}:${sparkPort}/default", "hadoop", "hadoop")
    try {
        def write = { String stmt -> JdbcUtils.executeToList(spark, stmt) }
        write("create database if not exists delta_colstats location '${warehouse}/delta_colstats'")
        write("drop table if exists delta_colstats.dv_changes")
        write("""create table delta_colstats.dv_changes(id bigint, v int) using delta
                tblproperties ('delta.enableDeletionVectors'='true')""")
        write("""insert into delta_colstats.dv_changes select /*+ COALESCE(1) */ * from values
                (1L,-100),(2L,cast(null as int)),(3L,10),(4L,10),(5L,20),(6L,null),(7L,999) as t(id,v)""")
        write("delete from delta_colstats.dv_changes where id in (1,2,7)")
        sql "refresh catalog ${catalog}"
        String table = "${catalog}.delta_colstats.dv_changes"
        String history = catalog + '.delta_colstats.`dv_changes$history`'
        qt_dv_created """select cast(operationMetrics['numDeletionVectorsAdded'] as int) > 0
                         from ${history} where operation = 'DELETE' order by version"""
        def verifyLogical = {
            def persisted = sql("""select count, ndv, null_count, min, max from internal.__internal_schema.column_statistics
                                   where ${statsKey("delta_colstats", "dv_changes")} and col_id='v'""").first()
            def actual = sql("""select count(*), count(distinct v), count(*) - count(v),
                                cast(min(v) as string), cast(max(v) as string) from ${table}""").first()
            assertEquals(actual, persisted)
            def visible = sql("show column stats ${table} (v)").first()
            assertEquals(persisted[3..4], visible[7..8])
        }
        collect("delta_colstats", "dv_changes", "after_delete")
        verifyLogical()
        write("insert into delta_colstats.dv_changes values (8L,42),(9L,null)")
        sql "refresh table ${table}"
        collect("delta_colstats", "dv_changes", "after_refresh")
        verifyLogical()
        // The latest column bounds exclude both extrema. The original snapshot must still return them.
        def first = sql("""select version, millisecond_timestamp(cast(timestamp as datetimev2(6)))
                          from ${history} where operation='WRITE' order by version limit 1""").first()
        def expected = write("""select id, v from delta_colstats.dv_changes version as of ${first[0]}
                               where v in (-100, 999) order by id""")[0]
        for (def selector : [version: "for version as of ${first[0]}", time: "for time as of ${first[1]}"]) {
            String query = "select id, v from ${table} ${selector.value} where v in (-100, 999) order by id"
            assertSparkDorisResultEquals(expected, sql(query))
            quickTest("history_after_analyze_${selector.key}", query)
        }
        test {
            sql "analyze table ${table} with sample percent 10 with sync"
            exception "doesn't support sample analyze"
        }
    } finally {
        spark.close()
    }
}
