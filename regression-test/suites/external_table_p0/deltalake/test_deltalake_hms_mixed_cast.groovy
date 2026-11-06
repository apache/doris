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

import org.apache.doris.regression.action.ProfileAction

// The same HMS catalog must keep the CAST policy local to each table, including within one SQL statement.
suite("test_deltalake_hms_mixed_cast", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    sql "set enable_sql_cache=false"
    sql "set enable_profile=true"
    sql "set num_files_in_batch_mode=1"
    def profiles = new ProfileAction(context)

    for (String kind : ["iceberg", "hms"]) {
        String catalog = "test_deltalake_hms_mixed_cast_${kind}"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='${kind}', 'iceberg.catalog.type'='hms',
            'hive.metastore.uris'='thrift://${host}:${hmsPort}',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
    }

    // Reuse the retained Delta golden. Create two Iceberg files in that same HMS database.
    // Integral decimal values make this a safe CAST control for Iceberg's existing pushdown policy.
    sql "switch test_deltalake_hms_mixed_cast_iceberg"
    sql "use delta_golden"
    sql "drop table if exists test_deltalake_hms_mixed_cast_iceberg"
    sql """create table test_deltalake_hms_mixed_cast_iceberg (id int, amount decimal(18, 2))
        properties ('format-version'='2')"""
    sql "insert into test_deltalake_hms_mixed_cast_iceberg values (123, 123.00)"
    sql "insert into test_deltalake_hms_mixed_cast_iceberg values (999, 999.00)"
    order_qt_iceberg_fixture "select id, amount from test_deltalake_hms_mixed_cast_iceberg"
    qt_iceberg_files "select count(*) from test_deltalake_hms_mixed_cast_iceberg\$files where content = 0"

    sql "switch test_deltalake_hms_mixed_cast_hms"
    sql "use delta_golden"
    String deltaCast = "cast(decimal_decimal_same_scale as bigint) = 123"
    String icebergSelect = """select 'iceberg' as source, amount
        from test_deltalake_hms_mixed_cast_iceberg where cast(amount as bigint) = 123"""
    String deltaSelect = """select 'delta' as source, decimal_decimal_same_scale as amount
        from type_widening where ${deltaCast}"""

    for (boolean batch : [false, true]) {
        sql "set enable_external_table_batch_mode=${batch}"
        String mode = batch ? "stream" : "sync"
        for (String variant : ["delta_first", "iceberg_first", "plain_and"]) {
            String deltaQuery = variant == "plain_and"
                    ? "${deltaSelect} and decimal_decimal_same_scale < 124" : deltaSelect
            String mixed = variant == "iceberg_first" ? "${icebergSelect} union all ${deltaQuery}"
                    : "${deltaQuery} union all ${icebergSelect}"
            // There are no join-derived predicates here: the only possible Iceberg pushdown is its CAST.
            String plan = sql("explain verbose ${mixed}").collect { it[0].toString() }.join("\n")
            assertTrue(plan.contains("icebergPredicatePushdown="), plan)
            String token = UUID.randomUUID().toString()
            "order_qt_${mode}_${variant}"("select /* ${token} */ * from (${mixed}) mixed")
            String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
            int selected = variant == "plain_and" ? 1 : 2
            assertTrue(profile.contains("TotalFiles: 2") && profile.contains("SelectedFiles: ${selected}"), profile)
            if (!batch) {
                // Iceberg emits SDK scan metrics only for synchronous planning; EXPLAIN checks both modes.
                // A catalog-wide false would return the right rows but make Iceberg read both files.
                assertTrue(profile.contains("Iceberg Scan Metrics"), profile)
                assertTrue((profile =~ /(?m)^\s*(?:-\s*)?data_files:\s+1\s*$/).find(), profile)
            }
        }

        // Both scans in a real join also share the HMS catalog's statement metadata.
        "order_qt_${mode}_join_delta_first"("""select d.decimal_decimal_same_scale, i.id, i.amount
            from type_widening d join test_deltalake_hms_mixed_cast_iceberg i
                on cast(d.decimal_decimal_same_scale as bigint) = i.id
            where cast(d.decimal_decimal_same_scale as bigint) = 123 and cast(i.amount as bigint) = 123""")
        "order_qt_${mode}_join_iceberg_first"("""select d.decimal_decimal_same_scale, i.id, i.amount
            from test_deltalake_hms_mixed_cast_iceberg i join type_widening d
                on i.id = cast(d.decimal_decimal_same_scale as bigint)
            where cast(i.amount as bigint) = 123 and cast(d.decimal_decimal_same_scale as bigint) = 123""")
        "order_qt_${mode}_iceberg_after"(icebergSelect)
    }
}
