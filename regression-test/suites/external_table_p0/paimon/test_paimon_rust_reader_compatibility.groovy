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

suite("test_paimon_rust_reader_compatibility", "p0,external,paimon") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        return
    }
    def endpoint = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("iceberg_minio_port")
    def catalog = "test_paimon_rust_compatibility"
    def database = "test_paimon_rust_compatibility_db"
    def settings = ["enable_paimon_rust_reader", "force_jni_scanner",
                    "enable_file_scanner_v2", "enable_profile", "enable_prune_nested_column",
                    "enable_push_down_no_group_agg", "time_zone"]
    def saved = settings.collectEntries { [(it): sql("select @@${it}")[0][0]] }
    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        'type'='paimon', 'paimon.catalog.type'='filesystem', 'fs.s3.support'='true',
        'warehouse'='s3://warehouse/wh', 's3.endpoint'='http://${endpoint}:${port}',
        's3.access_key'='admin', 's3.secret_key'='password',
        's3.region'='us-east-1', 'use_path_style'='true')"""
    sql "SWITCH ${catalog}"
    sql "DROP DATABASE IF EXISTS ${database} FORCE"
    sql "CREATE DATABASE ${database}"
    sql "USE ${database}"
    try {
        sql "set enable_profile=true"
        sql "set time_zone='+00:00'"
        sql "set enable_prune_nested_column=true"
        sql "set enable_file_scanner_v2=true"
        sql "set force_jni_scanner=true"
        def profiles = new ProfileAction(context)
        def check = { String query, List expected, boolean rustExpected ->
            sql "set enable_paimon_rust_reader=false"
            def baseline = sql(query)
            assertEquals(expected.toString(), baseline.toString())
            sql "set enable_paimon_rust_reader=true"
            def actual = sql(query)
            def queryId = sql("select last_query_id()")[0][0].toString()
            // A scanner can be published before its children; absence is meaningful only at completion.
            def required = ["FileScannerV2", "Profile Completion State: COMPLETE"]
            if (rustExpected) {
                required.add("PaimonRustReader")
            }
            def profile = profiles.getProfile(queryId, required)
            assertEquals(expected.toString(), actual.toString())
            assertEquals(rustExpected, profile.contains("PaimonRustReader"), query)
        }
        def createPk = { String name, String valueColumns, String extra ->
            // Separate commits must remain separate files so the reader performs the merge.
            sql """CREATE TABLE ${name} (id INT NOT NULL, ${valueColumns}) ENGINE=paimon
                PROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true',
                    'deletion-vectors.enabled'='false' ${extra})"""
        }

        createPk("credential_values", "v INT", "")
        sql "INSERT INTO credential_values VALUES (1,11)"
        check("select id,v from credential_values", [[1,11]], true)
        ["'s3.credentials_provider_type'='ANONYMOUS'"].eachWithIndex { extra, index ->
            def crossedCatalog = "${catalog}_credentials_${index}"
            sql "DROP CATALOG IF EXISTS ${crossedCatalog}"
            try {
                // Select S3 explicitly: endpoint guessing would choose MinIO and ignore S3 auth modes.
                // Static keys keep the catalog readable, while the conflicting provider mode must
                // conservatively keep the scan on JNI. Role ARN and session-token conflicts are
                // selector unit tests: on master they change the catalog's effective credentials,
                // so deliberately invalid values cannot form a readable regression fixture.
                sql """CREATE CATALOG ${crossedCatalog} PROPERTIES (
                    'type'='paimon', 'paimon.catalog.type'='filesystem', 'fs.s3.support'='true',
                    'warehouse'='s3://warehouse/wh', 's3.endpoint'='http://${endpoint}:${port}',
                    's3.access_key'='admin', 's3.secret_key'='password',
                    's3.region'='us-east-1', 'use_path_style'='true', ${extra})"""
                check("select id,v from ${crossedCatalog}.${database}.credential_values", [[1,11]], false)
            } finally {
                sql "DROP CATALOG IF EXISTS ${crossedCatalog}"
            }
        }

        createPk("branch_values", "v INT", "")
        sql "INSERT INTO branch_values VALUES (1,11)"
        spark_paimon """CALL paimon.sys.create_tag(table => '${database}.branch_values', tag => 'audit_tag')"""
        spark_paimon """CALL paimon.sys.create_branch(table => '${database}.branch_values',
            branch => 'audit', tag => 'audit_tag')"""
        // Master resolves a branch as its own table and binds its latest snapshot. The Rust
        // reader consumes that resolved table and split rather than re-evaluating the selector.
        check("select id,v from branch_values@branch(audit)", [[1,11]], true)

        sql """CREATE TABLE footer_aggregates (v INT NULL) ENGINE=paimon
            PROPERTIES ('bucket'='-1', 'file.format'='parquet')"""
        sql "INSERT INTO footer_aggregates VALUES (NULL), (-3), (7), (NULL), (2), (5)"
        def aggregateQueries = [
            ["select count(v) from footer_aggregates", [[4]], "COUNT"],
            ["select min(v), max(v) from footer_aggregates", [[-3,7]], "MINMAX"]]
        def rawRows = { String profile ->
            def values = (profile =~ /RawRowsRead: ([0-9]+)/).collect { it[1] as long }
            assertFalse(values.isEmpty(), "missing Parquet RawRowsRead counter")
            values
        }
        // Rust only replaces logical JNI splits. Native raw-file splits must retain footer
        // aggregation with Rust enabled; result equality alone would hide a full-scan regression.
        sql "set force_jni_scanner=false"
        aggregateQueries.each { entry ->
            [false, true].each { rustEnabled ->
                sql "set enable_paimon_rust_reader=${rustEnabled}"
                [false, true].each { pushdown ->
                    sql "set enable_push_down_no_group_agg=${pushdown}"
                    explain {
                        sql(entry[0])
                        contains "pushdown agg=${pushdown ? entry[2] : 'NONE'}"
                        contains "paimonNativeReadSplits="
                    }
                    assertEquals(entry[1].toString(), sql(entry[0]).toString())
                    def queryId = sql("select last_query_id()")[0][0].toString()
                    def profile = profiles.getProfile(queryId, ["FileScannerV2", "ParquetReader", "Profile Completion State: COMPLETE"])
                    assertFalse(profile.contains("PaimonRustReader"))
                    def rows = rawRows(profile)
                    if (pushdown) {
                        assertTrue(rows.every { it == 0 }, "footer aggregate must not read data rows")
                    } else {
                        assertTrue(rows.any { it > 0 }, "full-scan control must read data rows")
                    }
                }
            }
        }
        sql "set enable_push_down_no_group_agg=true"
        sql "set force_jni_scanner=true"
        // Forced logical splits still return real rows to the upper aggregate in both readers.
        aggregateQueries.each { entry -> check(entry[0], entry[1], true) }

        ["DATE", "DATETIME(3)"].eachWithIndex { target, index ->
            def name = "string_to_temporal_${index}"
            sql """CREATE TABLE ${name} (id INT, v STRING) ENGINE=paimon
                PROPERTIES ('bucket'='-1', 'file.format'='parquet')"""
            sql "INSERT INTO ${name} VALUES (1,'42'), (2,NULL)"
            check("select id,v from ${name} order by id", [[1,"42"],[2,null]], true)
            sql "ALTER TABLE ${name} MODIFY COLUMN v ${target} NULL"
            // The historical string is an epoch count in Java, not an Arrow calendar literal.
            check("select id,cast(v as string) from ${name} order by id",
                    [[1,index == 0 ? "1970-02-12" : "1970-01-01 00:00:00.042"],[2,null]], false)
        }
        ["INT", "BIGINT"].each { sourceType ->
            [0, 3, 6].each { precision ->
                def name = "integer_to_timestamp_${sourceType.toLowerCase()}_${precision}"
                sql """CREATE TABLE ${name} (id INT, v ${sourceType}) ENGINE=paimon
                    PROPERTIES ('bucket'='-1', 'file.format'='parquet')"""
                sql "INSERT INTO ${name} VALUES (1,1700000000), (2,NULL)"
                sql "ALTER TABLE ${name} MODIFY COLUMN v DATETIME(${precision}) NULL"
                // All target precisions must interpret the source number as epoch seconds.
                check("select id,date_format(v,'%Y-%m-%d %H:%i:%s') from ${name} order by id",
                        [[1,"2023-11-14 22:13:20"],[2,null]], false)
            }
        }

        createPk("nested_values", "v STRUCT<a:INT,b:INT>, arr ARRAY<STRUCT<a:INT,b:INT>>, "
                + "m MAP<INT,STRUCT<a:INT,b:INT>>", "")
        // Bracket array literals accept constants only; nested expressions need the array constructor.
        sql """INSERT INTO nested_values VALUES (1, named_struct('a',11,'b',22),
            array(named_struct('a',33,'b',44)), map(1,named_struct('a',55,'b',66)))"""
        check("select id from nested_values", [[1]], true)
        // Distinct siblings expose ordinal decoding of a pruned second child.
        check("select struct_element(v,'b') from nested_values", [[22]], false)
        check("select struct_element(arr[1],'b') from nested_values", [[44]], false)
        check("select struct_element(m[1],'b') from nested_values", [[66]], false)

        createPk("narrow_values", "v BIGINT NULL", "")
        sql "INSERT INTO narrow_values VALUES (1,383), (2,-129), (3,NULL)"
        check("select id,v from narrow_values order by id", [[1,383],[2,-129],[3,null]], true)
        sql "ALTER TABLE narrow_values MODIFY COLUMN v TINYINT NULL"
        check("select id,v from narrow_values order by id", [[1,127],[2,127],[3,null]], false)
        // A narrowing predicate must also be evaluated after the Java-compatible cast.
        check("select id,v from narrow_values where v=127 order by id", [[1,127],[2,127]], false)

        def integerCases = [
            ["TINYINT", "127", "1", "-128", "64", "2", "-128"],
            ["SMALLINT", "32767", "1", "-32768", "16384", "2", "-32768"],
            ["INT", "2147483647", "1", "-2147483648", "1073741824", "2", "-2147483648"],
            ["BIGINT", "9223372036854775807", "1", "-9223372036854775808",
                "4611686018427387904", "2", "-9223372036854775808"]]
        integerCases.eachWithIndex { c, index ->
            ["sum", "product"].each { function ->
                def name = "integer_${function}_${index}"
                int offset = function == "sum" ? 1 : 4
                createPk(name, "v ${c[0]}", ", 'merge-engine'='aggregation', "
                        + "'fields.v.aggregate-function'='${function}'")
                sql "INSERT INTO ${name} VALUES (1,${c[offset]})"
                sql "INSERT INTO ${name} VALUES (1,${c[offset+1]})"
                check("select cast(v as string) from ${name}", [[c[offset+2]]], false)
            }
        }
        createPk("decimal_sum", "v DECIMAL(2,0)", ", 'merge-engine'='aggregation', "
                + "'fields.v.aggregate-function'='sum'")
        [99, 1, -1].each { sql "INSERT INTO decimal_sum VALUES (1,${it})" }
        check("select cast(v as string) from decimal_sum", [["99"]], false)

        createPk("collected_values", "v ARRAY<INT>", ", 'merge-engine'='aggregation', "
                + "'fields.v.aggregate-function'='collect'")
        sql "INSERT INTO collected_values VALUES (1,[11])"
        sql "INSERT INTO collected_values VALUES (1,[22])"
        check("select element_at(array_sort(v),1), element_at(array_sort(v),2) from collected_values",
                [[11,22]], false)

        // Safe single-file merge engines remain positive controls for the Rust route.
        ["deduplicate", "first-row"].eachWithIndex { engine, index ->
            def name = "single_file_merge_${index}"
            createPk(name, "v INT", ", 'merge-engine'='${engine}'")
            sql "INSERT INTO ${name} VALUES (1,11)"
            if (engine == "first-row") {
                // Paimon hides first-row level-0 files until compaction has deduplicated them.
                spark_paimon """CALL paimon.sys.compact(
                    table => '${database}.${name}', compact_strategy => 'full')"""
            }
            check("select id,v from ${name} order by id", [[1,11]], true)
        }

        createPk("supported_sum", "v DOUBLE", ", 'merge-engine'='aggregation', "
                + "'fields.v.aggregate-function'='sum'")
        sql "INSERT INTO supported_sum VALUES (1,10.0)"
        // A single physical file can contain multiple versions from older Rust writers.
        check("select v from supported_sum", [[10.0]], false)
        sql "INSERT INTO supported_sum VALUES (1,3.0)"
        check("select v from supported_sum", [[13.0]], false)

        [false, true].each { decimal ->
            [false, true].each { override ->
                def name = "partial_default_${decimal}_${override}"
                def type = decimal ? "DECIMAL(2,0)" : "TINYINT"
                def first = decimal ? 99 : 127
                // Keep the trailing operator so Groovy continues the assignment across lines.
                def extra = ", 'merge-engine'='partial-update', 'fields.seq.sequence-group'='v', " +
                        "'fields.default-aggregate-function'='sum'"
                if (override) {
                    extra += ", 'fields.v.aggregate-function'='max'"
                }
                createPk(name, "v ${type}, seq INT", extra)
                sql "INSERT INTO ${name} VALUES (1,${first},1)"
                // The reader must reject these engines even before a second file is written.
                check("select cast(v as string) from ${name}", [[first.toString()]], false)
                sql "INSERT INTO ${name} VALUES (1,1,2)"
                if (decimal) {
                    sql "INSERT INTO ${name} VALUES (1,-1,3)"
                }
                def expected = decimal ? "99" : override ? "127" : "-128"
                check("select cast(v as string) from ${name}", [[expected]], false)
            }
        }

        ["aggregation", "partial-update"].eachWithIndex { engine, index ->
            def name = "unused_default_${index}"
            def extra = ", 'merge-engine'='${engine}', 'fields.default-aggregate-function'='collect', " +
                    "'fields.v.aggregate-function'='max'"
            boolean partial = engine == "partial-update"
            if (partial) {
                extra += ", 'fields.seq.sequence-group'='v'"
            }
            createPk(name, partial ? "v INT, seq INT" : "v INT", extra)
            sql "INSERT INTO ${name} VALUES (1,7${partial ? ',1' : ''})"
            // An unused Java SPI default still fails Rust's global function-name validation.
            check("select v from ${name}", [[7]], false)
        }

        createPk("insert_only_runs", "v STRING", ", 'read.batch-size'='1'")
        3.times { run ->
            sql """INSERT INTO insert_only_runs
                SELECT number, concat('${run}', repeat('x',4096)) FROM numbers('number'='1025')"""
        }
        // Overlapping INSERT-only files have no tombstones, but Rust retains losing batches.
        check("select count(*), min(length(v)), max(length(v)) from insert_only_runs "
                + "where substring(v,1,1)='2'", [[1025,4097,4097]], false)

        createPk("retract_sum", "v DOUBLE, kind STRING", ", 'merge-engine'='aggregation', "
                + "'fields.v.aggregate-function'='sum', 'rowkind.field'='kind'")
        sql "INSERT INTO retract_sum VALUES (1,10.0,'+I')"
        sql "INSERT INTO retract_sum VALUES (1,3.0,'-D')"
        check("select id,v from retract_sum", [[1,7.0]], false)

        createPk("deleted_prefix", "v STRING, kind STRING", ", 'rowkind.field'='kind'")
        sql """INSERT INTO deleted_prefix SELECT number, repeat('x',128), '+I'
            FROM numbers('number'='4096')"""
        sql """INSERT INTO deleted_prefix SELECT number, repeat('x',128), '-D'
            FROM numbers('number'='4096')"""
        // A scan-producing predicate avoids metadata COUNT shortcuts for an empty result.
        check("select id from deleted_prefix where v is not null", [], false)
        createPk("sparse_prefix", "v STRING, kind STRING", ", 'rowkind.field'='kind'")
        sql """INSERT INTO sparse_prefix SELECT number, repeat('x',128), '+I'
            FROM numbers('number'='4097')"""
        sql """INSERT INTO sparse_prefix SELECT number, repeat('x',128), '-D'
            FROM numbers('number'='4096')"""
        check("select id from sparse_prefix order by id", [[4096]], false)
    } finally {
        saved.each { name, value -> sql "set ${name}='${value}'" }
        sql "DROP DATABASE IF EXISTS ${database} FORCE"
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
