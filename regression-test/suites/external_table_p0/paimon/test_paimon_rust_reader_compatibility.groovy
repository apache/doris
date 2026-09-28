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
                    "enable_file_scanner_v2", "enable_profile", "enable_prune_nested_column"]
    def saved = settings.collectEntries { [(it): sql("select @@${it}")[0][0]] }
    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        'type'='paimon', 'paimon.catalog.type'='filesystem',
        'warehouse'='s3://warehouse/wh', 's3.endpoint'='http://${endpoint}:${port}',
        's3.access_key'='admin', 's3.secret_key'='password',
        's3.region'='us-east-1', 'use_path_style'='true')"""
    sql "SWITCH ${catalog}"
    sql "DROP DATABASE IF EXISTS ${database} FORCE"
    sql "CREATE DATABASE ${database}"
    sql "USE ${database}"
    try {
        sql "set enable_profile=true"
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
            def profile = profiles.getProfile(queryId, ["FileScannerV2"])
            assertEquals(expected.toString(), actual.toString())
            assertEquals(rustExpected, profile.contains("PaimonRustReader"), query)
        }
        def createPk = { String name, String valueColumns, String extra ->
            // Separate commits must remain separate files so the reader performs the merge.
            sql """CREATE TABLE ${name} (id INT NOT NULL, ${valueColumns}) ENGINE=paimon
                PROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true',
                    'deletion-vectors.enabled'='false' ${extra})"""
        }

        createPk("nested_values", "v STRUCT<a:INT,b:INT>, arr ARRAY<STRUCT<a:INT,b:INT>>, "
                + "m MAP<INT,STRUCT<a:INT,b:INT>>", "")
        sql """INSERT INTO nested_values VALUES (1, named_struct('a',11,'b',22),
            [named_struct('a',33,'b',44)], map(1,named_struct('a',55,'b',66)))"""
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

        createPk("supported_sum", "v DOUBLE", ", 'merge-engine'='aggregation', "
                + "'fields.v.aggregate-function'='sum'")
        sql "INSERT INTO supported_sum VALUES (1,10.0)"
        check("select v from supported_sum", [[10.0]], true)
        sql "INSERT INTO supported_sum VALUES (1,3.0)"
        check("select v from supported_sum", [[13.0]], false)

        [false, true].each { decimal ->
            [false, true].each { override ->
                def name = "partial_default_${decimal}_${override}"
                def type = decimal ? "DECIMAL(2,0)" : "TINYINT"
                def first = decimal ? 99 : 127
                def extra = ", 'merge-engine'='partial-update', 'fields.seq.sequence-group'='v', "
                        + "'fields.default-aggregate-function'='sum'"
                if (override) {
                    extra += ", 'fields.v.aggregate-function'='max'"
                }
                createPk(name, "v ${type}, seq INT", extra)
                sql "INSERT INTO ${name} VALUES (1,${first},1)"
                // The single-file control distinguishes aggregate fallback from merge fallback.
                check("select cast(v as string) from ${name}", [[first.toString()]], override)
                sql "INSERT INTO ${name} VALUES (1,1,2)"
                if (decimal) {
                    sql "INSERT INTO ${name} VALUES (1,-1,3)"
                }
                def expected = decimal ? "99" : override ? "127" : "-128"
                check("select cast(v as string) from ${name}", [[expected]], false)
            }
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
        saved.each { name, value -> sql "set ${name}=${value}" }
        sql "DROP DATABASE IF EXISTS ${database} FORCE"
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
