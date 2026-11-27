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

import java.time.Instant

suite("test_deltalake_system_tables", "p0,external,deltalake,external_docker") {
    def cfg = context.config.otherConfigs
    boolean fs = "true".equalsIgnoreCase(cfg.get("enableDeltaLakeTest"))
    boolean uc = "true".equalsIgnoreCase(cfg.get("enableDeltaLakeUcTest"))
    if (!fs && !uc) {
        return
    }
    sql "set time_zone='UTC'"
    if (fs) {
        String warehouse = cfg.get("deltalakeWarehouse")
        String storage = warehouse.startsWith("file:") ? "" : """,
            's3.endpoint'='http://${cfg.get("externalEnvIp")}:${cfg.get("deltalakeMinioPort")}',
            's3.region'='us-east-1','s3.access_key'='admin','s3.secret_key'='password','use_path_style'='true'"""
        sql "drop catalog if exists test_deltalake_sys_fs"
        sql """create catalog test_deltalake_sys_fs properties (
            'type'='deltalake','deltalake.catalog.type'='filesystem','warehouse'='${warehouse}' ${storage})"""
        qt_detail_schema 'desc test_deltalake_sys_fs.delta_m1.`basic$detail`'
        qt_files_schema 'desc test_deltalake_sys_fs.delta_m1.`basic$files`'
        qt_properties_schema 'desc test_deltalake_sys_fs.delta_m1.`basic$properties`'
        qt_partitions_schema 'desc test_deltalake_sys_fs.delta_m1.`basic$partitions`'
        qt_legacy '''select format,name,minReaderVersion,minWriterVersion,to_json(tableFeatures)
                    from test_deltalake_sys_fs.delta_m1.`basic$detail`'''
        qt_legacy_commit_time '''select d.lastModified = (select h.timestamp
            from test_deltalake_sys_fs.delta_m1.`basic$history` h order by h.version desc limit 1)
            from test_deltalake_sys_fs.delta_m1.`basic$detail` d'''
        qt_empty '''select (select count(*) from test_deltalake_sys_fs.delta_m1.`empty$files`),
                           (select count(*) from test_deltalake_sys_fs.delta_m1.`empty$partitions`),
                           numFiles,sizeInBytes from test_deltalake_sys_fs.delta_m1.`empty$detail`'''
        for (boolean batch in [false, true]) {
            sql "set enable_external_table_batch_mode=${batch}"
            quickTest("residual_count_" + batch, '''select count(*) from test_deltalake_sys_fs.delta_m1.`basic$files`
                where length(regexp_extract(path, '/part=a/', 0)) > 0''')
        }
    }
    if (uc) {
        sql "drop catalog if exists test_deltalake_sys_uc"
        sql """create catalog test_deltalake_sys_uc properties ('type'='deltalake',
            'deltalake.catalog.type'='uc','deltalake.uc.uri'='${cfg.get("deltalakeUcUri")}',
            'deltalake.uc.catalog'='doris_delta')"""
        qt_uc_unpartitioned '''select name, to_json(partitionColumns),
            (select count(*) from test_deltalake_sys_uc.m1.`uc_managed$partitions`)
            from test_deltalake_sys_uc.m1.`uc_managed$detail`'''
    }

    String fixture = cfg.get("deltalakeSystemTablesFixture")
    if (!fixture) {
        logger.info("Set deltalakeSystemTablesFixture to the reference from system_tables.py for Spark snapshot comparisons")
        return
    }
    def parser = new JsonSlurper()
    def reference = parser.parse(new File(fixture))
    def json = { value -> value == null ? null : parser.parseText(value.toString()) }
    def longValue = { value -> value == null ? null : value.toLong() }
    def canonical = { path -> path == null ? null : path.toString().replaceFirst('^s3a://', 's3://') }
    def byPartition = { rows -> rows.collectEntries { r -> [(new TreeMap(r[0])): r.drop(1).collect(longValue)] } }
    def kinds = ["detail", "files", "properties", "partitions"]
    for (String source in ["filesystem", "uc"]) {
        if ((source == "filesystem" && !fs) || (source == "uc" && !uc)) {
            continue
        }
        def ref = reference[source]
        assertNotNull(ref)
        String catalog = source == "uc" ? "test_deltalake_sys_uc" : "test_deltalake_sys_fs"
        String prefix = catalog + "." + ref.database + "."
        String base = prefix + ref.table
        def view = { kind -> prefix + "`" + ref.table + '$' + kind + "`" }
        def last = ref.versions.last()
        if (source == "uc") {
            String api = cfg.get("deltalakeUcUri") + "/api/2.1/unity-catalog/delta/v1/catalogs/doris_delta/schemas/"
            def window = parser.parse(new URL(api + ref.database + "/tables/" + ref.table))
            def staged = window.commits.find { it.version == last.version }
            assertNotNull(staged)
            File log = new File(new File(new URI(window.metadata.location)), "_delta_log")
            assertTrue(new File(log, "_staged_commits/" + staged["file-name"]).isFile())
            assertFalse(new File(log, String.format("%020d.json", last.version.toLong())).exists())
            assertFalse(new File(log, String.format("%020d.crc", last.version.toLong())).exists())
        }
        for (def snap in ref.versions) {
            String ts = Instant.ofEpochMilli(snap.timestamp.toLong()).toString()
            def selectors = [" for version as of ${snap.version}", " for time as of '${ts}'"]
            if (snap.version == last.version) {
                selectors << ""
            }
            for (String asOf in selectors) {
                def r = sql("""select format,id,name,description,location,
                    millisecond_timestamp(cast(createdAt as datetimev2(6))),
                    millisecond_timestamp(cast(lastModified as datetimev2(6))),
                    to_json(partitionColumns),to_json(clusteringColumns),numFiles,sizeInBytes,to_json(properties),
                    minReaderVersion,minWriterVersion,to_json(tableFeatures) from ${view("detail")}${asOf}""").first().toList()
                [5, 6, 9, 10, 12, 13].each { i -> r[i] = longValue(r[i]) }
                [7, 8, 11, 14].each { i -> r[i] = json(r[i]) }
                def d = snap.detail
                // Doris uses the history instant and explicit protocol features. Spark DETAIL also
                // reports the log object's modification time and can synthesize legacy features.
                def expected = [d.format, d.id, d.name, d.description, d.location, longValue(d.createdAt),
                    longValue(snap.timestamp), d.partitionColumns, d.clusteringColumns, longValue(d.numFiles),
                    longValue(d.sizeInBytes), d.properties, longValue(d.minReaderVersion),
                    longValue(d.minWriterVersion), snap.explicitFeatures]
                assertEquals(expected, r)
                logger.info("${source} v${snap.version} Spark DETAIL lastModified=${d.lastModified}, history=${r[6]}")
                def files = sql("""select path,size,millisecond_timestamp(cast(modification_time as datetimev2(6))),
                    to_json(partition_values),num_records,has_deletion_vector,deletion_vector_cardinality,
                    deletion_vector_path,deletion_vector_storage_type from ${view("files")}${asOf} order by path""").collect { f ->
                    [f[0], longValue(f[1]), longValue(f[2]), json(f[3]), longValue(f[4]),
                     f[5] instanceof Number ? f[5].intValue() != 0 : f[5], longValue(f[6]), f[7], f[8]]
                }
                def expectedFiles = snap.files.collect { f ->
                    [canonical(f[0]), longValue(f[1]), longValue(f[2]), f[3], longValue(f[4]),
                     f[5], longValue(f[6]), canonical(f[7]), f[8]]
                }.sort { it[0] }
                assertEquals(expectedFiles, files)
                def props = sql("select `key`,`value` from ${view('properties')}${asOf} order by `key`")
                assertEquals(new TreeMap(d.properties).collect { k, v -> [k, v] }, props)
                def parts = sql("""select to_json(`partition`),file_count,total_size,record_count,deleted_record_count
                    from ${view("partitions")}${asOf}""").collect { p -> [json(p[0])] + p.drop(1) }
                assertEquals(byPartition(snap.partitions), byPartition(parts))
                [detail: 1, files: files.size(), properties: props.size(), partitions: parts.size()].each { kind, n ->
                    assertEquals(n.toLong(), sql("select count(*) from ${view(kind)}${asOf}")[0][0].toLong())
                }
            }
        }
        def before = ref.versions[1]
        String initial = " for version as of " + ref.versions.first().version
        String old = " for time as of '" + Instant.ofEpochMilli(before.timestamp.toLong()) + "'"
        explain {
            sql "select path from ${view('files')}${old}"
            contains "Delta files at version: ${before.version}"
        }
        quickTest(source + "_mixed", """select (select count(*) from ${base}${old}), d.numFiles,
            (select count(*) from ${view("files")}${old}) = (select numFiles from ${view("detail")}${old}),
            (select `value` from ${view("properties")} where `key`='stage'),
            (select count(*) from ${view("partitions")}) from ${view("detail")}${initial} d""")
        quickTest(source + "_projection", """select `value`,`key`,`value` from ${view("properties")}${old}
            where `key`='stage' order by `key`""")

        String user = "delta_sys_table_reader_" + source
        String pwd = "Stage2Test_0123"
        sql "drop user if exists '${user}'@'%'"
        sql "create user '${user}'@'%' identified by '${pwd}'"
        if (isCloudMode()) {
            def cluster = sql("show clusters")[0][0]
            sql "grant usage_priv on cluster `${cluster}` to '${user}'@'%'"
        }
        sql "create database if not exists internal.regression_test"
        sql "grant select_priv on internal.regression_test.* to '${user}'@'%'"
        connect(user, pwd, context.config.jdbcUrl) {
            for (String kind in kinds) {
                test {
                    sql "select count(*) from ${view(kind)}${old}"
                    exception "denied"
                }
            }
        }
        sql "grant select_priv on ${base} to '${user}'@'%'"
        connect(user, pwd, context.config.jdbcUrl) {
            for (String kind in kinds) {
                sql "select count(*) from ${view(kind)}${old}"
            }
        }
    }
    if (fs) {
        String edgesPath = cfg.get("deltalakeTimeTravelFixture")
        if (edgesPath) {
            File edges = new File(edgesPath)
            def times = parser.parse(new File(edges, "expected.json")).retention
            sql "drop catalog if exists test_deltalake_sys_retention"
            sql """create catalog test_deltalake_sys_retention properties ('type'='deltalake',
                'warehouse'='${new File(edges, "warehouse").toURI()}')"""
            for (String kind in kinds) {
                String view = 'test_deltalake_sys_retention.edges.`retention$' + kind + '`'
                for (String selector in ["for version as of 2",
                        "for time as of '${Instant.ofEpochMilli(times.v2_ms.toLong())}'"]) {
                    test {
                        sql "select count(*) from ${view} ${selector}"
                        exception "delta.deletedFileRetentionDuration"
                    }
                }
            }
        }
        def clustered = reference.clustered
        assertNotNull(clustered)
        def relation = 'test_deltalake_sys_fs.' + clustered.database + '.`' + clustered.table + '$detail`'
        assertEquals(clustered.columns, json(sql("select to_json(clusteringColumns) from " + relation)[0][0]))
        def widened = reference.widened
        assertNotNull(widened)
        String wide = 'test_deltalake_sys_fs.' + widened.database + '.`' + widened.table
        for (def typed : [files: "partition_values", partitions: "`partition`"]) {
            def values = sql("select distinct ${typed.value}['p'] as p from " + wide + '$' + typed.key + '` order by p')
            assertEquals(widened.values, values.collect { it[0] })
        }
    }
}
