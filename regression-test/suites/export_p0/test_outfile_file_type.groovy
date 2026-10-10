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
import java.util.zip.GZIPInputStream

// Prerequisites and fixture build instructions: tools/file_type/README.md.
// The runner and every participating BE must share the same local output path.
suite("test_outfile_file_type") {
    def publicStruct = "STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT," +
            "content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>"
    if (!(context.config.otherConfigs.get("enableFileTypeLocalOutfile")?.toString()?.toBoolean())) {
        logger.warn("Skip test_outfile_file_type: enableFileTypeLocalOutfile requires a same-host/shared-path cluster")
        return
    }
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        throw new IllegalStateException("test_outfile_file_type requires FE enable_outfile_to_local=true")
    }
    def toolPath = context.config.otherConfigs.get("fileTypeFixtureTool")?.toString()
    def rootPath = context.config.otherConfigs.get("fileTypeOutfileRoot")?.toString()
    if (!toolPath || !new File(toolPath).isAbsolute() || !new File(toolPath).canExecute()) {
        throw new IllegalStateException("fileTypeFixtureTool must name the absolute standalone verifier executable")
    }
    if (!rootPath || !new File(rootPath).isAbsolute() || !new File(rootPath).isDirectory()) {
        throw new IllegalStateException("fileTypeOutfileRoot must name an existing absolute directory shared with all BEs")
    }
    File outputRoot = new File(rootPath, "file-type-${UUID.randomUUID()}").canonicalFile
    // A local SQL URL is embedded below; keep this dedicated test path unambiguous.
    if (!(outputRoot.path ==~ /[A-Za-z0-9_\/.-]+/)) {
        throw new IllegalStateException("fileTypeOutfileRoot must contain only letters, digits, slash, underscore, dot or dash")
    }
    if (!outputRoot.mkdir()) {
        throw new IllegalStateException("Cannot create shared output directory ${outputRoot}")
    }

    def verifyFiles = { String format, List<File> files, boolean expectNullInline = false,
                        String sparseCase = "" ->
        assertFalse(files.isEmpty(), "No local ${format} files; check the same-host/shared-path prerequisite")
        List<String> command = sparseCase ? [toolPath, "verify-sparse", sparseCase]
                                          : [toolPath, "verify", format]
        if (expectNullInline) {
            command.add("--expect-null-inline")
        }
        command.addAll(files.collect { it.canonicalPath })
        def process = new ProcessBuilder(command).redirectErrorStream(true).start()
        String result = process.inputStream.getText("UTF-8")
        assertEquals(0, process.waitFor(), "FILE binary/schema oracle failed: ${result}")
        logger.info(result.trim())
    }

    def verifyPublicJson = { List<File> files ->
        assertFalse(files.isEmpty(), "No local JSON files")
        def publicFields = ["uri", "offset", "size", "content_type", "checksum", "inline"].toSet()
        def checkFile = { value ->
            if (value != null) {
                assertTrue(value instanceof Map, "JSON FILE must be an object")
                assertEquals(publicFields, value.keySet(), "JSON FILE must have exactly six public properties")
            }
        }
        def checkArray = { value ->
            if (value != null) {
                assertTrue(value instanceof List, "JSON ARRAY<FILE> must be an array")
                value.each { checkFile(it) }
            }
        }
        def ids = [] as Set
        files.each { input ->
            input.withInputStream { raw ->
                def decoded = input.name.endsWith(".gz") ? new GZIPInputStream(raw) : raw
                decoded.withReader("UTF-8") { reader ->
                    reader.eachLine { line ->
                        def row = new JsonSlurper().parseText(line)
                        assertTrue(row instanceof Map, "JSON OUTFILE row must be an object")
                        assertEquals(["id", "f", "files", "holder", "lookup"].toSet(), row.keySet())
                        assertTrue(ids.add(row.id), "Duplicate JSON row id ${row.id}")
                        checkFile(row.f)
                        checkArray(row.files)
                        if (row.holder != null) {
                            assertTrue(row.holder instanceof Map)
                            assertEquals(["asset", "attachments"].toSet(), row.holder.keySet())
                            checkFile(row.holder.asset)
                            checkArray(row.holder.attachments)
                        }
                        if (row.lookup != null) {
                            assertTrue(row.lookup instanceof Map)
                            row.lookup.values().each { checkFile(it) }
                        }
                    }
                }
            }
        }
        assertEquals((1..6).toSet(), ids, "JSON OUTFILE must contain all six fixture rows")
    }

    def loadFiles = { String targetTable, String format, List<File> files ->
        long loaded = 0
        files.each { input ->
            streamLoad {
                table targetTable
                set "format", format
                set "columns", "id,f,files,holder,lookup"
                set "strict_mode", "true"
                set "max_filter_ratio", "0"
                if (format == "json") {
                    set "read_json_by_line", "true"
                    if (input.name.endsWith(".gz")) {
                        set "compress_type", "gz"
                    }
                }
                file input.canonicalPath
                time 10000
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def status = parseJson(result)
                    assertEquals("success", status.Status.toLowerCase(), result)
                    assertEquals(0L, status.NumberFilteredRows as long, result)
                    assertEquals(0L, status.NumberUnselectedRows as long, result)
                    assertEquals(status.NumberTotalRows as long, status.NumberLoadedRows as long, result)
                    loaded += status.NumberLoadedRows as long
                }
            }
        }
        assertEquals(6L, loaded, "Stream load must transport all six fixture rows")
        sql "sync"
    }

    def checkMetadata = { String tag, String sourceTable ->
        "qt_${tag}" """
            SELECT id, f IS NULL, ELEMENT_AT(f, 'uri'), ELEMENT_AT(f, 'offset'), ELEMENT_AT(f, 'size'),
                   ELEMENT_AT(f, 'content_type'), ELEMENT_AT(f, 'checksum'),
                   files IS NULL, ARRAY_SIZE(CAST(files AS ARRAY<${publicStruct}>)), ELEMENT_AT(ELEMENT_AT(files, 1), 'uri'),
                   ELEMENT_AT(ELEMENT_AT(files, 2), 'size'),
                   holder IS NULL, ELEMENT_AT(ELEMENT_AT(holder, 'asset'), 'uri'),
                   ARRAY_SIZE(CAST(ELEMENT_AT(holder, 'attachments') AS ARRAY<${publicStruct}>)),
                   ELEMENT_AT(ELEMENT_AT(ELEMENT_AT(holder, 'attachments'), 1), 'uri'),
                   lookup IS NULL, MAP_SIZE(CAST(lookup AS MAP<STRING,${publicStruct}>)),
                   ELEMENT_AT(ELEMENT_AT(lookup, 'blob'), 'uri'),
                   ELEMENT_AT(ELEMENT_AT(lookup, 'empty'), 'uri'),
                   ELEMENT_AT(ELEMENT_AT(lookup, 'external'), 'uri'),
                   ELEMENT_AT(lookup, 'null') IS NULL
            FROM ${sourceTable} ORDER BY id
        """
    }

    def exportFiles = { String sourceTable, String format, String stage, String queryHints = "", boolean gzip = false, boolean expectNullInline = false,
                        String sparseCase = "" ->
        File directory = new File(outputRoot, stage)
        if (!directory.mkdir()) {
            throw new IllegalStateException("Cannot create local OUTFILE directory ${directory}")
        }
        // JSON represents inline as Base64; ORC retains its raw binary bytes.
        String formatOptions = ""
        if (format == "json" && gzip) {
            formatOptions = 'PROPERTIES ("compress_type" = "gz")'
        }
        sql """
            SELECT ${queryHints} id, f, files, holder, lookup FROM ${sourceTable} ORDER BY id
            INTO OUTFILE 'file://${directory.canonicalPath}/' FORMAT AS ${format}
            ${formatOptions}
        """
        List<File> files = directory.listFiles().findAll {
            it.isFile() && it.name.endsWith(".${format}${gzip ? '.gz' : ''}")
        }.sort { it.name }
        if (format == "orc") {
            verifyFiles(format, files, expectNullInline, sparseCase)
        } else if (format == "json") {
            verifyPublicJson(files)
        }
        return files
    }

    File sparseDirectory = null

    try {
        sql "DROP TABLE IF EXISTS test_outfile_file_type_source"
        sql """
            CREATE TABLE test_outfile_file_type_source (
                id INT NOT NULL,
                f FILE,
                files ARRAY<FILE>,
                holder STRUCT<asset:FILE,attachments:ARRAY<FILE>>,
                lookup MAP<STRING,FILE>
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")
        """
        sql "DROP TABLE IF EXISTS test_outfile_file_type_restored"
        sql "CREATE TABLE test_outfile_file_type_restored LIKE test_outfile_file_type_source"
        order_qt_schema "DESC test_outfile_file_type_source"

        String inputFormat = "orc"
        File fixture = new File(context.dataPath, "file_type/canonical.orc")
        verifyFiles(inputFormat, [fixture])
        sql "TRUNCATE TABLE test_outfile_file_type_source"
        loadFiles("test_outfile_file_type_source", inputFormat, [fixture])
        checkMetadata("${inputFormat}_input", "test_outfile_file_type_source")

        // FILE must be rejected explicitly, including every nested container position.
        ["f", "files", "holder", "lookup"].each { column ->
            test {
                sql """
                    SELECT ${column} FROM test_outfile_file_type_source
                    INTO OUTFILE 'file://${outputRoot.canonicalPath}/unsupported_${column}_'
                    FORMAT AS PARQUET
                """
                exception "Parquet OUTFILE does not support FILE"
            }
        }

        ["f", "files", "holder", "lookup"].each { column ->
            test {
                sql """
                    SELECT ${column} FROM test_outfile_file_type_source
                    INTO OUTFILE 'file://${outputRoot.canonicalPath}/csv_${column}_'
                    FORMAT AS CSV
                """
                exception "CSV OUTFILE does not support FILE"
            }
        }

        // Computed scalar keys prevent reusing this table's id-bucket distribution for a colocate join.
        // Both join sides contribute full values; FILE never participates in hashing.
        String shuffled = """
            SELECT a.id, a.f, b.files, a.holder, b.lookup
            FROM test_outfile_file_type_source a
            JOIN [shuffle] test_outfile_file_type_source b
              ON CONCAT('row-', CAST(a.id AS STRING)) = CONCAT('row-', CAST(b.id AS STRING))
        """
        explain {
            sql shuffled
            contains "INNER JOIN(PARTITIONED)"
        }
        exportFiles("(${shuffled}) shuffled", "orc", "${inputFormat}_shuffle")

        // Force a full sort carrying values, including the nested inline bytes, rather than late row fetch.
        exportFiles("""(SELECT id, f, files, holder, lookup FROM test_outfile_file_type_source
                        ORDER BY id DESC LIMIT 6) sorted_values""", "orc", "${inputFormat}_sort",
                "/*+ SET_VAR(force_sort_algorithm='full', topn_lazy_materialization_threshold=-1) */")

        // Ordinary windows and conditional functions reject complete FILE payloads,
        // including nested containers; sort and shuffle transport remains covered above.
        ["f", "files", "holder", "lookup"].each { column ->
            ["LAG(${column}) OVER (ORDER BY id)", "LEAD(${column}) OVER (ORDER BY id)",
             "FIRST_VALUE(${column}) OVER (ORDER BY id)", "LAST_VALUE(${column}) OVER (ORDER BY id)",
             "NTH_VALUE(${column}, 1) OVER (ORDER BY id)", "IF(id % 2 = 0, ${column}, NULL)",
             "COALESCE(${column}, NULL)"].each { expression ->
                test {
                    sql "SELECT ${expression} FROM test_outfile_file_type_source"
                    exception "FILE"
                }
            }
        }

        ["orc", "json", "json_gzip"].each { outputCase ->
            String outputFormat = outputCase == "json_gzip" ? "json" : outputCase
            String stage = "${inputFormat}_to_${outputCase}"
            List<File> exported = exportFiles("test_outfile_file_type_source", outputFormat,
                    stage, "", outputCase == "json_gzip")
            sql "TRUNCATE TABLE test_outfile_file_type_restored"
            loadFiles("test_outfile_file_type_restored", outputFormat, exported)
            checkMetadata("${stage}_restored", "test_outfile_file_type_restored")
            // Every format must retain original inline bytes, including empty bytes versus NULL.
            exportFiles("test_outfile_file_type_restored",
                    "orc", "${stage}_restored",
                    "", false, false)
        }
        // LOCAL() resolves paths beneath the chosen BE's user_files_secure_path.
        def backendIps = [:]
        def backendHttpPorts = [:]
        getBackendIpHttpPort(backendIps, backendHttpPorts)
        assertFalse(backendIps.isEmpty(), "Sparse LOCAL() reads need a backend")
        String backendId = backendIps.keySet().iterator().next()
        def (configCode, configOutput, configError) = show_be_config(
                backendIps.get(backendId), backendHttpPorts.get(backendId))
        assertEquals(0, configCode, "Cannot read BE ${backendId} config: ${configError}")
        def securePathConfig = parseJson(configOutput.trim()).find { it[0] == "user_files_secure_path" }
        assertTrue(securePathConfig != null, "BE ${backendId} config user_files_secure_path not found")
        File secureRoot = new File(securePathConfig[2].toString())
        assertTrue(secureRoot.isAbsolute() && secureRoot.isDirectory(),
                "BE ${backendId} user_files_secure_path must be an absolute directory visible to the runner: ${secureRoot}")
        secureRoot = secureRoot.canonicalFile
        String sparseRelativeDirectory = "doris-file-orc-sparse-${UUID.randomUUID()}"
        File candidate = new File(secureRoot, sparseRelativeDirectory)
        assertEquals(secureRoot, candidate.canonicalFile.parentFile,
                "Sparse fixtures must remain beneath user_files_secure_path")
        if (!candidate.mkdir()) {
            throw new IllegalStateException("Cannot create sparse fixture directory ${candidate}")
        }
        // Assign only after creating it: cleanup owns exactly this new UUID directory.
        sparseDirectory = candidate
        def generator = new ProcessBuilder([toolPath, "generate-sparse", sparseDirectory.canonicalPath])
                .redirectErrorStream(true).start()
        String generated = generator.inputStream.getText("UTF-8")
        assertEquals(0, generator.waitFor(), "Sparse ORC fixture generation failed: ${generated}")
        def sparseSource = { String name ->
            """local("file_path" = "${sparseRelativeDirectory}/sparse_${name}.orc",
                     "backend_id" = "${backendId}", "format" = "orc")"""
        }
        def originalScannerV2 = (sql "SHOW VARIABLES LIKE 'enable_file_scanner_v2'")[0][1]
        try {
            [false, true].each { scannerV2 ->
                sql "SET enable_file_scanner_v2 = ${scannerV2}"
                String reader = scannerV2 ? "v2" : "legacy"
                test {
                    // Schema discovery must report this error without letting an exception escape
                    // the reader. The marked FILE has uri:bigint and the ORC file has no rows.
                    sql "SELECT f FROM ${sparseSource('invalid_uri_type')}"
                    exception "Invalid FILE ORC child uri type"
                }
                ["uri_only", "middle", "inline", "no_inline"].each { sparseCase ->
                    String source = sparseSource(sparseCase)
                    String stage = "sparse_orc_${reader}_${sparseCase}"
                    // Public metadata and ancestor NULLs are SQL oracles. The six-child ORC
                    // oracle additionally proves NULL filling and retained inline at all five
                    // FILE positions, including NULL versus empty inline and missing middle fields.
                    checkMetadata(stage, source)
                    exportFiles(source, "orc", stage, "", false, false, sparseCase)
                }
                ["top", "nested"].each { location ->
                    [null_uri: "FILE uri must not be NULL",
                     empty_uri: "FILE uri must be an absolute",
                     negative_size: "FILE size must be nonnegative",
                     negative_offset: "FILE offset must be nonnegative",
                     missing_size: "FILE offset requires size",
                     overflow: "FILE offset plus size exceeds BIGINT"].each { invalidCase, message ->
                        test {
                            // Read full values so projection/count optimizations cannot skip validation.
                            sql "SELECT id, f, files, holder, lookup FROM ${sparseSource("invalid_${location}_${invalidCase}")} ORDER BY id"
                            exception message
                        }
                    }
                }
            }
        } finally {
            sql "SET enable_file_scanner_v2 = ${originalScannerV2}"
        }
    } finally {
        // Retain tables for diagnosis. Delete only the two UUID directories this suite created.
        try {
            if (sparseDirectory != null) {
                sparseDirectory.deleteDir()
            }
        } finally {
            outputRoot.deleteDir()
        }
    }
}
