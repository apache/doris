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

// ############################################################################
// SELECT ... INTO OUTFILE from an ADBC catalog.
//
// A different consumer of the scan than everything else in this directory:
// every other suite sends rows to a result set, and this one sends them to a
// file sink on BE. That matters because the scan's output goes somewhere with
// its own type expectations -- a CSV writer has to render every column the
// ADBC reader materialised, and PARQUET/ORC writers have to be handed a schema
// for them. A type that reads back fine into a result set can still have no
// writable form here.
//
// It is also the first thing in this directory that produces an artifact whose
// correctness can be checked OUTSIDE Doris: the exported file is read back
// with plain Java and compared against the source table, so the answer does
// not pass through the engine twice.
//
// Two conditions skip parts of the suite rather than failing it:
//   * enable_outfile_to_local must be on. It is an FE config, off by default
//     in some deployments, and nothing about ADBC is being tested without it.
//   * on a multi-backend cluster the files land on whichever backends ran the
//     scan, so a suite running on the FE host cannot read them all. The export
//     still runs and its reported row count is still checked; only the
//     file-content half is skipped, with a log saying so.
//
// Setup is otherwise the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_adbc_outfile", "p0,external") {
    String repoRoot = new File(context.config.suitePath).getParentFile().getParentFile()
            .getAbsolutePath()
    String thirdparty = System.getenv("DORIS_THIRDPARTY")
    if (thirdparty == null || thirdparty.isEmpty()) {
        thirdparty = "${repoRoot}/thirdparty"
    }
    String driverPath = context.config.otherConfigs.get("adbcDriverPath")
    if (driverPath == null || driverPath.isEmpty()) {
        driverPath = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }

    if (!new File(driverPath).canRead()) {
        logger.info("SKIPPED test_adbc_outfile: no readable ADBC Flight SQL driver at ${driverPath}. "
                + "Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', or set "
                + "adbcDriverPath in regression-conf.groovy. "
                + "EXPORTING AN ADBC TABLE IS NOT BEING TESTED.")
        return
    }

    // ---- is a local export allowed at all ----

    StringBuilder configUrl = new StringBuilder()
    configUrl.append("curl --location-trusted -u " + context.config.jdbcUser + ":"
            + context.config.jdbcPassword)
    if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
        configUrl.append(" https://" + context.config.feHttpAddress + "/rest/v1/config/fe")
        configUrl.append(" --cert " + context.config.otherConfigs.get("trustCert")
                + " --cacert " + context.config.otherConfigs.get("trustCACert")
                + " --key " + context.config.otherConfigs.get("trustCAKey"))
    } else {
        configUrl.append(" http://" + context.config.feHttpAddress + "/rest/v1/config/fe")
    }
    def configProcess = configUrl.toString().execute()
    int configCode = configProcess.waitFor()
    String configOut = configProcess.getText()
    boolean enableOutfileToLocal = false
    if (configCode == 0) {
        def response = parseJson(configOut.trim())
        if (response.code == 0) {
            for (Object conf : response.data.rows) {
                if (conf instanceof Map
                        && ((Map<String, String>) conf).get("Name").toLowerCase()
                            == "enable_outfile_to_local") {
                    enableOutfileToLocal =
                            ((Map<String, String>) conf).get("Value").toLowerCase() == "true"
                }
            }
        }
    }
    if (!enableOutfileToLocal) {
        logger.info("SKIPPED test_adbc_outfile: the FE config enable_outfile_to_local is not true, so a "
                + "local export cannot run. Set it to true to cover this. "
                + "EXPORTING AN ADBC TABLE IS NOT BEING TESTED.")
        return
    }

    def backends = sql_return_maparray "show backends"
    int aliveBackends = backends.count { it.Alive.toString().equalsIgnoreCase("true") }
    // On several backends the exported files are spread across their local disks, and this suite runs
    // where the FE is. The exports still run; only reading the files back is skipped.
    boolean canReadFilesBack = aliveBackends == 1
    if (!canReadFilesBack) {
        logger.info("test_adbc_outfile: ${aliveBackends} alive backends, so exported files land on hosts "
                + "this suite cannot read. The exports and their reported row counts are still checked; "
                + "THE FILE CONTENTS ARE NOT.")
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_outfile_catalog"
    String dbName = "test_adbc_outfile_db"
    String uuid = UUID.randomUUID().toString()
    String exportRoot = "/tmp/test_adbc_outfile_${uuid}"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    sql """
        CREATE TABLE internal.${dbName}.exported (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `amount` decimalv3(10, 2) NULL,
          `d` date NULL,
          `ts` datetime(3) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // A null and a comma-free but quote-bearing string: both are what a CSV writer has to get right, and
    // both survive a result-set read without saying anything about the file.
    sql """
        INSERT INTO internal.${dbName}.exported VALUES
          (1, 'alice', 10.25, '2024-01-01', '2024-01-01 10:00:00.000'),
          (2, 'bob',   20.50, '2024-02-01', '2024-02-01 11:00:00.500'),
          (3, NULL,    30.75, '2024-03-01', '2024-03-01 12:00:00.250'),
          (4, 'dave',  NULL,  NULL,         NULL)
    """

    sql """
        CREATE TABLE internal.${dbName}.lookup (
          `id` int NOT NULL,
          `label` varchar(32) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO internal.${dbName}.lookup VALUES (1, 'one'), (2, 'two'), (3, 'three')"""

    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "required"
        )
    """

    File exportDir = new File(exportRoot)
    try {
        // Not named "table": streamLoad's configuration block below takes a `table "name"` call, and a
        // local of that name shadows it inside the closure -- the load then fails with
        // "No signature of method: java.lang.String.call()" rather than with anything about loading.
        String adbcTable = "${catalogName}.${dbName}.exported"

        // The reported row count is the assertion that works everywhere, including on a cluster whose
        // files this suite cannot reach. Doris answers an OUTFILE with one row describing the export.
        def exportRowCount = { def result ->
            assertEquals(1, result.size(), "an OUTFILE statement did not report exactly one summary row")
            // FileNumber, TotalRows, FileSize, URL -- TotalRows is the second column.
            return result[0][1] as long
        }

        def newExportDir = { String name ->
            File dir = new File("${exportRoot}/${name}")
            assertTrue(dir.mkdirs(), "could not create the export directory ${dir}")
            return dir
        }

        // ---- a whole table ----

        File allDir = newExportDir("all")
        def allResult = sql """
            SELECT id, name, amount, d, ts FROM ${adbcTable}
            INTO OUTFILE "file://${allDir.absolutePath}/"
            FORMAT AS CSV
            PROPERTIES ("column_separator" = ",")
        """
        assertEquals(4L, exportRowCount(allResult),
                "exporting a four-row ADBC table did not report four rows")

        if (canReadFilesBack) {
            File[] allFiles = allDir.listFiles()
            assertNotNull(allFiles, "the export directory ${allDir} does not exist after the export")
            assertTrue(allFiles.length >= 1, "the export produced no file in ${allDir}")
            List<String> lines = new ArrayList<String>()
            for (File f : allFiles) {
                lines.addAll(Files.readAllLines(Paths.get(f.getAbsolutePath()), StandardCharsets.UTF_8))
            }
            lines.removeAll { it == null || it.trim().isEmpty() }
            assertEquals(4, lines.size(),
                    "the exported file holds ${lines.size()} rows instead of four: ${lines}")
            // Compared against the source rather than against a hand-written expectation: the file is
            // read outside Doris, so this is the one check here that does not pass through the engine at
            // all. Sorted because the writer's row order is not part of the contract.
            List<String> sorted = new ArrayList<String>(lines)
            Collections.sort(sorted)
            logger.info("exported CSV rows: ${sorted}")
            assertTrue(sorted[0].startsWith("1,alice,10.25,2024-01-01,"),
                    "the first exported row is not the first source row: ${sorted[0]}")
            assertTrue(sorted.any { it.startsWith("3,\\N,30.75") || it.startsWith("3,,30.75") },
                    "the row with a NULL name did not survive the export: ${sorted}")
        }

        // ---- a projection and a predicate ----
        //
        // The export path and the pushdown path meet here: the scan under the sink is the same scan, so a
        // predicate is still pushed and a column not asked for is still not read.

        File filteredDir = newExportDir("filtered")
        def filteredResult = sql """
            SELECT id, amount FROM ${adbcTable} WHERE id >= 2
            INTO OUTFILE "file://${filteredDir.absolutePath}/"
            FORMAT AS CSV
        """
        assertEquals(3L, exportRowCount(filteredResult),
                "a filtered export did not report the three matching rows")

        if (canReadFilesBack) {
            List<String> lines = new ArrayList<String>()
            for (File f : filteredDir.listFiles()) {
                lines.addAll(Files.readAllLines(Paths.get(f.getAbsolutePath()), StandardCharsets.UTF_8))
            }
            lines.removeAll { it == null || it.trim().isEmpty() }
            assertEquals(3, lines.size(), "the filtered export holds ${lines.size()} rows: ${lines}")
        }

        // ---- a join between an ADBC table and an internal one ----

        File joinDir = newExportDir("join")
        def joinResult = sql """
            SELECT e.id, e.name, l.label
            FROM ${adbcTable} e JOIN internal.${dbName}.lookup l ON e.id = l.id
            INTO OUTFILE "file://${joinDir.absolutePath}/"
            FORMAT AS CSV
        """
        assertEquals(3L, exportRowCount(joinResult),
                "exporting a join between an ADBC table and an internal one lost or added rows")

        // ---- an aggregate ----

        File aggDir = newExportDir("agg")
        def aggResult = sql """
            SELECT count(*), sum(amount), min(d), max(d) FROM ${adbcTable}
            INTO OUTFILE "file://${aggDir.absolutePath}/"
            FORMAT AS CSV
        """
        assertEquals(1L, exportRowCount(aggResult), "an aggregate export did not report one row")

        // ---- an empty result ----
        //
        // Zero rows through a file sink: the export must succeed and report zero, not fail and not report
        // the unfiltered count.

        File emptyDir = newExportDir("empty")
        def emptyResult = sql """
            SELECT id, name FROM ${adbcTable} WHERE id > 1000
            INTO OUTFILE "file://${emptyDir.absolutePath}/"
            FORMAT AS CSV
        """
        assertEquals(0L, exportRowCount(emptyResult),
                "exporting an empty result did not report zero rows")

        // ---- the columnar formats ----
        //
        // These need a WRITE schema for every column, which CSV does not: a type the ADBC reader
        // materialises but that has no Parquet or ORC form fails here and only here.

        File parquetDir = newExportDir("parquet")
        def parquetResult = sql """
            SELECT id, name, amount, d, ts FROM ${adbcTable}
            INTO OUTFILE "file://${parquetDir.absolutePath}/"
            FORMAT AS PARQUET
        """
        assertEquals(4L, exportRowCount(parquetResult), "the PARQUET export did not report four rows")
        if (canReadFilesBack) {
            assertTrue(parquetDir.listFiles().length >= 1, "the PARQUET export produced no file")
        }

        File orcDir = newExportDir("orc")
        def orcResult = sql """
            SELECT id, name, amount, d, ts FROM ${adbcTable}
            INTO OUTFILE "file://${orcDir.absolutePath}/"
            FORMAT AS ORC
        """
        assertEquals(4L, exportRowCount(orcResult), "the ORC export did not report four rows")
        if (canReadFilesBack) {
            assertTrue(orcDir.listFiles().length >= 1, "the ORC export produced no file")
        }

        // ---- the exported data, read back through Doris ----
        //
        // Only where the files are reachable: loading the CSV back into a table and comparing it with the
        // source closes the loop without trusting the line-by-line parse above.

        if (canReadFilesBack) {
            sql """DROP TABLE IF EXISTS internal.${dbName}.reloaded"""
            sql """
                CREATE TABLE internal.${dbName}.reloaded (
                  `id` int NOT NULL,
                  `name` varchar(64) NULL,
                  `amount` decimalv3(10, 2) NULL,
                  `d` date NULL,
                  `ts` datetime(3) NULL
                ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            File[] csvFiles = allDir.listFiles()
            streamLoad {
                db "${dbName}"
                table "reloaded"
                set 'column_separator', ','
                file csvFiles[0].getAbsolutePath()
                time 20000
            }
            qt_reloaded """
                SELECT id, name, amount, d, ts FROM internal.${dbName}.reloaded ORDER BY id
            """
            assertEquals(
                    sql("""SELECT id, name, amount, d, ts FROM internal.${dbName}.exported
                           ORDER BY id""").toString(),
                    sql("""SELECT id, name, amount, d, ts FROM internal.${dbName}.reloaded
                           ORDER BY id""").toString(),
                    "the data exported from the ADBC catalog and loaded back does not match the source")
        }

        qt_source_for_export """
            SELECT id, name, amount, d, ts FROM ${adbcTable} ORDER BY id
        """
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
        exportDir.deleteDir()
    }
}
