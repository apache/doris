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
// Setup is otherwise the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

import org.apache.doris.regression.util.ExportTestHelper

import java.nio.charset.StandardCharsets
import java.nio.file.Files

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

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_outfile_catalog"
    String dbName = "test_adbc_outfile_db"
    def storage = new ExportTestHelper(delegate)
    String prefix = "regression/test_adbc_outfile/${UUID.randomUUID()}/"
    String exportRoot = storage.uri(prefix)

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
            -- The loopback source is Doris even when vendor detection is unavailable.
            "sql_dialect" = "doris",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "required"
        )
    """

    File downloads = Files.createTempDirectory("adbc-outfile-").toFile()
    try {
        // Not named "table": streamLoad's configuration block below takes a `table "name"` call, and a
        // local of that name shadows it inside the closure -- the load then fails with
        // "No signature of method: java.lang.String.call()" rather than with anything about loading.
        String adbcTable = "${catalogName}.${dbName}.exported"

        def exportRowCount = { def result ->
            assertEquals(1, result.size(), "an OUTFILE statement did not report exactly one summary row")
            // FileNumber, TotalRows, FileSize, URL -- TotalRows is the second column.
            return result[0][1] as long
        }

        // ---- a whole table ----

        def allResult = sql """
            SELECT id, name, amount, d, ts FROM ${adbcTable}
            INTO OUTFILE "${exportRoot}all/"
            FORMAT AS CSV
            PROPERTIES ("column_separator" = ",", ${storage.storageProperties()})
        """
        assertEquals(4L, exportRowCount(allResult),
                "exporting a four-row ADBC table did not report four rows")

        List<File> allFiles = storage.downloadFiles(prefix + "all/", downloads)
        assertFalse(allFiles.isEmpty(), "the export produced no CSV files")
        List<String> allLines = new ArrayList<String>()
        for (File f : allFiles) {
            allLines.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8))
        }
        allLines.removeAll { it == null || it.trim().isEmpty() }
        assertEquals(4, allLines.size(),
                "the exported file holds ${allLines.size()} rows instead of four: ${allLines}")
        // Compared against the source rather than against a hand-written expectation: the file is
        // read outside Doris, so this is the one check here that does not pass through the engine at
        // all. Sorted because the writer's row order is not part of the contract.
        List<String> sorted = new ArrayList<String>(allLines)
        Collections.sort(sorted)
        logger.info("exported CSV rows: ${sorted}")
        assertTrue(sorted[0].startsWith("1,alice,10.25,2024-01-01,"),
                "the first exported row is not the first source row: ${sorted[0]}")
        assertTrue(sorted.any { it.startsWith("3,\\N,30.75") || it.startsWith("3,,30.75") },
                "the row with a NULL name did not survive the export: ${sorted}")

        // ---- a projection and a predicate ----
        //
        // The export path and the pushdown path meet here: the scan under the sink is the same scan, so a
        // predicate is still pushed and a column not asked for is still not read.

        def filteredResult = sql """
            SELECT id, amount FROM ${adbcTable} WHERE id >= 2
            INTO OUTFILE "${exportRoot}filtered/"
            FORMAT AS CSV PROPERTIES (${storage.storageProperties()})
        """
        assertEquals(3L, exportRowCount(filteredResult),
                "a filtered export did not report the three matching rows")

        List<String> filteredLines = new ArrayList<String>()
        for (File f : storage.downloadFiles(prefix + "filtered/", downloads)) {
            filteredLines.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8))
        }
        filteredLines.removeAll { it == null || it.trim().isEmpty() }
        assertEquals(3, filteredLines.size(), "the filtered export holds ${filteredLines.size()} rows: ${filteredLines}")

        // ---- a join between an ADBC table and an internal one ----

        def joinResult = sql """
            SELECT e.id, e.name, l.label
            FROM ${adbcTable} e JOIN internal.${dbName}.lookup l ON e.id = l.id
            INTO OUTFILE "${exportRoot}join/"
            FORMAT AS CSV PROPERTIES (${storage.storageProperties()})
        """
        assertEquals(3L, exportRowCount(joinResult),
                "exporting a join between an ADBC table and an internal one lost or added rows")

        // ---- an aggregate ----

        def aggResult = sql """
            SELECT count(*), sum(amount), min(d), max(d) FROM ${adbcTable}
            INTO OUTFILE "${exportRoot}agg/"
            FORMAT AS CSV PROPERTIES (${storage.storageProperties()})
        """
        assertEquals(1L, exportRowCount(aggResult), "an aggregate export did not report one row")

        // ---- an empty result ----
        //
        // Zero rows through a file sink: the export must succeed and report zero, not fail and not report
        // the unfiltered count.

        def emptyResult = sql """
            SELECT id, name FROM ${adbcTable} WHERE id > 1000
            INTO OUTFILE "${exportRoot}empty/"
            FORMAT AS CSV PROPERTIES (${storage.storageProperties()})
        """
        assertEquals(0L, exportRowCount(emptyResult),
                "exporting an empty result did not report zero rows")

        // ---- the columnar formats ----
        //
        // These need a WRITE schema for every column, which CSV does not: a type the ADBC reader
        // materialises but that has no Parquet or ORC form fails here and only here.

        def parquetResult = sql """
            SELECT id, name, amount, d, ts FROM ${adbcTable}
            INTO OUTFILE "${exportRoot}parquet/"
            FORMAT AS PARQUET PROPERTIES (${storage.storageProperties()})
        """
        assertEquals(4L, exportRowCount(parquetResult), "the PARQUET export did not report four rows")
        assertFalse(storage.keys(prefix + "parquet/").isEmpty(), "the PARQUET export produced no file")

        def orcResult = sql """
            SELECT id, name, amount, d, ts FROM ${adbcTable}
            INTO OUTFILE "${exportRoot}orc/"
            FORMAT AS ORC PROPERTIES (${storage.storageProperties()})
        """
        assertEquals(4L, exportRowCount(orcResult), "the ORC export did not report four rows")
        assertFalse(storage.keys(prefix + "orc/").isEmpty(), "the ORC export produced no file")

        // ---- the exported data, read back through Doris ----
        //
        // Loading the CSV back into a table and comparing it with the
        // source closes the loop without trusting the line-by-line parse above.

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
        for (File csv : allFiles) {
            streamLoad {
                db "${dbName}"
                table "reloaded"
                set 'column_separator', ','
                file csv.absolutePath
                time 20000
            }
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

        // The source DATETIME is timezone-free, just like the reloaded column checked above.
        qt_source_for_export """
            SELECT id, name, amount, d, ts FROM ${adbcTable} ORDER BY id
        """
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
        downloads.deleteDir()
        storage.deleteFiles(prefix)
    }
}
