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
// Every Doris table model and partitioning scheme, read through ADBC.
//
// Every other suite here reads a DUPLICATE KEY table with one partition, which
// is the one shape where a row on disk is a row in the answer. The models
// below are the shapes where it is not:
//
//   AGGREGATE KEY    a key's value is the aggregate of its versions
//   UNIQUE KEY, MOW  a key's value is its newest version, resolved at WRITE
//                    time through a delete bitmap
//   UNIQUE KEY, MOR  the same, resolved at READ time by merging versions
//
// The reason this matters to an ADBC catalog is worth stating, because it is
// also why the assertions are the shape they are: the connector sends a plain
// SELECT and the SOURCE does the merging, so what is under test is not the
// merge itself but that this connector reads the merged answer rather than
// some pre-merge intermediate. Each fixture is therefore UPDATED and DELETED
// from after loading -- a table written once and never modified has no
// pre-merge state to read by mistake, so it could not fail this test.
//
// Partitioning matters for a different reason: a predicate pushed into the
// remote statement is what lets the SOURCE prune partitions, so a partitioned
// table is where pushdown stops being only a bandwidth saving.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_source_table_models", "p0,external") {
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
        logger.info("SKIPPED test_adbc_source_table_models: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "ADBC READS OF AGGREGATE, UNIQUE AND PARTITIONED SOURCES ARE NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_source_table_models_catalog"
    String dbName = "test_adbc_source_table_models_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    // ---- one fixture per model ----

    sql """
        CREATE TABLE internal.${dbName}.m_duplicate (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `v` int NULL
        ) DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    // The same key twice: in this model both rows survive, and an ADBC read must return both.
    sql """
        INSERT INTO internal.${dbName}.m_duplicate VALUES
          (1, 'a', 10), (1, 'a', 20), (2, 'b', 30), (3, 'c', 40)
    """

    sql """
        CREATE TABLE internal.${dbName}.m_aggregate (
          `id` int NOT NULL,
          `name` varchar(64) NOT NULL,
          `total` bigint SUM,
          `latest` varchar(64) REPLACE,
          `peak` int MAX
        ) AGGREGATE KEY(`id`, `name`) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    // Three loads against overlapping keys, so every aggregate function has something to do. Reading a
    // single version instead of the aggregate gives a plausible answer that is simply wrong.
    sql """INSERT INTO internal.${dbName}.m_aggregate VALUES (1, 'a', 10, 'first', 5)"""
    sql """INSERT INTO internal.${dbName}.m_aggregate VALUES (1, 'a', 20, 'second', 9)"""
    sql """INSERT INTO internal.${dbName}.m_aggregate VALUES (1, 'a', 30, 'third', 7)"""
    sql """INSERT INTO internal.${dbName}.m_aggregate VALUES (2, 'b', 100, 'only', 1)"""

    sql """
        CREATE TABLE internal.${dbName}.m_unique_mow (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `v` int NULL
        ) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql """
        INSERT INTO internal.${dbName}.m_unique_mow VALUES
          (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50)
    """
    // Overwrite, update and delete: after this the table's on-disk history contains rows that must NOT
    // appear in the answer, which is the whole point of pointing an external reader at it.
    sql """INSERT INTO internal.${dbName}.m_unique_mow VALUES (1, 'a-overwritten', 11)"""
    sql """UPDATE internal.${dbName}.m_unique_mow SET v = 22 WHERE id = 2"""
    sql """DELETE FROM internal.${dbName}.m_unique_mow WHERE id = 5"""

    sql """
        CREATE TABLE internal.${dbName}.m_unique_mor (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `v` int NULL
        ) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "false")
    """
    sql """
        INSERT INTO internal.${dbName}.m_unique_mor VALUES
          (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50)
    """
    sql """INSERT INTO internal.${dbName}.m_unique_mor VALUES (1, 'a-overwritten', 11)"""
    sql """DELETE FROM internal.${dbName}.m_unique_mor WHERE id = 5"""

    // ---- one fixture per partitioning scheme ----

    sql """
        CREATE TABLE internal.${dbName}.p_range (
          `d` date NOT NULL,
          `id` int NOT NULL,
          `v` int NULL
        ) DUPLICATE KEY(`d`, `id`)
        PARTITION BY RANGE(`d`) (
          PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
          PARTITION p2024q1 VALUES LESS THAN ('2024-04-01'),
          PARTITION p2024q2 VALUES LESS THAN ('2024-07-01'),
          PARTITION p2024rest VALUES LESS THAN ('2025-01-01')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.p_range VALUES
          ('2023-06-01', 1, 100), ('2024-01-15', 2, 200), ('2024-02-20', 3, 300),
          ('2024-05-05', 4, 400), ('2024-08-08', 5, 500), ('2024-12-31', 6, 600)
    """

    sql """
        CREATE TABLE internal.${dbName}.p_list (
          `region` varchar(16) NOT NULL,
          `id` int NOT NULL,
          `v` int NULL
        ) DUPLICATE KEY(`region`, `id`)
        PARTITION BY LIST(`region`) (
          PARTITION peast VALUES IN ('east'),
          PARTITION pwest VALUES IN ('west'),
          PARTITION pother VALUES IN ('north', 'south')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO internal.${dbName}.p_list VALUES
          ('east', 1, 10), ('east', 2, 20), ('west', 3, 30), ('north', 4, 40), ('south', 5, 50)
    """

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

    try {
        def sameAsSource = { String table, String columns, String tail ->
            def viaAdbc = sql("SELECT ${columns} FROM ${catalogName}.${dbName}.${table} ${tail}")
            def viaSource = sql("SELECT ${columns} FROM internal.${dbName}.${table} ${tail}")
            assertEquals(viaSource.toString(), viaAdbc.toString(),
                    "reading ${table} through ADBC returned different values than a native read of the "
                            + "same source table")
            return viaAdbc
        }

        // ---- DUPLICATE: duplicates are data ----

        qt_desc_duplicate """DESC ${catalogName}.${dbName}.m_duplicate"""
        qt_select_duplicate """
            SELECT id, name, v FROM ${catalogName}.${dbName}.m_duplicate ORDER BY id, v
        """
        sameAsSource("m_duplicate", "id, name, v", "ORDER BY id, v")
        assertEquals(4L, sql("SELECT count(*) FROM ${catalogName}.${dbName}.m_duplicate")[0][0],
                "a DUPLICATE KEY table lost its duplicate row")

        // ---- AGGREGATE: a key's value is the aggregate of its versions ----

        qt_desc_aggregate """DESC ${catalogName}.${dbName}.m_aggregate"""
        qt_select_aggregate """
            SELECT id, name, total, latest, peak FROM ${catalogName}.${dbName}.m_aggregate
            ORDER BY id, name
        """
        sameAsSource("m_aggregate", "id, name, total, latest, peak", "ORDER BY id, name")

        // Spelled out as well as compared: SUM=60, MAX=9 and REPLACE='third' are only reachable if the
        // source aggregated across all three loads before answering. Reading one version gives 10/5/'first'
        // -- a perfectly plausible row.
        def aggregated = sql """
            SELECT total, latest, peak FROM ${catalogName}.${dbName}.m_aggregate WHERE id = 1
        """
        assertEquals(1, aggregated.size(), "an AGGREGATE KEY table returned more than one row per key")
        assertEquals(60L, aggregated[0][0] as long,
                "SUM was not aggregated across loads: got ${aggregated[0][0]}, expected 60")
        assertEquals("third", aggregated[0][1].toString(),
                "REPLACE did not resolve to the newest load: ${aggregated[0][1]}")
        assertEquals(9, aggregated[0][2] as int, "MAX was not aggregated: ${aggregated[0][2]}")

        // ---- UNIQUE, merge-on-write ----

        qt_desc_mow """DESC ${catalogName}.${dbName}.m_unique_mow"""
        qt_select_mow """
            SELECT id, name, v FROM ${catalogName}.${dbName}.m_unique_mow ORDER BY id
        """
        sameAsSource("m_unique_mow", "id, name, v", "ORDER BY id")

        def mow = sql """
            SELECT count(*), count(DISTINCT id) FROM ${catalogName}.${dbName}.m_unique_mow
        """
        assertEquals(4L, mow[0][0] as long,
                "a merge-on-write table returned ${mow[0][0]} rows; four keys remain after the delete, so "
                        + "anything else means superseded or deleted versions were read")
        assertEquals(4L, mow[0][1] as long, "duplicate keys came back from a UNIQUE KEY table")

        // Each of the three modifications, checked on its own so the failure says which one leaked.
        assertEquals("[[a-overwritten, 11]]",
                sql("SELECT name, v FROM ${catalogName}.${dbName}.m_unique_mow WHERE id = 1").toString(),
                "an overwritten row was read at its old value")
        assertEquals("[[22]]",
                sql("SELECT v FROM ${catalogName}.${dbName}.m_unique_mow WHERE id = 2").toString(),
                "an UPDATEd row was read at its old value")
        assertEquals(0, sql("SELECT id FROM ${catalogName}.${dbName}.m_unique_mow WHERE id = 5").size(),
                "a DELETEd row was still readable through ADBC -- the delete bitmap was not applied")

        // ---- UNIQUE, merge-on-read ----
        //
        // The same table shape with the merge deferred to read time, which is a different code path on the
        // source and therefore a different thing to get wrong.

        qt_select_mor """
            SELECT id, name, v FROM ${catalogName}.${dbName}.m_unique_mor ORDER BY id
        """
        sameAsSource("m_unique_mor", "id, name, v", "ORDER BY id")

        def mor = sql """
            SELECT count(*), count(DISTINCT id) FROM ${catalogName}.${dbName}.m_unique_mor
        """
        assertEquals(4L, mor[0][0] as long,
                "a merge-on-read table returned ${mor[0][0]} rows instead of four")
        assertEquals(4L, mor[0][1] as long, "duplicate keys came back from a UNIQUE KEY table")
        assertEquals("[[a-overwritten, 11]]",
                sql("SELECT name, v FROM ${catalogName}.${dbName}.m_unique_mor WHERE id = 1").toString(),
                "an overwritten row was read at its old value on the merge-on-read path")
        assertEquals(0, sql("SELECT id FROM ${catalogName}.${dbName}.m_unique_mor WHERE id = 5").size(),
                "a DELETEd row was still readable from a merge-on-read table")

        // ---- RANGE partitioning ----

        qt_desc_range """DESC ${catalogName}.${dbName}.p_range"""
        qt_select_range """SELECT d, id, v FROM ${catalogName}.${dbName}.p_range ORDER BY d"""
        sameAsSource("p_range", "d, id, v", "ORDER BY d")
        sameAsSource("p_range", "count(*), min(d), max(d), sum(v)", "")

        // A predicate on the partition column. It is pushed into the remote statement, so the SOURCE gets
        // to prune -- which is the only way partition pruning can happen at all for an ADBC catalog, since
        // Doris knows nothing about the remote table's partitions.
        def prunable = "SELECT id FROM ${catalogName}.${dbName}.p_range WHERE d >= '2024-04-01'"
        String[] holder = new String[1]
        explain {
            sql(prunable)
            check { String plan ->
                String line = plan.readLines().find { it.trim().startsWith("QUERY: ") }
                assertNotNull(line, "no QUERY line in the plan:\n${plan}")
                holder[0] = line.trim().substring("QUERY: ".length())
                return true
            }
        }
        assertTrue(holder[0].contains("`d` >="),
                "the predicate on the partition column did not reach the source, so the source cannot "
                        + "prune anything: ${holder[0]}")
        sameAsSource("p_range", "id, d", "WHERE d >= '2024-04-01' ORDER BY id")
        sameAsSource("p_range", "id", "WHERE d >= '2024-01-01' AND d < '2024-07-01' ORDER BY id")
        // A range that selects no partition at all: the source prunes everything and returns nothing.
        assertEquals(0, sql("SELECT id FROM ${catalogName}.${dbName}.p_range WHERE d < '2020-01-01'").size())

        qt_range_pruned """
            SELECT d, id, v FROM ${catalogName}.${dbName}.p_range WHERE d >= '2024-04-01' ORDER BY d
        """

        // ---- LIST partitioning ----

        qt_select_list """SELECT region, id, v FROM ${catalogName}.${dbName}.p_list ORDER BY id"""
        sameAsSource("p_list", "region, id, v", "ORDER BY id")
        sameAsSource("p_list", "region, count(*), sum(v)", "GROUP BY region ORDER BY region")
        sameAsSource("p_list", "id", "WHERE region = 'east' ORDER BY id")
        sameAsSource("p_list", "id", "WHERE region IN ('north', 'south') ORDER BY id")

        qt_list_pruned """
            SELECT region, id FROM ${catalogName}.${dbName}.p_list WHERE region = 'east' ORDER BY id
        """

        // ---- a table with many partitions and many buckets ----
        //
        // Not a different model, but a different amount of work for the source's planner, and the shape
        // where a partitioned read is most likely to split into several partitions.

        sql """
            CREATE TABLE internal.${dbName}.p_many (
              `d` date NOT NULL,
              `id` int NOT NULL,
              `v` int NULL
            ) DUPLICATE KEY(`d`, `id`)
            PARTITION BY RANGE(`d`) (
              PARTITION p1 VALUES LESS THAN ('2024-02-01'),
              PARTITION p2 VALUES LESS THAN ('2024-03-01'),
              PARTITION p3 VALUES LESS THAN ('2024-04-01'),
              PARTITION p4 VALUES LESS THAN ('2024-05-01'),
              PARTITION p5 VALUES LESS THAN ('2024-06-01'),
              PARTITION p6 VALUES LESS THAN ('2024-07-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 8
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO internal.${dbName}.p_many
            SELECT date_add(CAST('2024-01-01' AS date), INTERVAL (number % 180) DAY), number, number * 2
            FROM numbers("number" = "5000")
        """
        sameAsSource("p_many", "count(*), count(DISTINCT id), sum(v), min(d), max(d)", "")
        sameAsSource("p_many", "count(*)", "WHERE d >= '2024-04-01'")
        qt_many_partitions """
            SELECT count(*), count(DISTINCT id), sum(v) FROM ${catalogName}.${dbName}.p_many
        """

        // ---- auto partitioning ----
        //
        // Last, and guarded: the AUTO PARTITION spelling has moved between Doris versions, and a fixture
        // that will not build must not be reported as an ADBC defect.
        boolean hasAutoPartition = true
        try {
            sql """
                CREATE TABLE internal.${dbName}.p_auto (
                  `d` date NOT NULL,
                  `id` int NOT NULL,
                  `v` int NULL
                ) DUPLICATE KEY(`d`, `id`)
                AUTO PARTITION BY RANGE (date_trunc(`d`, 'month')) ()
                DISTRIBUTED BY HASH(`id`) BUCKETS 2
                PROPERTIES ("replication_num" = "1")
            """
            sql """
                INSERT INTO internal.${dbName}.p_auto VALUES
                  ('2024-01-10', 1, 10), ('2024-02-10', 2, 20), ('2024-03-10', 3, 30),
                  ('2024-04-10', 4, 40)
            """
        } catch (Exception e) {
            hasAutoPartition = false
            logger.info("test_adbc_source_table_models: SKIPPING the AUTO PARTITION case, this Doris could "
                    + "not build the fixture: ${e.getMessage()}")
        }
        if (hasAutoPartition) {
            qt_select_auto """SELECT d, id, v FROM ${catalogName}.${dbName}.p_auto ORDER BY d"""
            sameAsSource("p_auto", "d, id, v", "ORDER BY d")
            sameAsSource("p_auto", "id", "WHERE d >= '2024-03-01' ORDER BY id")
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
