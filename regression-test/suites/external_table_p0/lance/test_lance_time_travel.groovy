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

suite("test_lance_time_travel", "p0,external") {
    /*
     * FOR VERSION AS OF / FOR TIME AS OF on a Lance filesystem catalog.
     *
     * time_travel.lance keeps three versions (see lance_build_time_travel.py):
     *   version 1  row_id 1..3  committed 2026-09-19 13:06:07.597 UTC
     *   version 2  row_id 4..6  committed 2026-09-19 13:06:09.113 UTC
     *   version 3  row_id 7..9  committed 2026-09-19 13:06:10.621 UTC
     * The timestamps below are those commit times; regenerating the fixture changes them.
     * Every version carries a Lance tag of the same name, and a branch "dev" forks from
     * version 2 with one extra append (row_id 100).
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_time_travel"
    String table = "`${catalogName}`.`default`.`time_travel`"
    def scannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    String originalScannerV2 = scannerV2Rows[0][1].toString()
    String originalTimeZone = (sql """SHOW VARIABLES LIKE 'time_zone'""")[0][1].toString()

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """SET enable_file_scanner_v2 = true"""
        sql """SET time_zone = 'UTC'"""
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """

        // Latest version without a selector.
        qt_latest """SELECT count(*), min(row_id), max(row_id) FROM ${table}"""
        explain {
            sql("SELECT row_id FROM ${table}")
            contains "lanceVersion=3"
            contains "lanceManagedVersioning=false"
            contains "lanceFragments=3"
        }

        // Explicit versions: each one is a fixed snapshot that cannot see later appends.
        order_qt_version_1 """SELECT * FROM ${table} FOR VERSION AS OF 1"""
        order_qt_version_2 """SELECT * FROM ${table} FOR VERSION AS OF 2"""
        order_qt_version_3 """SELECT * FROM ${table} FOR VERSION AS OF 3"""
        explain {
            sql("SELECT row_id FROM ${table} FOR VERSION AS OF 2")
            contains "lanceVersion=2"
            contains "lanceFragments=2"
        }

        // Pushed-down predicates and LIMIT are evaluated on the selected version.
        qt_version_with_predicate """
            SELECT count(*) FROM ${table} FOR VERSION AS OF 3 WHERE row_id > 6
        """
        explain {
            sql("SELECT row_id FROM ${table} FOR VERSION AS OF 2 WHERE row_id > 6")
            contains "lanceVersion=2"
            contains "lancePushdownPredicate="
        }
        qt_version_with_predicate_empty """
            SELECT count(*) FROM ${table} FOR VERSION AS OF 2 WHERE row_id > 6
        """

        // Two references to the same table pin two different versions in one statement.
        order_qt_self_join """
            SELECT a.row_id, a.tag, b.tag
            FROM ${table} FOR VERSION AS OF 1 a
            JOIN ${table} FOR VERSION AS OF 3 b ON a.row_id = b.row_id
        """
        order_qt_union_versions """
            SELECT 1 AS v, count(*) FROM ${table} FOR VERSION AS OF 1
            UNION ALL SELECT 2, count(*) FROM ${table} FOR VERSION AS OF 2
            UNION ALL SELECT 3, count(*) FROM ${table} FOR VERSION AS OF 3
        """

        // FOR TIME AS OF picks the latest version committed at or before the timestamp,
        // parsed in the session time zone.
        qt_time_before_version_2 """
            SELECT count(*), max(row_id) FROM ${table} FOR TIME AS OF '2026-09-19 13:06:08'
        """
        qt_time_before_version_3 """
            SELECT count(*), max(row_id) FROM ${table} FOR TIME AS OF '2026-09-19 13:06:10'
        """
        qt_time_after_version_3 """
            SELECT count(*), max(row_id) FROM ${table} FOR TIME AS OF '2030-01-01 00:00:00'
        """
        qt_time_millisecond_precision """
            SELECT count(*), max(row_id) FROM ${table} FOR TIME AS OF '2026-09-19 13:06:09.500'
        """
        // Commit times are compared at millisecond precision (version 2 is 09.113997): the
        // millisecond a commit lands in selects it, the one before does not.
        qt_time_exactly_at_commit_2 """
            SELECT count(*), max(row_id) FROM ${table} FOR TIME AS OF '2026-09-19 13:06:09.113'
        """
        qt_time_just_before_commit_2 """
            SELECT count(*), max(row_id) FROM ${table} FOR TIME AS OF '2026-09-19 13:06:09.112'
        """
        sql """SET time_zone = '+08:00'"""
        qt_time_session_time_zone """
            SELECT count(*), max(row_id) FROM ${table} FOR TIME AS OF '2026-09-19 21:06:10'
        """
        sql """SET time_zone = 'UTC'"""

        // Time travel across a delete: multi_frag.lance appends three fragments (versions 1-3)
        // and then deletes one row per fragment (version 4). An older version must show the
        // rows that a later deletion file removes, and COUNT(*) must follow the same version.
        String multiFrag = "`${catalogName}`.`default`.`multi_frag`"
        qt_multi_frag_latest """SELECT count(*) FROM ${multiFrag}"""
        qt_multi_frag_before_delete """SELECT count(*) FROM ${multiFrag} FOR VERSION AS OF 3"""
        order_qt_multi_frag_deleted_rows_before_delete """
            SELECT row_id FROM ${multiFrag} FOR VERSION AS OF 3 WHERE row_id IN (5, 15, 25)
        """
        order_qt_multi_frag_deleted_rows_after_delete """
            SELECT row_id FROM ${multiFrag} FOR VERSION AS OF 4 WHERE row_id IN (5, 15, 25)
        """
        order_qt_multi_frag_first_fragment_only """
            SELECT count(*), min(row_id), max(row_id) FROM ${multiFrag} FOR VERSION AS OF 1
        """
        explain {
            sql("SELECT row_id FROM ${multiFrag} FOR VERSION AS OF 2")
            contains "lanceVersion=2"
            contains "lanceFragments=2"
        }

        // Error cases.
        test {
            sql """SELECT count(*) FROM ${table} FOR VERSION AS OF 0"""
            exception "Lance FOR VERSION AS OF requires a positive version, but was 0"
        }
        // A signed number is still a version, not a tag name.
        test {
            sql """SELECT count(*) FROM ${table} FOR VERSION AS OF '-1'"""
            exception "Lance FOR VERSION AS OF requires a positive version, but was -1"
        }
        // As for Iceberg and Paimon, a non-numeric FOR VERSION AS OF names a tag.
        test {
            sql """SELECT count(*) FROM ${table} FOR VERSION AS OF 'abc'"""
            exception "Lance tag 'abc' of default.time_travel was not found"
        }
        test {
            sql """SELECT count(*) FROM ${table}@branch(dev) FOR VERSION AS OF 'abc'"""
            exception "FOR VERSION AS OF 'abc' names a tag, which cannot be combined with @branch"
        }
        test {
            sql """SELECT count(*) FROM ${table} FOR VERSION AS OF '99999999999999999999'"""
            exception "Lance FOR VERSION AS OF version 99999999999999999999 is out of range"
        }
        // The branch's chain starts where it was forked; earlier timestamps select nothing there.
        test {
            sql """SELECT count(*) FROM ${table}@branch(dev) FOR TIME AS OF '2026-09-19 13:06:10'"""
            exception "Lance branch 'dev' of default.time_travel has no version at or before '2026-09-19 13:06:10'"
        }
        test {
            sql """SELECT count(*) FROM ${table} FOR VERSION AS OF 99"""
            exception "Lance version 99 of default.time_travel was not found"
        }
        test {
            sql """SELECT count(*) FROM ${table} FOR TIME AS OF '2026-09-19 13:06:07'"""
            exception "Lance dataset has no version at or before '2026-09-19 13:06:07'"
        }
        test {
            sql """SELECT count(*) FROM ${table} FOR TIME AS OF 'not-a-time'"""
            exception "Cannot parse Lance FOR TIME AS OF value 'not-a-time'"
        }
        // A Lance tag (stored under _refs/tags/) resolves to the version it points at.
        order_qt_tag_v2 """SELECT * FROM ${table}@tag(v2)"""
        order_qt_version_as_of_tag_name """SELECT * FROM ${table} FOR VERSION AS OF 'v2'"""
        // "main" is the main chain, so a tag name in FOR VERSION AS OF still resolves.
        order_qt_branch_main_version_as_of_tag_name """SELECT * FROM ${table}@branch(main) FOR VERSION AS OF 'v2'"""
        qt_version_as_of_tag_on_branch """SELECT count(*), max(row_id) FROM ${table} FOR VERSION AS OF 'rel'"""
        test {
            sql """SELECT count(*) FROM ${table}@tag('name'='v1', 'x'='y')"""
            exception "Lance @tag takes exactly one name"
        }
        explain {
            sql("SELECT row_id FROM ${table}@tag(v2)")
            contains "lanceVersion=2"
        }
        test {
            sql """SELECT count(*) FROM ${table}@tag(no_such_tag)"""
            exception "Lance tag 'no_such_tag' of default.time_travel was not found"
        }
        // A Lance branch (a separate manifest chain under tree/<branch>/): "dev" forks from
        // version 2 and appends row 100, so its version 3 differs from main's version 3.
        order_qt_branch_dev """SELECT * FROM ${table}@branch(dev)"""
        order_qt_branch_dev_version_2 """SELECT * FROM ${table}@branch(dev) FOR VERSION AS OF 2"""
        explain {
            sql("SELECT row_id FROM ${table}@branch(dev)")
            contains "lanceBranch=dev"
            contains "lanceVersion=3"
            contains "lanceFragments=3"
        }
        order_qt_branch_vs_main """
            SELECT m.row_id AS main_row, d.row_id AS dev_row
            FROM ${table} FOR VERSION AS OF 3 m
            FULL OUTER JOIN ${table}@branch(dev) d ON m.row_id = d.row_id
            WHERE m.row_id IS NULL OR d.row_id IS NULL
        """
        // "main" is Lance's name for the main chain, and a branch can also be given in map form.
        qt_branch_main """SELECT count(*), max(row_id) FROM ${table}@branch(main)"""
        qt_branch_dev_map_form """SELECT count(*), max(row_id) FROM ${table}@branch('name'='dev')"""
        // FOR TIME AS OF inside a branch resolves against the branch's own commit times.
        qt_branch_dev_time """
            SELECT count(*), max(row_id) FROM ${table}@branch(dev) FOR TIME AS OF '2030-01-01 00:00:00'
        """
        // A tag that points into a branch selects that branch, not the same version number on main.
        order_qt_tag_on_branch """SELECT * FROM ${table}@tag(rel)"""
        explain {
            sql("SELECT row_id FROM ${table}@tag(rel)")
            contains "lanceBranch=dev"
            contains "lanceVersion=3"
        }
        test {
            sql """SELECT count(*) FROM ${table}@tag(v2) FOR VERSION AS OF 1"""
            exception "@tag cannot be combined with FOR VERSION AS OF or FOR TIME AS OF"
        }
        test {
            sql """SELECT count(*) FROM ${table}@branch(nope)"""
            exception "Lance branch 'nope' of default.time_travel was not found"
        }
        test {
            sql """SELECT count(*) FROM ${table}@branch(dev) FOR VERSION AS OF 9"""
            exception "Lance version 9 of default.time_travel@dev was not found"
        }
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """SET time_zone = '${originalTimeZone}'"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
