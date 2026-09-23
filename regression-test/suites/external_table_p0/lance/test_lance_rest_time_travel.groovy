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

suite("test_lance_rest_time_travel", "p0,external") {
    /*
     * Time travel through a Lance REST Namespace, with and without managed versioning.
     *
     * All three tables are the same time_travel.lance dataset in MinIO (three versions, see
     * test_lance_time_travel). They differ in what the REST fixture says about it:
     *
     *   time_travel                  managed_versioning=false: versions come from _versions/
     *   time_travel_managed          managed_versioning=true, namespace records versions 1, 2, 3
     *   time_travel_managed_partial  managed_versioning=true, namespace records versions 1, 3
     *   time_travel_managed_lagging  managed_versioning=true, namespace records versions 1, 2
     *   time_travel_managed_untimed  managed_versioning=true, records 1, 3 without commit times
     *   time_travel_managed_unprefixed  as time_travel_managed, credentials vended unprefixed
     *
     * For a managed table the FE opens the dataset through the namespace, so the latest
     * version is what ListTableVersions returns, FOR VERSION AS OF goes through
     * DescribeTableVersion, and FOR TIME AS OF picks from the commit times ListTableVersions
     * reports. A version the namespace does not record is unreachable even though its
     * manifest is still in storage. The BE then opens the resolved version by URI with the
     * vended credentials, exactly as for a storage-versioned table.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance REST test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceRestPort = context.config.otherConfigs.get("lance_rest_port")
    String catalogName = "test_lance_rest_time_travel"
    String plain = "`${catalogName}`.`default`.`time_travel`"
    String managed = "`${catalogName}`.`default`.`time_travel_managed`"
    String partial = "`${catalogName}`.`default`.`time_travel_managed_partial`"
    String lagging = "`${catalogName}`.`default`.`time_travel_managed_lagging`"
    String untimed = "`${catalogName}`.`default`.`time_travel_managed_untimed`"
    String unprefixed = "`${catalogName}`.`default`.`time_travel_managed_unprefixed`"
    def scannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    String originalScannerV2 = scannerV2Rows[0][1].toString()
    String originalTimeZone = (sql """SHOW VARIABLES LIKE 'time_zone'""")[0][1].toString()

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """SET enable_file_scanner_v2 = true"""
        sql """SET time_zone = 'UTC'"""
        // No static access key or secret key: every read below uses vended credentials.
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "rest",
                "lance.rest.uri" = "http://${externalEnvIp}:${lanceRestPort}",
                "lance.rest.security.type" = "bearer",
                "lance.rest.bearer-token" = "doris-lance-rest-test-token",
                "lance.namespace.root_database" = "default",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.region" = "us-east-1",
                "use_path_style" = "true",
                "test_connection" = "true"
            )
        """
        order_qt_tables """SHOW TABLES FROM `${catalogName}`.`default`"""

        // Storage-native versions through REST.
        qt_plain_latest """SELECT count(*), max(row_id) FROM ${plain}"""
        order_qt_plain_version_2 """SELECT * FROM ${plain} FOR VERSION AS OF 2"""
        qt_plain_time """
            SELECT count(*), max(row_id) FROM ${plain} FOR TIME AS OF '2026-09-19 13:06:10'
        """
        explain {
            sql("SELECT row_id FROM ${plain} FOR VERSION AS OF 2")
            contains "lanceCatalogType=rest"
            contains "lanceVersion=2"
            contains "lanceManagedVersioning=false"
        }

        // Namespace-managed versions: the namespace records all three.
        qt_managed_latest """SELECT count(*), max(row_id) FROM ${managed}"""
        explain {
            sql("SELECT row_id FROM ${managed}")
            contains "lanceVersion=3"
            contains "lanceManagedVersioning=true"
        }
        order_qt_managed_version_1 """SELECT * FROM ${managed} FOR VERSION AS OF 1"""
        order_qt_managed_version_2 """SELECT * FROM ${managed} FOR VERSION AS OF 2"""
        explain {
            sql("SELECT row_id FROM ${managed} FOR VERSION AS OF 2 WHERE row_id > 3")
            contains "lanceVersion=2"
            contains "lanceManagedVersioning=true"
            contains "lancePushdownPredicate="
        }
        qt_managed_version_with_predicate """
            SELECT count(*) FROM ${managed} FOR VERSION AS OF 2 WHERE row_id > 3
        """
        qt_managed_time """
            SELECT count(*), max(row_id) FROM ${managed} FOR TIME AS OF '2026-09-19 13:06:10'
        """
        order_qt_managed_self_join """
            SELECT a.row_id, a.tag, b.tag
            FROM ${managed} FOR VERSION AS OF 1 a
            JOIN ${managed} FOR VERSION AS OF 3 b ON a.row_id = b.row_id
        """
        test {
            sql """SELECT count(*) FROM ${managed} FOR VERSION AS OF 99"""
            exception "Lance version 99 of default.time_travel_managed was not found in the namespace"
        }

        // Namespace-managed versions where the namespace no longer records version 2.
        qt_partial_latest """SELECT count(*), max(row_id) FROM ${partial}"""
        order_qt_partial_version_3 """SELECT * FROM ${partial} FOR VERSION AS OF 3"""
        order_qt_partial_version_1 """SELECT * FROM ${partial} FOR VERSION AS OF 1"""
        test {
            sql """SELECT count(*) FROM ${partial} FOR VERSION AS OF 2"""
            exception "Lance version 2 of default.time_travel_managed_partial was not found in the namespace"
        }
        // FOR TIME AS OF resolves against the commit times the namespace records, so a timestamp
        // between the second and third commit selects version 1, the latest version the
        // namespace still knows at that time, rather than the dropped version 2.
        qt_partial_time_version_1 """
            SELECT count(*), max(row_id) FROM ${partial} FOR TIME AS OF '2026-09-19 13:06:10'
        """
        explain {
            sql("SELECT row_id FROM ${partial} FOR TIME AS OF '2026-09-19 13:06:10'")
            contains "lanceVersion=1"
            contains "lanceManagedVersioning=true"
        }
        qt_partial_time_version_3 """
            SELECT count(*), max(row_id) FROM ${partial} FOR TIME AS OF '2026-09-19 13:06:11'
        """
        test {
            sql """SELECT count(*) FROM ${partial} FOR TIME AS OF '2026-09-19 13:06:07'"""
            exception "Lance dataset has no version at or before '2026-09-19 13:06:07'"
        }

        // Namespace-managed versions where the namespace has not recorded version 3 yet even
        // though its manifest is in storage: the table's latest version is the namespace's
        // latest, by number and by time, and version 3 is not reachable.
        qt_lagging_latest """SELECT count(*), max(row_id) FROM ${lagging}"""
        explain {
            sql("SELECT row_id FROM ${lagging}")
            contains "lanceVersion=2"
            contains "lanceManagedVersioning=true"
        }
        qt_lagging_time_after_storage_version_3 """
            SELECT count(*), max(row_id) FROM ${lagging} FOR TIME AS OF '2026-09-19 13:06:11'
        """
        test {
            sql """SELECT count(*) FROM ${lagging} FOR VERSION AS OF 3"""
            exception "Lance version 3 of default.time_travel_managed_lagging was not found in the namespace"
        }

        // Tags live in the dataset's _refs/tags/ for managed tables too; the version a tag points
        // at is then resolved through the namespace, so the partial table's tag v2 points at a
        // version the namespace no longer records.
        order_qt_managed_tag_v1 """SELECT * FROM ${managed}@tag(v1)"""
        order_qt_plain_tag_v3 """SELECT row_id FROM ${plain}@tag(v3) WHERE row_id > 6"""
        test {
            sql """SELECT count(*) FROM ${managed}@tag(nope)"""
            exception "Lance tag 'nope' of default.time_travel_managed was not found"
        }
        test {
            sql """SELECT count(*) FROM ${partial}@tag(v2)"""
            exception "Lance version 2 of default.time_travel_managed_partial (tag 'v2') was not found in the namespace"
        }

        // Branches: the managed table's branch "dev" is recorded by the namespace (versions 2
        // and 3 under tree/dev/), the storage-versioned table's by its own tree/dev/ directory.
        qt_managed_branch_dev """SELECT count(*), max(row_id) FROM ${managed}@branch(dev)"""
        explain {
            sql("SELECT row_id FROM ${managed}@branch(dev)")
            contains "lanceBranch=dev"
            contains "lanceManagedVersioning=true"
            contains "lanceVersion=3"
        }
        qt_plain_branch_dev_version_2 """
            SELECT count(*), max(row_id) FROM ${plain}@branch(dev) FOR VERSION AS OF 2
        """
        qt_managed_branch_dev_time """
            SELECT count(*), max(row_id) FROM ${managed}@branch(dev) FOR TIME AS OF '2030-01-01 00:00:00'
        """
        // Tag "rel" points at version 3 of branch dev, which the namespace records for the branch.
        qt_managed_tag_on_branch """SELECT count(*), max(row_id) FROM ${managed}@tag(rel)"""
        test {
            sql """SELECT count(*) FROM ${managed}@branch(nope)"""
            exception "Lance branch 'nope' of default.time_travel_managed was not found"
        }

        // A namespace that lists versions without commit times: FOR TIME AS OF falls back to the
        // commit times in storage, but only among the versions the namespace lists, so the
        // instant between commits 2 and 3 still selects version 1, not the unlisted version 2.
        qt_untimed_latest """SELECT count(*), max(row_id) FROM ${untimed}"""
        qt_untimed_time_between_2_and_3 """
            SELECT count(*), max(row_id) FROM ${untimed} FOR TIME AS OF '2026-09-19 13:06:10'
        """
        explain {
            sql("SELECT row_id FROM ${untimed} FOR TIME AS OF '2026-09-19 13:06:10'")
            contains "lanceVersion=1"
            contains "lanceManagedVersioning=true"
        }

        // Managed versioning with credentials vended under the unprefixed spelling: the SDK
        // overlays the namespace's options on the ones Doris normalized, and both readers must
        // still be able to open the dataset.
        qt_unprefixed_latest """SELECT count(*), max(row_id) FROM ${unprefixed}"""
        order_qt_unprefixed_version_2 """SELECT * FROM ${unprefixed} FOR VERSION AS OF 2"""

        String showCreate = sql("""SHOW CREATE CATALOG `${catalogName}`""")[0][1].toString()
        assertFalse(showCreate.contains("doris-lance-rest-test-token"))
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """SET time_zone = '${originalTimeZone}'"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
