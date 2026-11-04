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

suite("test_lance_rest_search_snapshot", "p0,external") {
    /*
     * The search TVFs' snapshot selectors through a Lance REST Namespace, with and without
     * managed versioning. All tables are search_snapshot.lance (seven main versions, branch dev
     * with versions 4 to 6, tags rel and dev_rel; see test_lance_search_snapshot):
     *
     *   search_snapshot                  managed_versioning=false: versions come from _versions/
     *   search_snapshot_managed          managed_versioning=true, records every version and dev
     *   search_snapshot_managed_partial  managed_versioning=true, records every main version but 6
     *   unicode_branch_managed           managed_versioning=true, unicode_branch.lance: version 1
     *                                    has row_id 1..8; branch dev\u1c89 adds row_id 201..204
     *                                    in its version 2 (see lance_build_unicode_branch.py)
     *
     * For a managed table the namespace decides which versions exist, as for FOR VERSION AS OF
     * (see test_lance_rest_time_travel); the search TVFs follow the same rules.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance REST test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceRestPort = context.config.otherConfigs.get("lance_rest_port")
    String catalogName = "test_lance_rest_search_snapshot"
    String plain = "${catalogName}.`default`.search_snapshot"
    String managed = "${catalogName}.`default`.search_snapshot_managed"
    String partial = "${catalogName}.`default`.search_snapshot_managed_partial"
    String unicodeManaged = "${catalogName}.`default`.unicode_branch_managed"
    // U+1C89 was added in Unicode 16: Lance accepts the name, and JDK 17 leaves it unassigned.
    String unicodeBranch = "dev\u1c89"
    def vectorSearch = { String target, String selector, String query = "[0,1,2,3]" ->
        """vector_search(
                "table"="${target}",
                "column"="vec",
                "query_vector"="${query}",
                "top_k"="3",
                "metric"="l2",
                "nprobes"="2"${selector})"""
    }
    def fullTextSearch = { String target, String selector ->
        """full_text_search(
                "table"="${target}",
                "column"="body",
                "query"="lance",
                "top_k"="30",
                "coverage_mode"="index_only"${selector})"""
    }
    def scannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    String originalScannerV2 = scannerV2Rows[0][1].toString()
    String originalTimeZone = (sql """SHOW VARIABLES LIKE 'time_zone'""")[0][1].toString()
    String originalLazy = (sql """SHOW VARIABLES LIKE 'enable_lance_lazy_materialization'""")[0][1].toString()

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

        for (String target : [plain, managed]) {
            String managedFlag = target == managed ? "true" : "false"
            explain {
                sql("SELECT row_id FROM ${vectorSearch(target, "")}")
                contains "lanceCatalogType=rest"
                contains "lanceVersion=7"
                contains "lanceManagedVersioning=${managedFlag}"
            }
            explain {
                sql("SELECT row_id FROM ${vectorSearch(target, ', "tag"="rel"')}")
                contains "lanceVersion=5"
                contains "lanceManagedVersioning=${managedFlag}"
            }
            explain {
                sql("SELECT row_id FROM ${vectorSearch(target, ', "branch"="dev"')}")
                contains "lanceVersion=6"
                contains "lanceBranch=dev"
                contains "lanceManagedVersioning=${managedFlag}"
            }
            explain {
                sql("SELECT row_id FROM ${vectorSearch(target, ', "timestamp"="2026-09-29 14:51:58"')}")
                contains "lanceVersion=5"
            }
        }
        qt_plain_version_5 """
            SELECT row_id, _distance FROM ${vectorSearch(plain, ', "version"="5"', "[17,18,19,20]")}
            ORDER BY _distance, row_id
        """
        qt_managed_version_5 """
            SELECT row_id, _distance FROM ${vectorSearch(managed, ', "version"="5"', "[17,18,19,20]")}
            ORDER BY _distance, row_id
        """
        qt_managed_branch """
            SELECT row_id, _distance FROM ${vectorSearch(managed, ', "branch"="dev"', "[100,101,102,103]")}
            ORDER BY _distance, row_id
        """
        // Two-phase reads of a managed table: the second phase reopens the version and branch
        // directory the namespace resolved, with the first phase's vended credentials.
        sql """SET enable_lance_lazy_materialization = true"""
        String managedTwoPhase = """
            SELECT row_id, body, _distance
            FROM ${vectorSearch(managed, ', "branch"="dev"', "[100,101,102,103]")}
            ORDER BY _distance
            LIMIT 3
        """
        explain {
            sql "verbose ${managedTwoPhase}"
            contains "VMaterializeNode"
        }
        qt_managed_branch_two_phase "${managedTwoPhase}"
        qt_managed_version_two_phase """
            SELECT row_id, body, _distance
            FROM ${vectorSearch(managed, ', "version"="5"', "[17.25,18.25,19.25,20.25]")}
            ORDER BY _distance
            LIMIT 3
        """
        sql """SET enable_lance_lazy_materialization = ${originalLazy}"""
        order_qt_managed_branch_tag_fts """SELECT row_id FROM ${fullTextSearch(managed, ', "tag"="dev_rel"')}"""
        order_qt_managed_fts_version_4 """SELECT row_id FROM ${fullTextSearch(managed, ', "version"="4"')}"""

        // A managed branch whose name has a letter newer than the JDK's Unicode tables is opened by
        // the URI Doris joins, for table scans, searches and the second phase alike.
        order_qt_unicode_branch_scan """
            SELECT count(*), max(row_id) FROM ${unicodeManaged}@branch('name'='${unicodeBranch}')
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(unicodeManaged, ', "branch"="' + unicodeBranch + '"')}")
            contains "lanceVersion=2"
            contains "lanceBranch=${unicodeBranch}"
        }
        qt_unicode_branch_search """
            SELECT row_id, _distance
            FROM ${vectorSearch(unicodeManaged, ', "branch"="' + unicodeBranch + '"', "[200,201,202,203]")}
            ORDER BY _distance, row_id
        """
        sql """SET enable_lance_lazy_materialization = true"""
        String unicodeTwoPhase = """
            SELECT row_id, body, _distance
            FROM ${vectorSearch(unicodeManaged, ', "branch"="' + unicodeBranch + '"', "[200.25,201.25,202.25,203.25]")}
            ORDER BY _distance
            LIMIT 3
        """
        explain {
            sql "verbose ${unicodeTwoPhase}"
            contains "VMaterializeNode"
        }
        qt_unicode_branch_two_phase "${unicodeTwoPhase}"
        sql """SET enable_lance_lazy_materialization = ${originalLazy}"""

        // The namespace does not record version 6 of the partial table, so it cannot be searched
        // even though storage holds it; the latest version is still 7.
        explain {
            sql("SELECT row_id FROM ${vectorSearch(partial, "")}")
            contains "lanceVersion=7"
            contains "lanceManagedVersioning=true"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(partial, ', "version"="6"')}"""
            exception "Lance version 6 of default.search_snapshot_managed_partial was not found in the namespace"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(managed, ', "branch"="dev", "version"="9"')}"""
            exception "Lance version 9 of default.search_snapshot_managed@dev was not found in the namespace"
        }
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """SET time_zone = '${originalTimeZone}'"""
        sql """SET enable_lance_lazy_materialization = ${originalLazy}"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
