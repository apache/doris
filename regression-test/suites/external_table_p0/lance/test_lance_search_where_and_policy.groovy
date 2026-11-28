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

suite("test_lance_search_where_and_policy", "p0,external") {
    /*
     * Which filters filter a search's candidates, and the row policies a search can not apply.
     *
     * Every range of a search returns its own best candidates and a Doris TopN keeps the best of them. Only the
     * WHERE written on the search table function filters the candidates before that merge: every other filter
     * applies to the search's result, as SQL defines it, even where the optimizer moves it next to the merge.
     *
     * A search reads the table as a relation of its own, so the row policies Doris applies to a scan of the table
     * can not apply to it: it is refused while one applies to the user.
     *
     * Fixtures: doris.vs_ivf_pq_f32 (see test_lance_vector_search) and doris.full_text_search (see
     * test_lance_full_text_search). For row 1's vector the exact squared L2 distance of row n is 16 * (n - 1)^2,
     * so a flat search orders the rows by row_id; odd-category rows are the even row IDs. The rows matching
     * "lance" score 3 > 2 > 1 > 7.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance search WHERE and policy test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_search_where_and_policy"
    String vectorTable = "${catalogName}.doris.vs_ivf_pq_f32"
    String textTable = "${catalogName}.doris.full_text_search"
    String headQuery = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
    String topThree = """vector_search("table"="${vectorTable}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="3", "metric"="l2", "use_index"="false")"""
    def textSearch = { String topK ->
        """full_text_search("table"="${textTable}", "column"="body", "query"="lance", "top_k"="${topK}")"""
    }

    sql "DROP CATALOG IF EXISTS `${catalogName}`"
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

    // The queries below read every column of the search, so that no column pruning comes between a filter and the
    // merge: the optimizer then brings each filter right over the merge, and only the WHERE written on the search
    // may cross it.

    // The WHERE written on the search, here through its alias, filters the candidates: each fragment's flat
    // search returns its three nearest rows, 1-3 and 513-515, and the WHERE keeps 2 and 514 before the merge.
    order_qt_where_on_search """
        SELECT * FROM ${topThree} v WHERE v.category = 'odd'
    """

    // An outer WHERE applies to what the inner ORDER BY ... LIMIT returns, rows 3 and 2, although that TopN has
    // the search's order and is merged into the search's own TopN.
    order_qt_outer_where_over_limit """
        SELECT * FROM (
            SELECT * FROM ${textSearch("4")} ORDER BY _score DESC LIMIT 2
        ) s WHERE row_id < 3
    """

    // A CTE that both queries filter: the rewriter puts the OR of their filters on the CTE, which must stay over
    // the search's result, rows 1, 2 and 3.
    order_qt_cte_filtered_twice """
        WITH c AS (SELECT * FROM ${topThree})
        SELECT row_id FROM c WHERE row_id = 3
        UNION ALL
        SELECT row_id FROM c WHERE row_id = 513
    """

    // w.row_id > 2 and the join imply v.row_id > 2, which applies to the search's result, rows 1, 2 and 3.
    order_qt_inferred_through_join """
        SELECT v.*, w.label AS w_label FROM ${topThree} v JOIN ${vectorTable} w ON v.row_id = w.row_id
        WHERE w.row_id > 2
    """

    // The WHERE of a query that joins the search applies to the search's result; written on the search, in a
    // subquery, it filters the candidates.
    order_qt_where_over_join """
        SELECT v.*, w.label AS w_label FROM ${topThree} v JOIN ${vectorTable} w ON v.row_id = w.row_id
        WHERE v.category = 'odd'
    """
    order_qt_where_on_joined_search """
        SELECT v.*, w.label AS w_label
        FROM (SELECT * FROM ${topThree} WHERE category = 'odd') v
        JOIN ${vectorTable} w ON v.row_id = w.row_id
    """

    String user = "test_lance_search_policy_user"
    String password = "C123_567p"
    try_sql("DROP USER ${user}")
    sql "CREATE USER ${user} IDENTIFIED BY '${password}'"
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}"""
    }
    // connect() opens the regression database.
    sql "CREATE DATABASE IF NOT EXISTS internal.regression_test"
    sql "GRANT SELECT_PRIV ON internal.regression_test.* TO ${user}"
    sql "GRANT SELECT_PRIV ON ${textTable} TO ${user}"
    sql "DROP ROW POLICY IF EXISTS test_lance_search_row_policy ON ${textTable} FOR ${user}"

    // With SELECT on the table, and so on every column, and no row policy, the user can search.
    connect(user, password, context.config.jdbcUrl) {
        order_qt_user_search """SELECT row_id FROM ${textSearch("4")}"""
    }

    sql """
        CREATE ROW POLICY test_lance_search_row_policy ON ${textTable}
        AS RESTRICTIVE TO ${user} USING (row_id > 2)
    """
    connect(user, password, context.config.jdbcUrl) {
        test {
            sql """SELECT row_id FROM ${textSearch("4")}"""
            exception "full_text_search() is not supported on table '${textTable}' while row policies apply to it"
        }
        // A scan of the table applies the policy.
        order_qt_user_scan_with_policy """SELECT row_id FROM ${textTable} WHERE row_id < 5"""
    }
    // The policy is the user's: root searches as before.
    order_qt_root_search_with_policy """SELECT row_id FROM ${textSearch("4")}"""
}
