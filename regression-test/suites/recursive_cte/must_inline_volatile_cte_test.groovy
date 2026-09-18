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

suite("must_inline_volatile_cte_test", "rec_cte") {
    // The cte is only referenced by the non-recursive term of the recursive cte, which is executed once.
    // uuid() lives in a materialized cte there, so the query is allowed.
    qt_anchor_only """
        WITH RECURSIVE
        u AS (SELECT uuid() AS v),
        r(n) AS (
            SELECT CAST(1 AS INT) FROM u
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM r WHERE n < 3
        )
        SELECT n FROM r ORDER BY n
    """

    // The reference of the volatile cte is eliminated together with the dead branch, so nothing has to
    // be inlined and the query only returns the anchor row.
    qt_dead_branch """
        WITH RECURSIVE
        u AS (SELECT uuid() AS v),
        r(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM r JOIN u ON TRUE WHERE n < 3 AND FALSE
        )
        SELECT n FROM r ORDER BY n
    """

    // The recursive child only uses k, so the volatile output v is pruned from the inlined copy
    // instead of rejecting the query.
    qt_unused_volatile_output """
        WITH RECURSIVE
        u AS (SELECT 1 AS k, uuid() AS v),
        r(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + u.k AS INT) FROM r JOIN u ON TRUE WHERE n < 3
        )
        SELECT n FROM r ORDER BY n
    """

    // The outer consumer needs v, the recursive child only needs k: the volatile producer stays
    // materialized for the outer consumer while a pruned copy is inlined into the recursive child.
    qt_mixed_consumer """
        WITH RECURSIVE
        u AS (SELECT 1 AS k, uuid() AS v),
        r(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + u.k AS INT) FROM r JOIN u ON TRUE WHERE n < 3
        )
        SELECT r.n FROM r, u WHERE u.v IS NOT NULL ORDER BY r.n
    """

    // The anchor never produces a row, so the recursive side is never executed and the volatile cte
    // is never read again: the query simply returns no rows.
    qt_empty_anchor """
        WITH RECURSIVE
        u AS (SELECT uuid() AS v),
        r(n) AS (
            SELECT CAST(1 AS INT) WHERE FALSE
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM r JOIN u ON TRUE WHERE n < 3
        )
        SELECT n FROM r ORDER BY n
    """

    // A stable udf returns the same value for the same arguments within a statement, so it is safe to
    // inline it into the recursive child even though it is not deterministic.
    def udfName = "must_inline_stable_udf"
    def jarPath = """${context.file.parent}/../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)
    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP FUNCTION IF EXISTS ${udfName}(int) """
        sql """ CREATE FUNCTION ${udfName}(int) RETURNS int PROPERTIES (
                "file"="file://${jarPath}",
                "symbol"="org.apache.doris.udf.IntTest",
                "type"="JAVA_UDF",
                "volatility"="STABLE"
            ) """
        qt_stable_udf """
            WITH RECURSIVE
            u AS (SELECT ${udfName}(1) AS v),
            v AS (SELECT v AS x FROM u),
            r(n) AS (
                SELECT CAST(1 AS INT)
                UNION ALL
                SELECT CAST(n + 1 AS INT) FROM r JOIN v ON TRUE WHERE n < 2
            )
            SELECT n FROM r ORDER BY n
        """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS ${udfName}(int)")
    }
}
