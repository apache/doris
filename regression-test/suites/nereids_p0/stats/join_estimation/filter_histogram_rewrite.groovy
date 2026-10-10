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

// Filter rewrites column histogram so join sees consistent distribution and row count.
suite("filter_histogram_rewrite") {

    def wait_row_count_reported = { db, table, row, column, expected ->
        def result = sql """show frontends;"""
        def host
        def port
        for (int i = 0; i < result.size(); i++) {
            if (result[i][8] == "true") {
                host = result[i][1]
                port = result[i][4]
            }
        }
        def tokens = context.config.jdbcUrl.split('/')
        def url = tokens[0] + "//" + host + ":" + port
        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
            sql """use ${db}"""
            for (int i = 0; i < 120; i++) {
                Thread.sleep(5000)
                result = sql """SHOW DATA FROM ${table};"""
                if (result[row][column] == expected) {
                    return
                }
            }
            throw new Exception("Row count report timeout.")
        }
    }

    def joinCardinality = { String query ->
        def plan = sql("explain ${query}").collect { it[0].toString() }
        for (int i = 0; i < plan.size(); i++) {
            if (plan[i].contains("HASH JOIN") || plan[i].contains("NESTED LOOP JOIN")) {
                for (int j = i; j < Math.min(i + 8, plan.size()); j++) {
                    def matcher = plan[j] =~ /cardinality=([0-9,]+)/
                    if (matcher.find()) {
                        return Long.parseLong(matcher.group(1).replace(",", ""))
                    }
                }
            }
        }
        throw new IllegalStateException("no join node in the plan of: " + query)
    }

    def scalar = { String query ->
        return new BigDecimal(sql(query)[0][0].toString()).longValue()
    }

    def assertWithin = { String name, long estimate, long truth, double tolerance ->
        double error = Math.abs(estimate - truth) / (double) truth
        logger.info(String.format("%s: estimate=%d truth=%d error=%.4f%%", name, estimate, truth, error * 100))
        assertTrue(error <= tolerance,
                "${name}: estimate ${estimate}, truth ${truth}, error ${error * 100}% over ${tolerance * 100}%")
    }

    String db = "filter_histogram_rewrite"
    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""
    sql """set global enable_auto_analyze=false"""
    sql """set disable_nereids_rules='SALT_JOIN'"""
    sql """set enable_mcv_histogram=true"""

    for (String t : ["fl", "fr"]) {
        sql """
            CREATE TABLE ${t} (k INT NULL) ENGINE=OLAP DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        """
        // 200 distinct values so that a single value still holds enough rows for the
        // relative error of the estimate not to be dominated by rounding
        sql """
            INSERT INTO ${t}
            SELECT IF(number % 2 = 0, 0, number % 199 + 1) FROM numbers("number" = "10000")
        """
        wait_row_count_reported(db, t, 0, 4, "10000")
        sql """analyze table ${t}(k) with sample rows 400000 with sync"""
        sql """analyze table ${t}(k) with histogram with sync"""
    }

    // predicate on a, its rewrite of the histogram, and the join above it
    def check = { String name, String predicate, double tolerance ->
        long truth = scalar("""
            select ifnull(sum(cast(a.c as largeint) * b.c), 0)
            from (select k, count(*) c from fl where ${predicate.replace("a.k", "k")} group by k) a
            join (select k, count(*) c from fr group by k) b on a.k = b.k
        """)
        String query = "select count(*) from fl a join fr b on a.k = b.k where ${predicate}"
        sql """set enable_histogram_join_estimation=false"""
        long linear = joinCardinality(query)
        sql """set enable_histogram_join_estimation=true"""
        long rewritten = joinCardinality(query)
        logger.info("${name}: truth=${truth}, min/max share=${linear}, histogram=${rewritten}")
        assertWithin(name, rewritten, truth, tolerance)
    }

    // the value is not a hot value, so 1 / ndv gives it twice the rows it has
    check("a.k = 5", "a.k = 5", 0.05)
    // the whole hot value is kept, and the range holds three quarters of the rows, not half
    check("a.k < 100", "a.k < 100", 0.05)
    // the hot value is cut away, only the other values are left
    check("a.k >= 100", "a.k >= 100", 0.05)
    // three cold values, each with its share of the bucket holding it
    check("a.k in (5, 7, 9)", "a.k in (5, 7, 9)", 0.05)
    // the hot value is removed and the ratios of the rest are scaled to the rows kept
    check("a.k not in (0, 5)", "a.k not in (0, 5)", 0.05)

    sql """set enable_histogram_join_estimation=true"""
    sql """set enable_mcv_histogram=true"""
}
