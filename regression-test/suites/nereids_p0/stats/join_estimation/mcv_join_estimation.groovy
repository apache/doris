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

// Equi-join cardinality from MCV collision probability (enable_mcv_join_estimation).
suite("mcv_join_estimation") {

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

    // cardinality of the topmost join node of the plan
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

    String db = "mcv_join_estimation"
    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""
    sql """set global enable_auto_analyze=false"""
    // the skew rewrite would give the two switch settings different plan shapes
    sql """set disable_nereids_rules='SALT_JOIN'"""

    // 10000 rows on each side: the value 0 holds half of them, the values 1..999 share the rest
    for (String t : ["skew_l", "skew_r"]) {
        sql """
            CREATE TABLE ${t} (k INT NULL)
            ENGINE=OLAP DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO ${t}
            SELECT IF(number % 2 = 0, 0, number % 999 + 1) FROM numbers("number" = "10000")
        """
        wait_row_count_reported(db, t, 0, 4, "10000")
        // hot values are collected by a sample analyze, a full one leaves them null
        sql """analyze table ${t}(k) with sample rows 400000 with sync"""
    }

    def cachedStats = sql """show column cached stats skew_l(k)"""
    def hotValues = cachedStats[0][17].toString()
    logger.info("hot values of skew_l: " + hotValues)
    assertTrue(hotValues.contains("0.5"), "the skewed value was not collected: " + hotValues)

    String query = "select count(*) from skew_l a join skew_r b on a.k = b.k"
    long truth = scalar("""
        select sum(cast(a.c as largeint) * b.c)
        from (select k, count(*) c from skew_l group by k) a
        join (select k, count(*) c from skew_r group by k) b on a.k = b.k
    """)

    sql """set enable_mcv_join_estimation=false"""
    long uniform = joinCardinality(query)
    sql """set enable_mcv_join_estimation=true"""
    long collision = joinCardinality(query)

    logger.info("skewed key: truth=${truth}, 1/max(ndv)=${uniform}, collision probability=${collision}")
    assertWithin("skewed key", collision, truth, 0.05)
    assertTrue(uniform != collision, "the switch changed nothing, both estimates are ${uniform}")

    // a self join reads the same distribution on both sides
    String selfJoin = "select count(*) from skew_l a join skew_l b on a.k = b.k"
    long selfTruth = scalar("""
        select sum(cast(c as largeint) * c) from (select k, count(*) c from skew_l group by k) t
    """)
    assertWithin("self join", joinCardinality(selfJoin), selfTruth, 0.05)

    // the key of the join carries its distribution to the next join
    String chain = """
        select count(*) from skew_l a join skew_r b on a.k = b.k join skew_l c on b.k = c.k
    """
    long chainTruth = scalar("""
        select sum(cast(a.c as largeint) * b.c * c.c)
        from (select k, count(*) c from skew_l group by k) a
        join (select k, count(*) c from skew_r group by k) b on a.k = b.k
        join (select k, count(*) c from skew_l group by k) c on b.k = c.k
    """)
    assertWithin("three table chain", joinCardinality(chain), chainTruth, 0.05)

    sql """set enable_mcv_join_estimation=true"""
}
