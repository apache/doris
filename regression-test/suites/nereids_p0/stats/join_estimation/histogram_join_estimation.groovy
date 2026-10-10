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

// Equi-join cardinality from key histograms (enable_histogram_join_estimation).
suite("histogram_join_estimation") {

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

    String db = "histogram_join_estimation"
    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""
    sql """set global enable_auto_analyze=false"""
    sql """set disable_nereids_rules='SALT_JOIN'"""
    sql """set enable_mcv_histogram=true"""

    // uniform keys of the same ndv over ranges that share half of their values
    sql """
        CREATE TABLE rng_lo (hj_k INT NULL) ENGINE=OLAP DUPLICATE KEY(`hj_k`)
        DISTRIBUTED BY HASH(`hj_k`) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO rng_lo SELECT number % 1000 FROM numbers("number" = "10000")"""
    sql """
        CREATE TABLE rng_hi (hj_k INT NULL) ENGINE=OLAP DUPLICATE KEY(`hj_k`)
        DISTRIBUTED BY HASH(`hj_k`) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO rng_hi SELECT number % 1000 + 500 FROM numbers("number" = "10000")"""
    wait_row_count_reported(db, "rng_lo", 0, 4, "10000")
    wait_row_count_reported(db, "rng_hi", 0, 4, "10000")
    for (String t : ["rng_lo", "rng_hi"]) {
        sql """analyze table ${t}(hj_k) with sample rows 400000 with sync"""
        sql """analyze table ${t}(hj_k) with histogram with sync"""
    }

    String query = "select count(*) from rng_lo a join rng_hi b on a.hj_k = b.hj_k"
    long truth = scalar("""
        select sum(cast(a.c as largeint) * b.c)
        from (select hj_k, count(*) c from rng_lo group by hj_k) a
        join (select hj_k, count(*) c from rng_hi group by hj_k) b on a.hj_k = b.hj_k
    """)

    sql """set enable_histogram_join_estimation=false"""
    long uniform = joinCardinality(query)
    sql """set enable_histogram_join_estimation=true"""
    long merged = joinCardinality(query)

    logger.info("half overlap: truth=${truth}, 1/max(ndv)=${uniform}, bucket merge=${merged}")
    assertWithin("half overlap", merged, truth, 0.05)
    assertTrue(uniform != merged, "the switch changed nothing, both estimates are ${uniform}")

    // one side without a histogram keeps the shape of the other, so the estimate stays sound
    sql """
        CREATE TABLE plain (hj_k INT NULL) ENGINE=OLAP DUPLICATE KEY(`hj_k`)
        DISTRIBUTED BY HASH(`hj_k`) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO plain SELECT number % 1000 FROM numbers("number" = "10000")"""
    wait_row_count_reported(db, "plain", 0, 4, "10000")
    sql """analyze table plain(hj_k) with sample rows 400000 with sync"""

    String oneSided = "select count(*) from rng_lo a join plain b on a.hj_k = b.hj_k"
    long oneSidedTruth = scalar("""
        select sum(cast(a.c as largeint) * b.c)
        from (select hj_k, count(*) c from rng_lo group by hj_k) a
        join (select hj_k, count(*) c from plain group by hj_k) b on a.hj_k = b.hj_k
    """)
    assertWithin("one side without histogram", joinCardinality(oneSided), oneSidedTruth, 0.05)

    // Without mcv_histogram, join uses full-row buckets.
    sql """set enable_mcv_histogram=false"""
    for (String t : ["rng_lo", "rng_hi"]) {
        sql """analyze table ${t}(hj_k) with histogram with sync"""
    }
    def plainStats = sql """
        SELECT buckets FROM internal.__internal_schema.histogram_statistics
        WHERE col_id = 'hj_k' AND buckets LIKE '%mcv_histogram%'
    """
    assertEquals(0, plainStats.size(), "the section is still stored: " + plainStats)
    assertWithin("half overlap, histogram without the section", joinCardinality(query), truth, 0.05)

    sql """set enable_mcv_histogram=true"""
    sql """set enable_histogram_join_estimation=true"""
}
