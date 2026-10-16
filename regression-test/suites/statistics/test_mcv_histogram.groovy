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

// ANALYZE ... WITH HISTOGRAM writes mcv_histogram when enable_mcv_histogram is on.
suite("test_mcv_histogram") {

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

    // the column name is unique so that the stored row can be found without the table id
    def storedBuckets = { ->
        def result = sql """
            SELECT buckets FROM internal.__internal_schema.histogram_statistics
            WHERE col_id = 'mcv_hist_k'
        """
        assertEquals(1, result.size())
        return result[0][0].toString()
    }

    sql """drop database if exists test_mcv_histogram"""
    sql """create database test_mcv_histogram"""
    sql """use test_mcv_histogram"""
    sql """set global enable_auto_analyze=false"""

    // 10000 rows: the value 0 holds half of them, the values 1..999 share the other half
    sql """
        CREATE TABLE mcv_hist_t (mcv_hist_k INT NULL)
        ENGINE=OLAP DUPLICATE KEY(`mcv_hist_k`)
        DISTRIBUTED BY HASH(`mcv_hist_k`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO mcv_hist_t
        SELECT IF(number % 2 = 0, 0, number % 999 + 1) FROM numbers("number" = "10000")
    """
    wait_row_count_reported("test_mcv_histogram", "mcv_hist_t", 0, 4, "10000")

    // switch off: the stored histogram is the one the fork writes, with no section
    sql """set enable_mcv_histogram=false"""
    sql """analyze table mcv_hist_t(mcv_hist_k) with histogram with sync"""
    def buckets = storedBuckets()
    logger.info("histogram without the section: " + buckets.substring(0, Math.min(120, buckets.length())))
    assertFalse(buckets.contains("mcv_histogram"))
    assertTrue(buckets.contains("num_buckets"))

    // switch on: the section carries the top hot_value_collect_count values and their own buckets
    sql """set enable_mcv_histogram=true"""
    sql """set hot_value_collect_count=3"""
    sql """analyze table mcv_hist_t(mcv_hist_k) with histogram with sync"""
    buckets = storedBuckets()
    logger.info("histogram with the section: " + buckets.substring(0, Math.min(200, buckets.length())))
    assertTrue(buckets.contains("mcv_histogram"))

    def mcvRow = sql """
        SELECT JSON_UNQUOTE(JSON_EXTRACT(buckets, '\$.mcv_histogram.mcv'))
        FROM internal.__internal_schema.histogram_statistics WHERE col_id = 'mcv_hist_k'
    """
    def mcv = mcvRow[0][0].toString()
    logger.info("mcv: " + mcv)
    String[] hotValues = mcv.split(";")
    // hot_value_collect_count of the session that submitted the statement, not the declared default
    assertEquals(3, hotValues.length)
    assertTrue(hotValues[0].startsWith("0 :0.5"), "the skewed value is missing from " + mcv)

    // the section's buckets hold the other values only, so they start above the hot value
    def sectionRow = sql """
        SELECT JSON_UNQUOTE(JSON_EXTRACT(buckets, '\$.mcv_histogram.buckets[0].lower'))
        FROM internal.__internal_schema.histogram_statistics WHERE col_id = 'mcv_hist_k'
    """
    def sectionLower = sectionRow[0][0].toString()
    logger.info("first bucket of the other values: " + sectionLower)
    assertNotEquals("0", sectionLower)

    sql """set hot_value_collect_count=10"""
    sql """set enable_mcv_histogram=true"""
}
