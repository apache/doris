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

suite("test_show_tablet") {
    sql """drop table if exists show_tablets_test_t;"""
    sql """create table show_tablets_test_t (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 5
            PROPERTIES (
                "replication_num" = "1"
            );"""

    def res = sql """ SHOW TABLETS FROM show_tablets_test_t """
    def noDbJdbcUrl = context.config.jdbcUrl.replaceFirst(/(jdbc:mysql:\/\/[^\/]+\/)[^?]*/, '$1')
    connect(context.config.jdbcUser, context.config.jdbcPassword, noDbJdbcUrl) {
        def tabletId = res[0][0]
        def tabletRes = sql """ SHOW TABLET ${tabletId} """
        assertTrue(tabletRes.size() == 1)
    }
    if (res.size() == 5) {
        // replication num == 1
        res = sql """SHOW TABLETS FROM show_tablets_test_t limit 5, 1;"""
        logger.info("result: " + res.toString());
        assertTrue(res.size() == 0)

        res = sql """SHOW TABLETS FROM show_tablets_test_t limit 3, 5;"""
        assertTrue(res.size() == 2)

        res = sql """SHOW TABLETS FROM show_tablets_test_t limit 10;"""
        assertTrue(res.size() == 5)
    } else if (res.size() == 15) {
        // in multi-be cluster and force_olap_table_replication_num=3
        // will change to 3 replication even though set "replication_num" = "1" in create table
        res = sql """SHOW TABLETS FROM show_tablets_test_t limit 15, 1;"""
        logger.info("result: " + res.toString());
        assertTrue(res.size() == 0)

        res = sql """SHOW TABLETS FROM show_tablets_test_t limit 13, 5;"""
        assertTrue(res.size() == 2)

        res = sql """SHOW TABLETS FROM show_tablets_test_t limit 15;"""
        assertTrue(res.size() == 15)
    } else {
        assertTrue(1 == 2)
    }

    // An explicit ORDER BY must be applied to the whole tablet set of the table, not to the
    // prefix that happens to be collected first while walking partitions and indexes.
    sql """drop table if exists show_tablets_multi_part_t;"""
    sql """create table show_tablets_multi_part_t (
                id INT,
                username VARCHAR(20)
            )
            DUPLICATE KEY(id)
            PARTITION BY RANGE(id) (
                PARTITION p1 VALUES LESS THAN (10),
                PARTITION p2 VALUES LESS THAN (20),
                PARTITION p3 VALUES LESS THAN (30)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            );"""

    def allTablets = sql """SHOW TABLETS FROM show_tablets_multi_part_t"""
    logger.info("all tablets: " + allTablets.toString())
    // 3 partitions * 3 buckets, one row per replica
    assertTrue(allTablets.size() >= 9)

    def allIds = allTablets.collect { it[0] as long }
    def ascIds = new ArrayList(allIds)
    Collections.sort(ascIds)
    def descIds = new ArrayList(ascIds)
    Collections.reverse(descIds)

    // Without ORDER BY and without LIMIT every tablet is collected anyway, so the whole result
    // is returned ordered by (TabletId, ReplicaId). Note this holds only for the unbounded
    // result -- see the LIMIT cases below, where no ordering is promised.
    assertEquals(ascIds, allIds)

    // ORDER BY without LIMIT returns every tablet
    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t ORDER BY TabletId DESC"""
    assertEquals(descIds, res.collect { it[0] as long })

    // ORDER BY ... LIMIT returns the globally largest tablet ids, not the largest ones
    // of the first partition scanned
    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t ORDER BY TabletId DESC LIMIT 3"""
    assertEquals(descIds.subList(0, 3), res.collect { it[0] as long })

    // OFFSET is applied after sorting
    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t ORDER BY TabletId DESC LIMIT 2, 3"""
    assertEquals(descIds.subList(2, 5), res.collect { it[0] as long })

    // Without ORDER BY the scan stops as soon as enough rows are gathered, so what comes back is
    // an arbitrary subset of the table -- which partition is walked first is not defined. The
    // rows are deliberately not sorted either, because sorting an arbitrary subset would make it
    // look like the globally smallest tablet ids. Only the row count and the fact that the rows
    // belong to this table are guaranteed.
    def assertAnySubsetOfTable = { rows, expectedSize ->
        assertEquals(expectedSize, rows.size())
        def ids = rows.collect { it[0] as long }
        assertTrue(allIds.containsAll(ids))
    }

    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t LIMIT 3"""
    assertAnySubsetOfTable(res, 3)

    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t LIMIT 2, 3"""
    assertAnySubsetOfTable(res, 3)

    // an offset past the end of the result yields no row
    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t LIMIT ${allTablets.size()}, 3"""
    assertTrue(res.isEmpty())

    // LIMIT 0 asks for no row and must not fall back to "no limit at all", with or without
    // an OFFSET and with or without an ORDER BY
    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t LIMIT 0"""
    assertTrue(res.isEmpty())

    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t LIMIT 2, 0"""
    assertTrue(res.isEmpty())

    res = sql """SHOW TABLETS FROM show_tablets_multi_part_t ORDER BY TabletId DESC LIMIT 0"""
    assertTrue(res.isEmpty())

    sql """drop table if exists show_tablets_multi_part_t;"""
}
