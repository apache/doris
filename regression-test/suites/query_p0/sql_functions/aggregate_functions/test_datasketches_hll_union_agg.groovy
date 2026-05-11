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

suite("test_datasketches_hll_union_agg") {
    def tableName = "test_datasketches_hll_union_agg_tbl"
    def emptyTableName = "test_datasketches_hll_union_agg_empty_tbl"

    // sk = new HllSketch(8, HLL_8); for (int i = 0; i < 7; i++) sk.update(i);
    def sk1Base64 = "AgEHCAMIBwjL18IEK/L7BoYv+Q11gWYHgbxdBntl5gj8LUIK"

    // sk = new HllSketch(8, HLL_8); for (int i = 20; i < 30; i++) sk.update(i);
    def sk2Base64 = "AwEHCAUIAAkKAAAAIjvrBcS1nwfGGWoEyHokBO8t9wc1qTEENkcJB7hWqQxZf9QNnuSbGA=="

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            sk STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
            (1, from_base64('${sk1Base64}')),
            (2, from_base64('${sk2Base64}')),
            (3, NULL),
            (4, '')
    """

    // 1) Basic union: {0..6} U {20..29} => 17 distinct values
    def res = sql "SELECT datasketches_hll_union_agg(sk) FROM ${tableName}"
    assertEquals(1, res.size())
    assertEquals(17L, ((Number) res[0][0]).longValue())

    // 2) Aliases should behave identically
    res = sql """
        SELECT
            datasketches_hll_union_agg(sk) = ds_hll_union_count(sk)
            AND ds_hll_union_count(sk) = ds_cardinality(sk)
        FROM ${tableName}
    """
    assertEquals(1, res.size())
    assertEquals(true, (Boolean) res[0][0])

    // 3) Group-by (single sketch estimate should be exact for these small n)
    res = sql """
        SELECT id, datasketches_hll_union_agg(sk)
        FROM ${tableName}
        WHERE id IN (1, 2)
        GROUP BY id
        ORDER BY id
    """
    assertEquals(2, res.size())
    assertEquals(1, ((Number) res[0][0]).intValue())
    assertEquals(7L, ((Number) res[0][1]).longValue())
    assertEquals(2, ((Number) res[1][0]).intValue())
    assertEquals(10L, ((Number) res[1][1]).longValue())

    // 4) DISTINCT should not change result in this data set
    sql "INSERT INTO ${tableName} VALUES (5, from_base64('${sk1Base64}'))"
    res = sql "SELECT datasketches_hll_union_agg(sk) FROM ${tableName}"
    assertEquals(17L, ((Number) res[0][0]).longValue())
    res = sql "SELECT datasketches_hll_union_agg(DISTINCT sk) FROM ${tableName}"
    assertEquals(17L, ((Number) res[0][0]).longValue())

    // 5) Empty input should return 0 (per FE resultForEmptyInput + BE state result)
    sql "DROP TABLE IF EXISTS ${emptyTableName}"
    sql """
        CREATE TABLE ${emptyTableName} (
            id INT,
            sk STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    res = sql "SELECT datasketches_hll_union_agg(sk) FROM ${emptyTableName}"
    assertEquals(1, res.size())
    assertEquals(0L, ((Number) res[0][0]).longValue())

    // 6) Illegal input should throw (base64 is valid but bytes are not a datasketches HLL sketch)
    test {
        sql """SELECT datasketches_hll_union_agg(from_base64('AA=='))"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }
    test {
        sql """SELECT ds_hll_union_count(from_base64('AA=='))"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }
    test {
        sql """SELECT ds_cardinality(from_base64('AA=='))"""
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${emptyTableName}"
}
