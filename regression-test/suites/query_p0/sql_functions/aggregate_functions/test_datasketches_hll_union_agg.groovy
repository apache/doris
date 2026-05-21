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
    def varcharTableName = "test_datasketches_hll_union_agg_varchar_tbl"
    def emptyTableName = "test_datasketches_hll_union_agg_empty_tbl"
    def badTableName = "test_datasketches_hll_union_agg_bad_tbl"

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
            (3, NULL)
    """

    // 1) Basic union: {0..6} U {20..29} => 17 distinct values
    qt_basic_union """SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT) FROM ${tableName}"""

    // 2) Aliases should behave identically
    qt_aliases """SELECT
            CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT),
            CAST(ROUND(ds_hll_estimate(sk)) AS BIGINT),
            CAST(ROUND(datasketches_hll_estimate(sk)) AS BIGINT)
        FROM ${tableName}
    """

    // 3) Group-by
    qt_group_by """SELECT id, CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT)
        FROM ${tableName}
        WHERE id IN (1, 2)
        GROUP BY id
        ORDER BY id
    """

    // 4) DISTINCT should not change result in this data set
    sql "INSERT INTO ${tableName} VALUES (5, from_base64('${sk1Base64}'))"
    qt_distinct """SELECT
            CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT),
            CAST(ROUND(datasketches_hll_union_agg(DISTINCT sk)) AS BIGINT)
        FROM ${tableName}
    """

    // 4.1) Input type coverage: VARCHAR
    sql "DROP TABLE IF EXISTS ${varcharTableName}"
    sql """
        CREATE TABLE ${varcharTableName} (
            id INT,
            sk VARCHAR(256)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ${varcharTableName} VALUES
            (1, from_base64('${sk1Base64}')),
            (2, from_base64('${sk2Base64}')),
            (3, NULL)
    """

    qt_basic_union_varchar """SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT) FROM ${varcharTableName}"""

    qt_aliases_varchar """SELECT
            CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT),
            CAST(ROUND(ds_hll_estimate(sk)) AS BIGINT),
            CAST(ROUND(datasketches_hll_estimate(sk)) AS BIGINT)
        FROM ${varcharTableName}
    """

    // 4.2) Input type coverage: VARBINARY (no table column; VARBINARY is not supported for table storage)
    qt_basic_union_varbinary """SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT) FROM (
            SELECT from_base64_binary('${sk1Base64}') AS sk
            UNION ALL SELECT from_base64_binary('${sk2Base64}')
            UNION ALL SELECT NULL
        ) t
    """

    qt_aliases_varbinary """SELECT
            CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT),
            CAST(ROUND(ds_hll_estimate(sk)) AS BIGINT),
            CAST(ROUND(datasketches_hll_estimate(sk)) AS BIGINT)
        FROM (
            SELECT from_base64_binary('${sk1Base64}') AS sk
            UNION ALL SELECT from_base64_binary('${sk2Base64}')
            UNION ALL SELECT NULL
        ) t
    """

    // 5) Empty input should return 0
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
    qt_empty_input """SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT) FROM ${emptyTableName}"""

    // 6) Illegal input should throw (base64 is valid but bytes are not a datasketches HLL sketch)
    test {
        sql """SELECT datasketches_hll_union_agg(from_base64('AA=='))"""
        exception "CORRUPTION"
    }
    test {
        sql """SELECT ds_hll_estimate(from_base64('AA=='))"""
        exception "CORRUPTION"
    }
    test {
        sql """SELECT datasketches_hll_estimate(from_base64('AA=='))"""
        exception "CORRUPTION"
    }

    // Empty string is a valid STRING value, but it is an invalid serialized DataSketches HLL sketch.
    // It should not fail at INSERT time; it should fail when the aggregate function reads it.
    sql "DROP TABLE IF EXISTS ${badTableName}"
    sql """
        CREATE TABLE ${badTableName} (
            id INT,
            sk STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """INSERT INTO ${badTableName} VALUES (1, '')"""
    test {
        sql """SELECT datasketches_hll_union_agg(sk) FROM ${badTableName}"""
        exception "CORRUPTION"
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${varcharTableName}"
    sql "DROP TABLE IF EXISTS ${emptyTableName}"
    sql "DROP TABLE IF EXISTS ${badTableName}"
}
