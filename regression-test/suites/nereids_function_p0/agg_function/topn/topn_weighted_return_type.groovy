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

suite("topn_weighted_return_type") {
    sql "DROP TABLE IF EXISTS topn_weighted_return_type"
    sql """
        CREATE TABLE topn_weighted_return_type (
            id INT NOT NULL,
            g INT NOT NULL,
            d0 DATETIMEV2(0) NULL,
            d3 DATETIMEV2(3) NULL,
            d6 DATETIMEV2(6) NULL,
            amount DECIMAL(18, 3) NULL,
            weight BIGINT NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO topn_weighted_return_type VALUES
            (1, 1, '2024-01-01 00:00:00', '2024-01-01 00:00:00.123',
                '2024-01-01 00:00:00.123456', 1.25, 10),
            (2, 1, '2024-01-02 00:00:00', '2024-01-02 00:00:00.456',
                '2024-01-02 00:00:00.456789', 2.50, 40),
            (3, 2, '2024-01-01 00:00:00', '2024-01-01 00:00:00.123',
                '2024-01-01 00:00:00.123456', 1.25, 20),
            (4, 2, NULL, NULL, NULL, NULL, 5),
            (5, 3, NULL, NULL, NULL, NULL, 5)
    """

    [0, 3, 6].each { scale ->
        "order_qt_constant_${scale}" """
            SELECT topn_weighted(CAST('2024-01-01 00:00:00.123456' AS DATETIMEV2(${scale})),
                       CAST(1 AS BIGINT), 1),
                   topn_weighted(CAST('2024-01-01 00:00:00.123456' AS DATETIMEV2(${scale})),
                       CAST(1 AS BIGINT), 1, 100),
                   topn_weighted(CAST('2024-01-01 00:00:00.123456' AS DATETIMEV2(${scale})),
                       CAST(1 AS BIGINT), 1, 100)[1]
        """
    }

    ["d0", "d3", "d6", "amount"].each { column ->
        "order_qt_${column}_global" """
            SELECT topn_weighted(${column}, weight, 2),
                   topn_weighted(${column}, weight, 2, 100),
                   topn_weighted(${column}, weight, 2, 100)[1]
            FROM topn_weighted_return_type
        """
        "order_qt_${column}_grouped" """
            SELECT g, topn_weighted(${column}, weight, 2),
                   topn_weighted(${column}, weight, 2, 100),
                   topn_weighted(${column}, weight, 2, 100)[1]
            FROM topn_weighted_return_type GROUP BY g
        """
        "order_qt_${column}_empty" """
            SELECT topn_weighted(${column}, weight, 2),
                   topn_weighted(${column}, weight, 2, 100)
            FROM topn_weighted_return_type WHERE id < 0
        """
    }
}
