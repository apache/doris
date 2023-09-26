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

suite("test_intersect2") {
    sql """ DROP TABLE IF EXISTS `test_intersect2` """
    sql """
        CREATE TABLE `test_intersect2` (
          `sku_code` varchar(200) NOT NULL,
          `site_code` varchar(200) NOT NULL,
          `is_lst` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sku_code`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`sku_code`, `site_code`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    streamLoad {
        table "test_intersect2"
        file  "test_intersect2.csv"
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }
    sql "sync"

    qt_select1 """
        select
            *
        from
            (
                (
                    SELECT
                        a.sku_code
                    FROM
                        (
                            SELECT
                                *
                            FROM
                                test_intersect2
                        ) a
                    WHERE
                        a.is_lst > 0
                        AND a.site_code = 'UK'
                    ORDER BY
                        a.sku_code
                )
                INTERSECT
                (
                    SELECT
                        b.sku_code
                    FROM
                        (
                            SELECT
                                *
                            FROM
                                test_intersect2
                        ) b
                    WHERE
                        b.is_lst >= 0
                        AND b.site_code = 'DE'
                    ORDER BY
                        b.sku_code
                )
                INTERSECT
                (
                    SELECT
                        c.sku_code
                    FROM
                        (
                            SELECT
                                *
                            FROM
                                test_intersect2
                        ) c
                    WHERE
                        c.is_lst >= 0
                        AND c.site_code = 'ES'
                    ORDER BY
                        c.sku_code
                )
            ) aa
        order by
            aa.sku_code;
    """
}
