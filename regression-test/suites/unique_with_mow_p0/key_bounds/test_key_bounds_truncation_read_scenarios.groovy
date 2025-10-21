
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

import com.google.common.collect.Maps
import org.apache.commons.lang.RandomStringUtils
import org.apache.doris.regression.util.Http
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_key_bounds_truncation_read_scenarios", "nonConcurrent") {

    def tableName = "test_key_bounds_truncation_read_scenarios"
    
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE `${tableName}` (
            `k1` int NOT NULL,
            `k2` int NOT NULL,
            `k3` int NOT NULL,
            `c1` int NOT NULL )
        ENGINE=OLAP UNIQUE KEY(k1,k2,k3)
        DISTRIBUTED BY HASH(k1,k2,k3) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction"="true",
        "store_row_column" = "true", "enable_mow_light_delete" = "false" );
    """

    def getRowsetMetas = { int version ->
        def metaUrl = sql_return_maparray("show tablets from ${tableName};").get(0).MetaUrl
        def jsonMeta = Http.GET(metaUrl, true, false)
        for (def meta : jsonMeta.rs_metas) {
            int end_version = meta.end_version
            if (end_version == version) {
                return meta
            }
        }
    }

    def checkKeyBounds = { int version, int length, boolean turnedOn ->
        def rowsetMeta = getRowsetMetas(version)
        def keyBounds = rowsetMeta.segments_key_bounds

        logger.info("\nversion=${version}, segments_key_bounds_truncated=${rowsetMeta.segments_key_bounds_truncated}, turnedOn=${turnedOn}")
        assertEquals(turnedOn, rowsetMeta.segments_key_bounds_truncated)

        for (def bounds : keyBounds) {
            String min_key = bounds.min_key
            String max_key = bounds.max_key
            logger.info("\nmin_key=${min_key}, size=${min_key.size()}\nmax_key=${max_key}, size=${max_key.size()}")
            assertTrue(min_key.size() <= length)
            assertTrue(max_key.size() <= length)
        }
    }


    def customBeConfig = [
        segments_key_bounds_truncation_threshold : 2
    ]

    setBeConfigTemporary(customBeConfig) {
        // 1. mow load
        int k1 = 3757202
        for (int j=1;j<=10;j++) {
            for (int i=1;i<=9;i++) {
                sql """insert into ${tableName} values
                    (${k1},${i},1,9),
                    (${k1},${i},2,8),
                    (${k1},${i},3,7)"""
            }
        }
        (2..91).each { idx ->
            checkKeyBounds(idx, 2, true)
        }
        qt_sql "select * from ${tableName} order by k1,k2,k3;"


        // 2. point lookup on mow table
        for (int i=1;i<=9;i++) {
            explain {
                sql """ select * from ${tableName} where k1=${k1} and k2=${i} and k3=1; """
                contains "SHORT-CIRCUIT"
            }
            qt_sql """ select * from ${tableName} where k1=${k1} and k2=${i} and k3=1; """
        }
    }
}
