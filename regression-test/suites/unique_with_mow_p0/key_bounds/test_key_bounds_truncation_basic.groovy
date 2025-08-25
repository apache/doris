
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
import org.apache.doris.regression.util.Http

suite("test_key_bounds_truncation_basic", "nonConcurrent") {

    // see be/src/util/key_util.h:50
    def keyNormalMarker = new String(new Byte[]{2})

    def tableName = "test_key_bounds_truncation_basic"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
    sql """ CREATE TABLE ${tableName} (
        `k` varchar(65533) NOT NULL,
        `v` int)
        UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES("replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"); """

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

    def truncateString = { String s, int l ->
        if (s.size() > l) {
            return s.substring(0, l)
        }
        return s
    }

    def checkKeyBounds = { String k1, String k2, int version, boolean doTruncation, int length, boolean expected ->
        def rowsetMeta = getRowsetMetas(version)
        def keyBounds = rowsetMeta.segments_key_bounds
        assertEquals(keyBounds.size(), 1)
        def bounds = keyBounds.get(0)
        
        String min_key = bounds.min_key
        String max_key = bounds.max_key

        String expected_min_key = keyNormalMarker + k1
        String expected_max_key = keyNormalMarker + k2
        if (doTruncation) {
            expected_min_key = truncateString(expected_min_key, length)
            expected_max_key = truncateString(expected_max_key, length)
        }

        logger.info("\nk1=${k1}, size=${k1.size()}, k2=${k2}, size=${k2.size()}")
        logger.info("\nexpected_min_key=${expected_min_key}, size=${expected_min_key.size()}, expected_max_key=${expected_max_key}, size=${expected_max_key.size()}")
        logger.info("\nmin_key=${min_key}, size=${min_key.size()}\nmax_key=${max_key}, size=${max_key.size()}")
        logger.info("\nsegments_key_bounds_truncated=${rowsetMeta.segments_key_bounds_truncated}, expected=${expected}")
        assertEquals(min_key, expected_min_key)
        assertEquals(max_key, expected_max_key)
        
        assertEquals(expected, rowsetMeta.segments_key_bounds_truncated)
    }

    int curVersion = 1

    // 1. turn off enable_segments_key_bounds_truncation, should not do truncation
    def customBeConfig = [
        segments_key_bounds_truncation_threshold : -1
    ]

    setBeConfigTemporary(customBeConfig) {
        String key1 = "aaaaaaaaaaaa"
        String key2 = "bbbbbbzzzzzzzzzzz"
        sql """insert into ${tableName} values("$key1", 1), ("$key2", 2);"""
        checkKeyBounds(key1, key2, ++curVersion, false, -1, false)
    }

    // 2. turn on enable_segments_key_bounds_truncation, should do truncation
    customBeConfig = [
        segments_key_bounds_truncation_threshold : 6
    ]

    setBeConfigTemporary(customBeConfig) {
        String key1 = "aa"
        String key2 = "bbbb"
        sql """insert into ${tableName} values("$key1", 1), ("$key2", 2);"""
        checkKeyBounds(key1, key2, ++curVersion, true, 6, false)

        key1 = "000000000000000"
        key2 = "1111111111111111111"
        sql """insert into ${tableName} values("$key1", 1), ("$key2", 2);"""
        checkKeyBounds(key1, key2, ++curVersion, true, 6, true)

        key1 = "xxx"
        key2 = "yyyyyyyyyyyyyyyyyyyyy"
        sql """insert into ${tableName} values("$key1", 1), ("$key2", 2);"""
        checkKeyBounds(key1, key2, ++curVersion, true, 6, true)

        key1 = "cccccccccccccccccccc"
        key2 = "dddd"
        sql """insert into ${tableName} values("$key1", 1), ("$key2", 2);"""
        checkKeyBounds(key1, key2, ++curVersion, true, 6, true)
    }

}
