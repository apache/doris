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

package org.apache.doris.binlog;

import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class BinlogUtilsTest {
    @Test
    void testEffectiveStartTso() {
        Assertions.assertEquals(100L, BinlogUtils.effectiveStartTso(null, 100L, false));
        Assertions.assertEquals(100L, BinlogUtils.effectiveStartTso(99L, 100L, false));
        Assertions.assertEquals(101L, BinlogUtils.effectiveStartTso(101L, 100L, true));
        Assertions.assertEquals(100L, BinlogUtils.effectiveStartTso(100L, 100L, true));
    }

    @Test
    void testRejectExpiredMinDeltaOffset() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> BinlogUtils.effectiveStartTso(99L, 100L, true));
        Assertions.assertTrue(exception.getMessage().contains(BinlogUtils.ROW_BINLOG_OFFSET_EXPIRED));
    }

    @Test
    void testMarkExplicitRowTtl() {
        Map<String, String> properties = new HashMap<>();
        BinlogUtils.markExplicitRowTtl(properties);
        Assertions.assertFalse(properties.containsKey("binlog.row_ttl_enabled"));

        properties.put("binlog.ttl_seconds", "0");
        BinlogUtils.markExplicitRowTtl(properties);
        Assertions.assertEquals("true", properties.get("binlog.row_ttl_enabled"));

        properties.put("binlog.ttl_seconds", "-1");
        BinlogUtils.markExplicitRowTtl(properties);
        Assertions.assertEquals("false", properties.get("binlog.row_ttl_enabled"));
    }
}
