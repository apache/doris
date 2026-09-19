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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class RowBinlogTtlConfigTest {
    @Test
    public void propertiesSurviveCreateTableAnalysis() throws Exception {
        BinlogConfig config = new BinlogConfig();
        config.setEnable(true);
        config.setBinlogFormat(BinlogConfig.BinlogFormat.ROW);
        config.applyExplicitRowTtl(60);
        Map<String, String> properties = config.toProperties();
        Map<String, String> analyzed = PropertyAnalyzer.analyzeBinlogConfig(properties);
        Assertions.assertTrue(properties.isEmpty());
        Assertions.assertEquals(config, BinlogConfig.fromProperties(analyzed));
        Assertions.assertEquals(60L, config.toThrift().getTtlSeconds());
        Assertions.assertEquals(60L, config.toProtobuf().getTtlSeconds());
        TableProperty tableProperty = new TableProperty(new HashMap<>());
        tableProperty.setBinlogConfig(config);
        TableProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(tableProperty), TableProperty.class);
        Assertions.assertEquals(config, restored.getBinlogConfig());
    }

    @Test
    public void defaultRetentionSurvivesSerialization() {
        BinlogConfig config = new BinlogConfig(true, BinlogConfig.TTL_SECONDS, 1024, 10,
                BinlogConfig.BinlogFormat.ROW, false);
        Assertions.assertTrue(config.isRowTtlEnabled());
        Assertions.assertEquals(config, GsonUtils.GSON.fromJson(config.toString(), BinlogConfig.class));
        Assertions.assertEquals(config, BinlogConfig.fromProperties(config.toProperties()));
        Assertions.assertEquals(BinlogConfig.TTL_SECONDS, config.toThrift().getTtlSeconds());
        Assertions.assertEquals(BinlogConfig.TTL_SECONDS, config.toProtobuf().getTtlSeconds());
    }

    @Test
    public void rowRequestsRejectNonpositiveRetention() {
        BinlogConfig config = new BinlogConfig(true, 60, 1024, 10, BinlogConfig.BinlogFormat.ROW, false);
        Assertions.assertThrows(AnalysisException.class, () -> config.applyExplicitRowTtl(0));
        Assertions.assertThrows(AnalysisException.class, () -> config.applyExplicitRowTtl(-1));
        Assertions.assertEquals(60L, config.getTtlSeconds());
    }

    @Test
    public void ccrKeepsItsExistingRetentionValues() throws Exception {
        BinlogConfig ccr = new BinlogConfig();
        ccr.setEnable(true);
        ccr.applyExplicitRowTtl(-1);
        Assertions.assertEquals(-1L, ccr.getTtlSeconds());
        ccr.applyExplicitRowTtl(0);
        Assertions.assertEquals(0L, ccr.getTtlSeconds());
        Assertions.assertFalse(ccr.isRowTtlEnabled());
    }
}
