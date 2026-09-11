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

import java.util.Map;

public class RowBinlogTtlConfigTest {
    @Test
    public void normalizedPropertiesSurviveCreateTableAnalysis() throws Exception {
        BinlogConfig config = new BinlogConfig();
        config.setEnable(true);
        config.setBinlogFormat(BinlogConfig.BinlogFormat.ROW);
        config.applyExplicitRowTtl(60);
        Map<String, String> properties = config.toProperties();
        Map<String, String> analyzed = PropertyAnalyzer.analyzeBinlogConfig(properties);
        Assertions.assertTrue(properties.isEmpty());
        Assertions.assertEquals(config, BinlogConfig.fromProperties(analyzed));
    }

    @Test
    public void historicalOneDayDefaultDoesNotActivateTtl() {
        BinlogConfig config = GsonUtils.GSON.fromJson(
                "{\"enable\":true,\"binlogFormat\":\"ROW\",\"ttlSeconds\":86400}", BinlogConfig.class);
        Assertions.assertFalse(config.isRowTtlEnabled());
        Assertions.assertEquals(-1L, config.toThrift().getEffectiveRowTtlSeconds());
        Assertions.assertEquals(-1L, config.toProtobuf().getEffectiveRowTtlSeconds());
        Assertions.assertFalse(BinlogConfig.fromProperties(config.toProperties()).isRowTtlEnabled());
        Assertions.assertFalse(GsonUtils.GSON.fromJson(config.toString(), BinlogConfig.class).isRowTtlEnabled());
    }

    @Test
    public void historicalZeroSurvivesButNewRowRequestsRejectIt() {
        BinlogConfig config = GsonUtils.GSON.fromJson(
                "{\"enable\":true,\"binlogFormat\":\"ROW\",\"ttlSeconds\":0,\"rowTtlEnabled\":true}",
                BinlogConfig.class);
        Assertions.assertTrue(config.isRowTtlEnabled());
        Assertions.assertEquals(0L, config.getEffectiveRowTtlSeconds());
        Assertions.assertThrows(AnalysisException.class, () -> config.applyExplicitRowTtl(0));
        Assertions.assertThrows(AnalysisException.class, () -> config.applyExplicitRowTtl(-1));
        Assertions.assertEquals(0L, config.getEffectiveRowTtlSeconds());
    }

    @Test
    public void ccrKeepsItsExistingValuesAndNormalizedMetadataWins() throws Exception {
        BinlogConfig ccr = new BinlogConfig();
        ccr.setEnable(true);
        ccr.applyExplicitRowTtl(-1);
        Assertions.assertEquals(-1L, ccr.getTtlSeconds());
        ccr.applyExplicitRowTtl(0);
        Assertions.assertEquals(0L, ccr.getTtlSeconds());
        Assertions.assertFalse(ccr.isRowTtlEnabled());
        BinlogConfig normalized = GsonUtils.GSON.fromJson(
                "{\"enable\":true,\"binlogFormat\":\"ROW\",\"ttlSeconds\":86400,"
                        + "\"rowTtlEnabled\":true,\"effectiveRowTtlSeconds\":-1}", BinlogConfig.class);
        Assertions.assertFalse(normalized.isRowTtlEnabled());
        normalized.applyExplicitRowTtl(12);
        Assertions.assertEquals(12L, normalized.getEffectiveRowTtlSeconds());
        normalized.setTtlSeconds(86400);
        StringBuilder ddl = new StringBuilder();
        normalized.appendToShowCreateTable(ddl);
        Assertions.assertTrue(ddl.toString().contains("\"binlog.ttl_seconds\" = \"12\""));
        TableProperty properties = new TableProperty(normalized.toProperties());
        Assertions.assertEquals(12L, properties.buildBinlogConfig().getBinlogConfig().getEffectiveRowTtlSeconds());
    }
}
