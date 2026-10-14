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
    public void publicationVersionSurvivesJournalImageAndWireFormats() {
        BinlogConfig config = new BinlogConfig(true, 60, 1024, 10, BinlogConfig.BinlogFormat.ROW, false);
        config.setConfigVersion(42);
        Assertions.assertEquals(config, new BinlogConfig(config));
        Assertions.assertEquals(config, BinlogConfig.fromProperties(config.toProperties()));
        Assertions.assertEquals(42, config.toThrift().getConfigVersion());
        Assertions.assertEquals(42, config.toProtobuf().getConfigVersion());
        TableProperty properties = new TableProperty(new HashMap<>());
        properties.setBinlogConfig(config);
        TableProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(properties), TableProperty.class);
        Assertions.assertEquals(config, restored.getBinlogConfig());
        Assertions.assertEquals(config, restored.buildBinlogConfig().getBinlogConfig());
        Map<String, String> legacy = new HashMap<>(config.toProperties());
        legacy.remove(BinlogConfig.CONFIG_VERSION);
        Assertions.assertEquals(0, BinlogConfig.fromProperties(legacy).getConfigVersion());
        BinlogConfig statement = new BinlogConfig();
        statement.mergeFromProperties(config.toProperties(), false);
        Assertions.assertEquals(0, statement.getConfigVersion(), "Statements cannot set the internal generation");
    }

    @Test
    public void pendingRetentionSurvivesJournalImageAndPropertyRebuild() {
        BinlogConfig config = new BinlogConfig(true, 60, 1024, 10, BinlogConfig.BinlogFormat.ROW, false);
        config.setConfigVersion(42);
        config.setPendingTtlSeconds(3600);

        Assertions.assertEquals(config, new BinlogConfig(config));
        Assertions.assertEquals(config, BinlogConfig.fromProperties(config.toProperties()));
        Assertions.assertEquals(60, config.getTtlSeconds());
        Assertions.assertEquals(3600, config.toThrift().getTtlSeconds());
        Assertions.assertEquals(3600, config.toProtobuf().getTtlSeconds());
        Assertions.assertEquals(42, config.toThrift().getConfigVersion());
        Assertions.assertEquals(42, config.toProtobuf().getConfigVersion());

        TableProperty properties = new TableProperty(new HashMap<>());
        properties.setBinlogConfig(config);
        TableProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(properties), TableProperty.class);
        Assertions.assertEquals(config, restored.getBinlogConfig());
        Assertions.assertEquals(config, restored.buildBinlogConfig().getBinlogConfig());

        Map<String, String> legacy = new HashMap<>(config.toProperties());
        legacy.remove(BinlogConfig.PENDING_TTL_SECONDS);
        BinlogConfig legacyConfig = BinlogConfig.fromProperties(legacy);
        Assertions.assertEquals(0, legacyConfig.getPendingTtlSeconds());
        Assertions.assertEquals(60, legacyConfig.toThrift().getTtlSeconds());
    }

    @Test
    public void completedPublicationClearsPendingRetentionWhenPropertiesAreMerged() {
        BinlogConfig pending = new BinlogConfig(true, 60, 1024, 10, BinlogConfig.BinlogFormat.ROW, false);
        pending.setConfigVersion(42);
        pending.setPendingTtlSeconds(3600);
        TableProperty properties = new TableProperty(new HashMap<>(pending.toProperties()));
        properties.buildBinlogConfig();

        BinlogConfig completed = new BinlogConfig(pending);
        completed.setTtlSeconds(3600);
        completed.setPendingTtlSeconds(0);
        Assertions.assertNotEquals(pending, completed);
        Assertions.assertEquals("0", completed.toProperties().get(BinlogConfig.PENDING_TTL_SECONDS));

        properties.modifyTableProperties(completed.toProperties());
        Assertions.assertEquals(completed, properties.buildBinlogConfig().getBinlogConfig());
        Assertions.assertEquals(0, properties.getBinlogConfig().getPendingTtlSeconds());
        Assertions.assertEquals(3600, properties.getBinlogConfig().toThrift().getTtlSeconds());
        Assertions.assertEquals(3600, properties.getBinlogConfig().toProtobuf().getTtlSeconds());
        TableProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(properties), TableProperty.class);
        Assertions.assertEquals(completed, restored.buildBinlogConfig().getBinlogConfig());
    }

    @Test
    public void statementsCannotSetOrClearPendingRetentionAndGeneration() {
        Map<String, String> internalProperties = new HashMap<>();
        internalProperties.put(BinlogConfig.CONFIG_VERSION, "99");
        internalProperties.put(BinlogConfig.PENDING_TTL_SECONDS, "7200");
        BinlogConfig config = new BinlogConfig();
        Assertions.assertTrue(config.mergeFromProperties(internalProperties, false).first);
        Assertions.assertEquals(0, config.getConfigVersion());
        Assertions.assertEquals(0, config.getPendingTtlSeconds());

        config.setConfigVersion(42);
        config.setPendingTtlSeconds(3600);
        internalProperties.put(BinlogConfig.PENDING_TTL_SECONDS, "0");
        Assertions.assertTrue(config.mergeFromProperties(internalProperties, false).first);
        Assertions.assertEquals(42, config.getConfigVersion());
        Assertions.assertEquals(3600, config.getPendingTtlSeconds());
    }

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
