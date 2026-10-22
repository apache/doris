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

public class ChangelogTtlConfigTest {
    @Test
    public void publicationVersionSurvivesJournalImageAndWireFormats() {
        ChangelogConfig config = new ChangelogConfig(true, 60, false);
        config.setConfigVersion(42);
        Assertions.assertEquals(config, new ChangelogConfig(config));
        Assertions.assertEquals("42", config.toProperties().get(ChangelogConfig.CONFIG_VERSION));
        TabletBinlogConfig wire = TabletBinlogConfig.of(new BinlogConfig(), config);
        Assertions.assertEquals(42, wire.toThrift().getConfigVersion());
        Assertions.assertEquals(42, wire.toProtobuf().getConfigVersion());
        TableProperty properties = new TableProperty(new HashMap<>());
        properties.setChangelogConfig(config);
        TableProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(properties), TableProperty.class);
        Assertions.assertEquals(config, restored.getChangelogConfig());
        Assertions.assertEquals(config, restored.buildBinlogConfig().getChangelogConfig());
        // The journal of an ALTER carries the generation, the one of a statement never sets it.
        Map<String, String> statement = new HashMap<>(config.toProperties());
        Assertions.assertEquals(0, ChangelogConfig.fromProperties(statement).getConfigVersion(),
                "Statements cannot set the internal generation");
        statement.remove(ChangelogConfig.CONFIG_VERSION);
        TableProperty withoutVersion = new TableProperty(statement);
        Assertions.assertEquals(0, withoutVersion.getChangelogConfig().getConfigVersion());
    }

    @Test
    public void newTableDoesNotStoreTheGeneration() {
        ChangelogConfig config = new ChangelogConfig(true, ChangelogConfig.TTL_SECONDS, true);
        Map<String, String> properties = config.toProperties();
        Assertions.assertFalse(properties.containsKey(ChangelogConfig.CONFIG_VERSION));
        Assertions.assertEquals("true", properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE));
        Assertions.assertEquals("86400", properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS));
        Assertions.assertEquals("true", properties.get(PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE));
        Assertions.assertEquals(3, properties.size());
    }

    @Test
    public void propertiesSurviveCreateTableAnalysis() throws Exception {
        ChangelogConfig config = new ChangelogConfig(true, ChangelogConfig.TTL_SECONDS, false);
        config.applyExplicitTtl(60);
        Map<String, String> properties = config.toProperties();
        Map<String, String> analyzed = PropertyAnalyzer.analyzeChangelogConfig(properties);
        Assertions.assertTrue(properties.isEmpty());
        Assertions.assertEquals(config, ChangelogConfig.fromProperties(analyzed));
        TabletBinlogConfig wire = TabletBinlogConfig.of(new BinlogConfig(), config);
        Assertions.assertEquals(60L, wire.toThrift().getTtlSeconds());
        Assertions.assertEquals(60L, wire.toProtobuf().getTtlSeconds());
        TableProperty tableProperty = new TableProperty(new HashMap<>());
        tableProperty.setChangelogConfig(config);
        TableProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(tableProperty), TableProperty.class);
        Assertions.assertEquals(config, restored.getChangelogConfig());
    }

    @Test
    public void defaultRetentionSurvivesSerialization() {
        ChangelogConfig config = new ChangelogConfig(true, ChangelogConfig.TTL_SECONDS, false);
        Assertions.assertTrue(config.isTtlEnabled());
        Assertions.assertEquals(config, GsonUtils.GSON.fromJson(config.toString(), ChangelogConfig.class));
        Assertions.assertEquals(config, ChangelogConfig.fromProperties(config.toProperties()));
        TabletBinlogConfig wire = TabletBinlogConfig.of(new BinlogConfig(), config);
        Assertions.assertEquals(ChangelogConfig.TTL_SECONDS, wire.toThrift().getTtlSeconds());
        Assertions.assertEquals(ChangelogConfig.TTL_SECONDS, wire.toProtobuf().getTtlSeconds());
    }

    @Test
    public void changelogRequestsRejectNonpositiveRetention() {
        ChangelogConfig config = new ChangelogConfig(true, 60, false);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> config.applyExplicitTtl(0));
        Assertions.assertTrue(exception.getMessage().contains("changelog.ttl_seconds must be greater than 0"),
                exception.getMessage());
        Assertions.assertThrows(AnalysisException.class, () -> config.applyExplicitTtl(-1));
        Assertions.assertEquals(60L, config.getTtlSeconds());
    }

    @Test
    public void disabledChangelogKeepsAnyRetentionAsDefault() throws Exception {
        // A database keeps the retention it is given while it does not enable changelog.
        ChangelogConfig disabled = new ChangelogConfig();
        disabled.applyExplicitTtl(-1);
        Assertions.assertEquals(-1L, disabled.getTtlSeconds());
        disabled.applyExplicitTtl(0);
        Assertions.assertEquals(0L, disabled.getTtlSeconds());
        Assertions.assertFalse(disabled.isTtlEnabled());
    }

    @Test
    public void alterCanOnlyChangeTheRetention() throws Exception {
        ChangelogConfig config = new ChangelogConfig(true, 60, true);
        config.alterFromProperties(Map.of(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE, "true",
                PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE, "true",
                PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS, "30"));
        Assertions.assertEquals(new ChangelogConfig(true, 30, true), config);

        AnalysisException disable = Assertions.assertThrows(AnalysisException.class,
                () -> config.alterFromProperties(Map.of(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE, "false")));
        Assertions.assertTrue(disable.getMessage().contains("not support change changelog.enable from true to false"),
                disable.getMessage());
        AnalysisException historical = Assertions.assertThrows(AnalysisException.class,
                () -> config.alterFromProperties(Map.of(
                        PropertyAnalyzer.PROPERTIES_CHANGELOG_NEED_HISTORICAL_VALUE, "false")));
        Assertions.assertTrue(historical.getMessage().contains(
                "not support change changelog.need_historical_value from true to false"), historical.getMessage());
        Assertions.assertThrows(AnalysisException.class, () -> config.alterFromProperties(
                Map.of(PropertyAnalyzer.PROPERTIES_CHANGELOG_TTL_SECONDS, "0")));
        Assertions.assertEquals(new ChangelogConfig(true, 30, true), config);

        ChangelogConfig plain = new ChangelogConfig();
        AnalysisException enable = Assertions.assertThrows(AnalysisException.class,
                () -> plain.alterFromProperties(Map.of(PropertyAnalyzer.PROPERTIES_CHANGELOG_ENABLE, "true")));
        Assertions.assertTrue(enable.getMessage().contains("not support change changelog.enable from false to true"),
                enable.getMessage());
    }
}
