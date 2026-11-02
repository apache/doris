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

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

public class ChangelogFeatureGateTest extends TestWithFeService {
    private boolean originalEnableFeatureChangelog;
    private boolean originalEnableFeatureBinlog;
    private Database db;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("changelog_gate");
        db = Env.getCurrentInternalCatalog().getDbOrDdlException("changelog_gate");
    }

    // The CCR switch stays on: it does not enable changelog any more.
    @BeforeEach
    public void disableChangelog() {
        originalEnableFeatureChangelog = Config.enable_feature_changelog;
        originalEnableFeatureBinlog = Config.enable_feature_binlog;
        Config.enable_feature_changelog = false;
        Config.enable_feature_binlog = true;
    }

    @AfterEach
    public void restoreChangelog() {
        Config.enable_feature_changelog = originalEnableFeatureChangelog;
        Config.enable_feature_binlog = originalEnableFeatureBinlog;
    }

    @Test
    public void testRejectExplicitChangelog() {
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> createChangelogTable(db, "explicit_changelog", "'changelog.enable'='true'"));
        Assertions.assertTrue(exception.getMessage().contains(
                "Cannot create table explicit_changelog with changelog when enable_feature_changelog=false"),
                exception.getMessage());
        Assertions.assertNull(db.getTableNullable("explicit_changelog"));
    }

    @Test
    public void testRejectChangelogCtas() {
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("CREATE TABLE changelog_gate.ctas_changelog "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'changelog.enable'='true') "
                        + "AS SELECT 1 AS k"));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_changelog"), exception.getMessage());
        Assertions.assertNull(db.getTableNullable("ctas_changelog"));
    }

    @Test
    public void testRejectInheritedChangelog() throws Exception {
        createDatabase("changelog_gate_inherited");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("changelog_gate_inherited");
        // Simulate database defaults restored from an image written while changelog was enabled.
        db.replayUpdateDbProperties(Map.of("changelog.enable", "true"));
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> createTable("CREATE TABLE changelog_gate_inherited.inherited_changelog (k INT) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_changelog"), exception.getMessage());
        Assertions.assertNull(db.getTableNullable("inherited_changelog"));
    }

    @Test
    public void testCcrDoesNotNeedChangelogSwitch() throws Exception {
        OlapTable table = createChangelogTable(db, "ccr_without_changelog", "'binlog.enable'='true'");
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(table.enableTso());
        alterTableSync("ALTER TABLE changelog_gate.ccr_without_changelog SET ('binlog.ttl_seconds'='123')");
        Assertions.assertEquals(123, table.getBinlogConfig().getTtlSeconds());
        Assertions.assertFalse(table.hasChangelog());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testChangelogCannotBeEnabledDynamically(boolean changelogFeature) throws Exception {
        String tableName = "plain_" + changelogFeature;
        OlapTable table = createChangelogTable(db, tableName, "'binlog.enable'='false'");
        Assertions.assertFalse(table.enableTso());
        Config.enable_feature_changelog = changelogFeature;
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> alterTableSync(
                "ALTER TABLE changelog_gate." + tableName + " SET ('changelog.enable'='true')"));
        Assertions.assertTrue(exception.getMessage().contains(changelogFeature
                ? "not support change changelog.enable from false to true" : "Changelog feature is not enabled"),
                exception.getMessage());
        Assertions.assertFalse(table.enableTso());
        Assertions.assertFalse(table.hasChangelog());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCcrCannotSwitchToChangelog(boolean changelogFeature) throws Exception {
        Config.enable_feature_changelog = changelogFeature;
        String tableName = "ccr_" + changelogFeature;
        OlapTable table = createChangelogTable(db, tableName, "'binlog.enable'='true'");
        Assertions.assertThrows(DdlException.class, () -> alterTableSync(
                "ALTER TABLE changelog_gate." + tableName + " SET ('changelog.enable'='true')"));
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(table.enableTso());
    }

    private OlapTable createChangelogTable(Database db, String name, String properties) throws Exception {
        createTable("CREATE TABLE " + db.getFullName() + "." + name + " (k INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', " + properties + ")");
        return (OlapTable) db.getTableOrDdlException(name);
    }
}
