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

package org.apache.doris.changelog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.ivm.IvmRefreshSummary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

class ChangelogMonitorTest {
    @Test
    void publishesOnlyCompletedMasterSnapshots() {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        MTMV first = Mockito.mock(MTMV.class);
        MTMV second = Mockito.mock(MTMV.class);
        MTMV plainMv = Mockito.mock(MTMV.class);
        OlapTable plainTable = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(catalog.getDbs()).thenReturn(List.of(database));
        Mockito.when(database.getFullName()).thenReturn("sales");
        Mockito.when(database.getTables()).thenReturn(List.of(first, second, plainMv, plainTable));
        Mockito.when(first.isIvm()).thenReturn(true);
        Mockito.when(first.getId()).thenReturn(7L);
        Mockito.when(first.getName()).thenReturn("first");
        Mockito.when(first.getIvmRefreshMetricValues()).thenReturn(new IvmRefreshSummary.MetricValues(1, 1, 0, true, 0));
        Mockito.when(second.isIvm()).thenReturn(true);
        Mockito.when(second.getId()).thenReturn(8L);
        Mockito.when(second.getName()).thenReturn("second");
        Mockito.when(second.getIvmRefreshMetricValues())
                .thenReturn(new IvmRefreshSummary.MetricValues(0, 0, 0, false, 0));
        ChangelogMonitor monitor = new ChangelogMonitor(env);

        boolean enabled = Config.enable_changelog_monitor;
        int interval = Config.changelog_monitor_interval_seconds;
        try {
            Config.enable_changelog_monitor = true;
            Config.changelog_monitor_interval_seconds = 1;
            monitor.sample(0);
            Assertions.assertNull(monitor.getSnapshot());
            Mockito.verify(env, Mockito.never()).getInternalCatalog();

            Mockito.when(env.isMaster()).thenReturn(true);
            Assertions.assertNull(monitor.getSnapshot());
            monitor.sample(0);
            ChangelogMonitorSnapshot snapshot = monitor.getSnapshot();
            Assertions.assertEquals(List.of(7L, 8L),
                    snapshot.ivmRefreshes().stream().map(ChangelogMonitorSnapshot.IvmRefresh::mvId).toList());
            Assertions.assertSame(snapshot, monitor.getSnapshot());
            Mockito.verify(env).getInternalCatalog();

            Mockito.when(second.getIvmRefreshMetricValues()).thenThrow(new IllegalStateException("test failure"));
            monitor.sample(1_000_000_000L);
            Assertions.assertSame(snapshot, monitor.getSnapshot());

            Mockito.when(database.getTables()).thenReturn(List.of(first));
            Mockito.when(first.getName()).thenReturn("renamed");
            monitor.sample(2_000_000_000L);
            Assertions.assertEquals(List.of("renamed"),
                    monitor.getSnapshot().ivmRefreshes().stream()
                            .map(ChangelogMonitorSnapshot.IvmRefresh::mvName).toList());

            Config.enable_changelog_monitor = false;
            Assertions.assertNull(monitor.getSnapshot());
            monitor.reset();
            Config.enable_changelog_monitor = true;
            Assertions.assertNull(monitor.getSnapshot());
            monitor.sample(0);
            Assertions.assertNotNull(monitor.getSnapshot());
            Mockito.when(env.isMaster()).thenReturn(false);
            Assertions.assertNull(monitor.getSnapshot());
        } finally {
            Config.enable_changelog_monitor = enabled;
            Config.changelog_monitor_interval_seconds = interval;
        }
    }

    @Test
    void disablingThroughExperimentalConfigAliasClearsSnapshot() throws Exception {
        Env env = new Env(false) {
            @Override
            public boolean isMaster() {
                return true;
            }

            @Override
            public boolean isReady() {
                return true;
            }
        };
        ChangelogMonitor monitor = env.getChangelogMonitor();

        Field configField = Config.class.getField("enable_changelog_monitor");
        String alias = "experimental_enable_changelog_monitor";
        Map<String, Field> oldFields = ConfigBase.confFields;
        boolean enabled = Config.enable_changelog_monitor;
        try {
            ConfigBase.confFields = Map.of(alias, configField);
            Config.enable_changelog_monitor = true;
            monitor.sample(0);
            Assertions.assertNotNull(monitor.getSnapshot());

            env.setMutableConfigWithCallback(alias, "false");
            Assertions.assertNull(monitor.getSnapshot());
            env.setMutableConfigWithCallback(alias, "true");
            Assertions.assertNull(monitor.getSnapshot());

            monitor.sample(0);
            Assertions.assertNotNull(monitor.getSnapshot());
        } finally {
            Config.enable_changelog_monitor = enabled;
            ConfigBase.confFields = oldFields;
        }
    }
}
