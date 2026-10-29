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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ChangelogIvmMonitorTest {
    private boolean originEnableChangelog;
    private boolean originEnableMonitor;
    private int originInterval;

    @BeforeEach
    public void setUp() {
        originEnableChangelog = Config.enable_feature_changelog;
        originEnableMonitor = Config.enable_changelog_ivm_monitor;
        originInterval = Config.changelog_ivm_monitor_interval_seconds;
        Config.enable_feature_changelog = true;
        Config.enable_changelog_ivm_monitor = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_feature_changelog = originEnableChangelog;
        Config.enable_changelog_ivm_monitor = originEnableMonitor;
        Config.changelog_ivm_monitor_interval_seconds = originInterval;
    }

    @Test
    public void testRoundSumsTheChangelogShowDataReports() {
        OlapTable first = tableWithChangelog(100L, 300L);
        OlapTable second = tableWithChangelog(20L, 60L);
        OlapTable withoutChangelog = tableWithChangelog(0L, 0L);
        Database firstDb = Mockito.mock(Database.class);
        Mockito.when(firstDb.getTables()).thenReturn(List.of(first, Mockito.mock(OlapTableStream.class)));
        Database secondDb = Mockito.mock(Database.class);
        Mockito.when(secondDb.getTables()).thenReturn(List.of(second, withoutChangelog));
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbs()).thenReturn(List.of(firstDb, secondDb));
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ChangelogIvmMonitor monitor = new ChangelogIvmMonitor();
            long startMs = System.currentTimeMillis();
            monitor.sample();
            ChangelogIvmMonitorSnapshot snapshot = monitor.getSnapshot();
            Assertions.assertEquals(120L, snapshot.getChangelogSize());
            Assertions.assertEquals(360L, snapshot.getTotalReplicaChangelogSize());
            // SHOW DATA lists the changelog of all the replicas of each table
            Assertions.assertEquals(first.getChangelogSize() + second.getChangelogSize()
                    + withoutChangelog.getChangelogSize(), snapshot.getTotalReplicaChangelogSize());
            Assertions.assertTrue(snapshot.getSampleTimeMs() >= startMs);
            Assertions.assertTrue(snapshot.getSampleDurationMs() >= 0);
            Assertions.assertEquals(0L, monitor.getSampleFailures());
        }
    }

    @Test
    public void testFailedRoundKeepsThePreviousSnapshot() {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbs()).thenReturn(List.of())
                .thenThrow(new IllegalStateException("injected sampling failure"))
                .thenReturn(List.of());
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ChangelogIvmMonitor monitor = new ChangelogIvmMonitor();
            monitor.sample();
            ChangelogIvmMonitorSnapshot first = monitor.getSnapshot();
            Assertions.assertNotNull(first);

            monitor.sample();
            Assertions.assertSame(first, monitor.getSnapshot());
            Assertions.assertEquals(1L, monitor.getSampleFailures());

            monitor.sample();
            Assertions.assertNotSame(first, monitor.getSnapshot());
            Assertions.assertEquals(1L, monitor.getSampleFailures());
        }
    }

    @Test
    public void testRoundsFollowTheIntervalAndStopWhileDisabled() {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbs()).thenReturn(List.of());
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ChangelogIvmMonitor monitor = new ChangelogIvmMonitor();
            Config.changelog_ivm_monitor_interval_seconds = 3600;
            monitor.runAfterCatalogReady();
            ChangelogIvmMonitorSnapshot first = monitor.getSnapshot();
            Assertions.assertNotNull(first);
            // within the interval
            monitor.runAfterCatalogReady();
            Assertions.assertSame(first, monitor.getSnapshot());
            // a shorter interval applies at the next check, not after the old interval
            Config.changelog_ivm_monitor_interval_seconds = 0;
            monitor.runAfterCatalogReady();
            Assertions.assertNotSame(first, monitor.getSnapshot());
            Mockito.verify(catalog, Mockito.times(2)).getDbs();

            // Disabled, the monitor samples nothing and drops the snapshot it has, so that it does not export the
            // snapshot once enabled again; the same without the changelog feature.
            Config.enable_changelog_ivm_monitor = false;
            monitor.runAfterCatalogReady();
            Assertions.assertNull(monitor.getSnapshot());
            Config.enable_changelog_ivm_monitor = true;
            Config.enable_feature_changelog = false;
            monitor.runAfterCatalogReady();
            Assertions.assertNull(monitor.getSnapshot());
            Mockito.verify(catalog, Mockito.times(2)).getDbs();

            Config.enable_feature_changelog = true;
            monitor.runAfterCatalogReady();
            Assertions.assertNotNull(monitor.getSnapshot());
        }
    }

    private static OlapTable tableWithChangelog(long changelogSize, long totalReplicaChangelogSize) {
        OlapTable table = new OlapTable();
        table.setStatistics(new OlapTable.Statistics("db", "table", 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                changelogSize, totalReplicaChangelogSize));
        return table;
    }
}
