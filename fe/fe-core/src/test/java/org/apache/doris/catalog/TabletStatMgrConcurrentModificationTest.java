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

import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TabletStatMgrConcurrentModificationTest {

    private static class FakeDatabase extends Database {
        private final List<Table> tables;

        FakeDatabase(long id, String name, List<Table> tables) {
            super(id, name);
            this.tables = tables;
        }

        @Override
        public List<Table> getTables() {
            return tables;
        }
    }

    @Test
    public void testRunAfterCatalogReadyNoConcurrentModificationException() throws Exception {
        List<Table> sharedTables = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < 2000; i++) {
            BrokerTable table = new BrokerTable();
            table.setId(i);
            table.setName("dummy_" + i);
            sharedTables.add(table);
        }

        FakeDatabase fakeDb = new FakeDatabase(1L, "db1", sharedTables);
        List<Long> dbIds = Collections.singletonList(1L);

        SystemInfoService systemInfoService = new SystemInfoService();
        InternalCatalog internalCatalog = new InternalCatalog();

        new MockUp<Env>() {
            @Mock
            public static SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }

            @Mock
            public static InternalCatalog getCurrentInternalCatalog() {
                return internalCatalog;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ImmutableMap<Long, Backend> getAllBackendsByAllCluster() {
                return ImmutableMap.of();
            }
        };

        new MockUp<InternalCatalog>() {
            @Mock
            public List<Long> getDbIds() {
                return dbIds;
            }

            @Mock
            public Database getDbNullable(long dbId) {
                return fakeDb;
            }
        };

        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        Thread modifier = new Thread(() -> {
            started.countDown();
            long id = 10_000;
            while (!stop.get()) {
                BrokerTable table = new BrokerTable();
                table.setId(id);
                table.setName("dummy_" + id);
                id++;
                sharedTables.add(table);
                if (sharedTables.size() > 2500) {
                    sharedTables.remove(0);
                }
            }
        }, "tablet-stat-mgr-test-modifier");
        modifier.setDaemon(true);
        modifier.start();

        started.await(5, TimeUnit.SECONDS);

        TabletStatMgr mgr = new TabletStatMgr();
        for (int i = 0; i < 50; i++) {
            mgr.runAfterCatalogReady();
        }

        stop.set(true);
        modifier.join(TimeUnit.SECONDS.toMillis(5));
    }
}
