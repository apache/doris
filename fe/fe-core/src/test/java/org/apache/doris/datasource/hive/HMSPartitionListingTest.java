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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.ShowPartitionsCommand;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class HMSPartitionListingTest {
    @Test
    void unlimitedCommandMissingTableRepairsLookup() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException missing = fixture.missingTable();
            Mockito.when(fixture.client.listPartitionNames("remote_db", "remote_t")).thenThrow(missing);
            Assertions.assertSame(missing, Assertions.assertThrows(RuntimeException.class,
                    () -> fixture.runCommand(-1, 0, false)));
            fixture.assertRetired();
        }
    }

    @Test
    void limitedCommandMissingTableRepairsLookup() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException missing = fixture.missingTable();
            Mockito.when(fixture.client.listPartitionNames("remote_db", "remote_t", 2)).thenThrow(missing);
            Assertions.assertSame(missing, Assertions.assertThrows(RuntimeException.class,
                    () -> fixture.runCommand(2, 0, false)));
            fixture.assertRetired();
        }
    }

    @Test
    void offsetCommandKeepsUnlimitedListingAndSort() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Mockito.when(fixture.client.listPartitionNames("remote_db", "remote_t"))
                    .thenReturn(Lists.newArrayList("p=3", "p=1", "p=2"));
            Assertions.assertEquals(Collections.singletonList(Collections.singletonList("p=2")),
                    fixture.runCommand(1, 1, false).getResultRows());
            Mockito.verify(fixture.client).listPartitionNames("remote_db", "remote_t");
            fixture.assertCurrent(fixture.table);
        }
    }

    @Test
    void descendingCommandKeepsUnlimitedListingAndSort() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Mockito.when(fixture.client.listPartitionNames("remote_db", "remote_t"))
                    .thenReturn(Lists.newArrayList("p=1", "p=3", "p=2"));
            Assertions.assertEquals(Lists.newArrayList(Collections.singletonList("p=3"),
                    Collections.singletonList("p=2")), fixture.runCommand(2, 0, true).getResultRows());
            Mockito.verify(fixture.client).listPartitionNames("remote_db", "remote_t");
            fixture.assertCurrent(fixture.table);
        }
    }

    @Test
    void delayedMissingTableKeepsSameNameReplacement() throws Exception {
        try (Fixture fixture = new Fixture()) {
            HMSExternalTable replacement = fixture.newWarmTable();
            Mockito.when(fixture.client.listPartitionNames("remote_db", "remote_t")).thenAnswer(ignored -> {
                fixture.cache.invalidateLookup("t", false);
                fixture.cache.updateCache("remote_t", "t", replacement, 10L);
                throw fixture.missingTable();
            });
            Assertions.assertThrows(RuntimeException.class, () -> fixture.runCommand(-1, 0, false));
            fixture.assertCurrent(replacement);
        }
    }

    @Test
    void transientListingFailureKeepsTableLookup() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException timeout = new HMSClientException("listing failed", new java.net.SocketTimeoutException());
            Mockito.when(fixture.client.listPartitionNames("remote_db", "remote_t")).thenThrow(timeout);
            Assertions.assertSame(timeout, Assertions.assertThrows(RuntimeException.class,
                    () -> fixture.runCommand(-1, 0, false)));
            fixture.assertCurrent(fixture.table);
        }
    }

    @Test
    void individualPartitionAbsenceDoesNotRetireTable() throws Exception {
        try (Fixture fixture = new Fixture()) {
            RuntimeException missingPartition = new HMSClientException("partition missing",
                    new NoSuchObjectException("only a partition disappeared"));
            Mockito.when(fixture.client.getPartition("remote_db", "remote_t", Collections.singletonList("1")))
                    .thenThrow(missingPartition);
            Assertions.assertSame(missingPartition, Assertions.assertThrows(RuntimeException.class,
                    () -> fixture.table.getPartition(Collections.singletonList("1"))));
            fixture.assertCurrent(fixture.table);
        }
    }

    @Test
    void showCreateAfterNamesRefreshRetiresHotObjectAndId() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.remoteNames.set(Collections.emptyList());
            fixture.cache.refreshNames();
            Assertions.assertTrue(fixture.cache.listNames().isEmpty());
            Assertions.assertSame(fixture.table, fixture.cache.getMetaObjById(10L, false).get());
            RuntimeException missing = fixture.missingTable();
            Mockito.when(fixture.client.getTable("remote_db", "remote_t")).thenThrow(missing);
            Assertions.assertSame(missing, Assertions.assertThrows(RuntimeException.class,
                    fixture.table::getCreateTableSql));
            fixture.assertRetired();
        }
    }

    @Test
    void bindingPartitionValuesMissingTableRepairsLookup() throws Exception {
        verifyBindingPartitionValues(true, false, false);
    }

    @Test
    void bindingOldAbsenceKeepsSameNameReplacement() throws Exception {
        verifyBindingPartitionValues(true, true, false);
    }

    @Test
    void bindingPartitionValuesTimeoutKeepsLookup() throws Exception {
        verifyBindingPartitionValues(false, false, false);
    }

    @Test
    void bindingPartitionValuesRetryMissingTableRepairsLookup() throws Exception {
        verifyBindingPartitionValues(true, false, true);
    }

    private void verifyBindingPartitionValues(boolean missingTable, boolean recreateDuringLoad,
            boolean retryInconsistency) throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<Column> columns = Collections.singletonList(new Column("p", Type.INT));
            List<Type> types = Collections.singletonList(Type.INT);
            Mockito.doReturn(columns).when(fixture.table).getPartitionColumns(Mockito.any());
            Mockito.doReturn(columns).when(fixture.table).getPartitionColumns();
            Mockito.doReturn(types).when(fixture.table).getPartitionColumnTypes(Mockito.any());
            Mockito.doReturn(HMSExternalTable.DLAType.HIVE).when(fixture.table).getDlaType();
            Mockito.doReturn(true).when(fixture.table).supportInternalPartitionPruned();
            HiveExternalMetaCache hive = Mockito.mock(HiveExternalMetaCache.class);
            Env env = Mockito.mock(Env.class);
            ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
            Mockito.when(manager.hive(fixture.catalog.getId())).thenReturn(hive);
            RuntimeException failure = missingTable ? fixture.missingTable()
                    : new HMSClientException("timeout", new java.net.SocketTimeoutException());
            HMSExternalTable replacement = fixture.newWarmTable();
            AtomicInteger attempts = new AtomicInteger();
            Mockito.when(hive.getPartitionValues(fixture.table, types)).thenAnswer(ignored -> {
                if (attempts.getAndIncrement() == 0 && retryInconsistency) {
                    throw new RuntimeException(HiveExternalMetaCache.ERR_CACHE_INCONSISTENCY);
                }
                if (recreateDuringLoad) {
                    fixture.cache.invalidateLookup("t", false);
                    fixture.cache.updateCache("remote_t", "t", replacement, 10L);
                }
                throw failure;
            });
            try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
                current.when(Env::getCurrentEnv).thenReturn(env);
                // This is the query-binding partition initialization entry, after warm schema reads.
                Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                        () -> fixture.table.initSelectedPartitions(Optional.empty())));
            }
            Assertions.assertEquals(retryInconsistency ? 2 : 1, attempts.get());
            if (missingTable && !recreateDuringLoad) {
                fixture.assertRetired();
            } else {
                fixture.assertCurrent(recreateDuringLoad ? replacement : fixture.table);
            }
        }
    }

    private static class Fixture implements AutoCloseable {
        private final HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        private final HMSExternalDatabase database = Mockito.mock(HMSExternalDatabase.class);
        private final HMSCachedClient client = Mockito.mock(HMSCachedClient.class);
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private final MetaCache<HMSExternalTable> cache;
        private final HMSExternalTable table;
        private final AtomicInteger repairs = new AtomicInteger();
        private final AtomicReference<List<Pair<String, String>>> remoteNames = new AtomicReference<>(
                Lists.newArrayList(Pair.of("remote_t", "t")));

        Fixture() throws Exception {
            Mockito.when(database.getFullName()).thenReturn("db");
            Mockito.when(database.getRemoteName()).thenReturn("remote_db");
            Mockito.when(catalog.getClient()).thenReturn(client);
            Mockito.doReturn(database).when(catalog).getDbOrAnalysisException("db");
            table = Mockito.spy(newWarmTable());
            Mockito.doReturn(table).when(database).getTableOrAnalysisException("t");
            cache = new MetaCache<>("db", executor, OptionalLong.empty(), OptionalLong.empty(), 10,
                    ignored -> remoteNames.get(), ignored -> Optional.empty(), null);
            cache.refreshNames();
            cache.updateCache("remote_t", "t", table, 10L);
            Mockito.when(database.observeTableLookup(table)).thenAnswer(ignored ->
                    cache.observeLookup("t", "remote_t", table, false, () -> {
                        Assertions.assertFalse(Thread.holdsLock(table));
                        repairs.incrementAndGet();
                    }));
            // The warm remote metadata still validates the table after a remote drop.
            Assertions.assertTrue(table.isPartitionedTable());
            Mockito.verify(client, Mockito.never()).getTable(Mockito.anyString(), Mockito.anyString());
        }

        HMSExternalTable newWarmTable() {
            HMSExternalTable retained = new HMSExternalTable(10L, "t", "remote_t", catalog, database) {
                @Override
                protected void makeSureInitialized() {
                    // Simulate already initialized metadata; no live table probe precedes the listing.
                }
            };
            Table remote = new Table();
            remote.setPartitionKeys(Collections.singletonList(new FieldSchema("p", "int", "")));
            Deencapsulation.setField(retained, "remoteTable", remote);
            return retained;
        }

        RuntimeException missingTable() {
            return new HMSClientException("listing failed", new NoSuchObjectException("table disappeared"));
        }

        ShowResultSet runCommand(long limit, long offset, boolean descending) {
            ShowPartitionsCommand command = new ShowPartitionsCommand(
                    new TableNameInfo("ctl", "db", "t"), null, null, limit, offset, false);
            Deencapsulation.setField(command, "catalog", catalog);
            if (descending) {
                Deencapsulation.setField(command, "orderByPairs", Lists.newArrayList(new OrderByPair(0, true)));
            }
            return Deencapsulation.invoke(command, "handleShowHMSTablePartitions");
        }

        void assertRetired() {
            Assertions.assertEquals(1, repairs.get());
            Assertions.assertFalse(cache.tryGetMetaObj("t").isPresent());
            Assertions.assertFalse(cache.getMetaObjById(10L, false).isPresent());
        }

        void assertCurrent(HMSExternalTable expected) {
            Assertions.assertEquals(0, repairs.get());
            Assertions.assertSame(expected, cache.tryGetMetaObj("t").get());
            Assertions.assertSame(expected, cache.getMetaObjById(10L, false).get());
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
