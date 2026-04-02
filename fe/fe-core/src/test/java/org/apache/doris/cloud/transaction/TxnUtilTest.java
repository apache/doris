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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.CloudTabletStatMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudFEVersionSynchronizer;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TableStatsPB;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.event.Event;
import org.apache.doris.event.EventProcessor;
import org.apache.doris.statistics.AnalysisManager;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TxnUtilTest {

    private static final long DB_ID = 10001;
    private static final long TABLE_ID = 10002;
    private static final long PARTITION_ID = 10003;
    private static final long TABLET_ID_1 = 10004;
    private static final long TABLET_ID_2 = 10005;
    private static final long TXN_ID = 30001;
    private static final long PARTITION_VERSION = 5;
    private static final long TABLE_VERSION = 10;
    private static final long UPDATED_ROW_COUNT = 1000;

    private AtomicReference<Map<Long, Long>> capturedUpdatedRows;
    private AtomicReference<List<Long>> capturedNewPartitionLoaded;
    private AtomicReference<List<Long>> capturedActiveTablets;
    private AtomicBoolean eventProcessed;

    @Before
    public void setUp() {
        capturedUpdatedRows = new AtomicReference<>(null);
        capturedNewPartitionLoaded = new AtomicReference<>(null);
        capturedActiveTablets = new AtomicReference<>(null);
        eventProcessed = new AtomicBoolean(false);

        // Mock AnalysisManager
        new MockUp<AnalysisManager>() {
            @Mock
            public void updateUpdatedRows(Map<Long, Long> updatedRows) {
                capturedUpdatedRows.set(updatedRows);
            }

            @Mock
            public void setNewPartitionLoaded(List<Long> tableIds) {
                capturedNewPartitionLoaded.set(tableIds);
            }
        };

        // Mock CloudTabletStatMgr
        new MockUp<CloudTabletStatMgr>() {
            @Mock
            public CloudTabletStatMgr getInstance() {
                return new CloudTabletStatMgr();
            }

            @Mock
            public void addActiveTablets(List<Long> tabletIds) {
                capturedActiveTablets.set(tabletIds);
            }
        };

        // Mock EventProcessor
        new MockUp<EventProcessor>() {
            @Mock
            public boolean processEvent(Event event) {
                eventProcessed.set(true);
                return true;
            }
        };

        // Mock Env to return AnalysisManager and EventProcessor
        new MockUp<Env>() {
            @Mock
            public AnalysisManager getAnalysisManager() {
                return new AnalysisManager();
            }

            @Mock
            public EventProcessor getEventProcessor() {
                return new EventProcessor();
            }

            @Mock
            public boolean isMaster() {
                return true;
            }

            @Mock
            public InternalCatalog getInternalCatalog() {
                return new InternalCatalog();
            }
        };

        // Mock CloudEnv for pushVersionAsync in updateVersion
        new MockUp<CloudEnv>() {
            @Mock
            public CloudFEVersionSynchronizer getCloudFEVersionSynchronizer() {
                return new CloudFEVersionSynchronizer();
            }
        };

        new MockUp<CloudFEVersionSynchronizer>() {
            @Mock
            public void pushVersionAsync(long dbId, List<Pair<OlapTable, Long>> tableVersions,
                    Map<CloudPartition, Pair<Long, Long>> partitionVersionMap) {
            }
        };
    }

    private CommitTxnResponse buildResponseNoPartitions() {
        TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
        txnInfoBuilder.setDbId(DB_ID);
        txnInfoBuilder.setTxnId(TXN_ID);
        txnInfoBuilder.addTableIds(TABLE_ID);

        TableStatsPB.Builder statsBuilder = TableStatsPB.newBuilder();
        statsBuilder.setTableId(TABLE_ID);
        statsBuilder.setUpdatedRowCount(UPDATED_ROW_COUNT);

        return CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .setTxnInfo(txnInfoBuilder.build())
                .addTableStats(statsBuilder.build())
                .build();
    }

    private CommitTxnResponse buildResponseWithPartitions() {
        TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
        txnInfoBuilder.setDbId(DB_ID);
        txnInfoBuilder.setTxnId(TXN_ID);
        txnInfoBuilder.addTableIds(TABLE_ID);

        TableStatsPB.Builder statsBuilder = TableStatsPB.newBuilder();
        statsBuilder.setTableId(TABLE_ID);
        statsBuilder.setUpdatedRowCount(UPDATED_ROW_COUNT);
        statsBuilder.setTableVersion(TABLE_VERSION);

        return CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .setTxnInfo(txnInfoBuilder.build())
                .addPartitionIds(PARTITION_ID)
                .addVersions(PARTITION_VERSION)
                .addTableIds(TABLE_ID)
                .addTableStats(statsBuilder.build())
                .setVersionUpdateTimeMs(System.currentTimeMillis())
                .build();
    }

    private CommitTxnResponse buildEmptyResponse() {
        TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
        txnInfoBuilder.setDbId(DB_ID);
        txnInfoBuilder.setTxnId(TXN_ID);
        txnInfoBuilder.addTableIds(TABLE_ID);

        return CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .setTxnInfo(txnInfoBuilder.build())
                .build();
    }

    /**
     * Test: afterCommitCommon updates row count for AnalysisManager.
     */
    @Test
    public void testAfterCommitCommonUpdatesRowCount() {
        CommitTxnResponse response = buildResponseNoPartitions();
        List<Long> tabletIds = Lists.newArrayList(TABLET_ID_1, TABLET_ID_2);

        TxnUtil.afterCommitCommon(response, tabletIds);

        Assert.assertNotNull("updateUpdatedRows should be called", capturedUpdatedRows.get());
        Assert.assertEquals("should have 1 table entry", 1, capturedUpdatedRows.get().size());
        Assert.assertEquals("row count should match",
                Long.valueOf(UPDATED_ROW_COUNT), capturedUpdatedRows.get().get(TABLE_ID));
    }

    /**
     * Test: afterCommitCommon notifies CloudTabletStatMgr with tablet ids.
     */
    @Test
    public void testAfterCommitCommonNotifiesTabletStatMgr() {
        CommitTxnResponse response = buildEmptyResponse();
        List<Long> tabletIds = Lists.newArrayList(TABLET_ID_1, TABLET_ID_2);

        TxnUtil.afterCommitCommon(response, tabletIds);

        Assert.assertNotNull("addActiveTablets should be called", capturedActiveTablets.get());
        Assert.assertEquals("should pass both tablet ids", 2, capturedActiveTablets.get().size());
        Assert.assertTrue("should contain TABLET_ID_1", capturedActiveTablets.get().contains(TABLET_ID_1));
        Assert.assertTrue("should contain TABLET_ID_2", capturedActiveTablets.get().contains(TABLET_ID_2));
    }

    /**
     * Test: afterCommitCommon with null tabletIds does not call addActiveTablets.
     */
    @Test
    public void testAfterCommitCommonNullTabletIds() {
        CommitTxnResponse response = buildEmptyResponse();

        TxnUtil.afterCommitCommon(response, null);

        Assert.assertNull("addActiveTablets should not be called for null tabletIds",
                capturedActiveTablets.get());
    }

    /**
     * Test: afterCommitCommon with empty tabletIds does not call addActiveTablets.
     */
    @Test
    public void testAfterCommitCommonEmptyTabletIds() {
        CommitTxnResponse response = buildEmptyResponse();

        TxnUtil.afterCommitCommon(response, Collections.emptyList());

        Assert.assertNull("addActiveTablets should not be called for empty tabletIds",
                capturedActiveTablets.get());
    }

    /**
     * Test: afterCommitCommon handles DataChangeEvent failures gracefully.
     * DataChangeEvent constructor resolves catalog/db/table names via Env, which
     * may fail in test. Verify the method doesn't crash.
     */
    @Test
    public void testAfterCommitCommonHandlesEventFailure() {
        CommitTxnResponse response = buildEmptyResponse();

        // Should not throw even if DataChangeEvent fails internally
        TxnUtil.afterCommitCommon(response, Lists.newArrayList(TABLET_ID_1));
    }

    /**
     * Test: afterCommitCommon calls setNewPartitionLoaded.
     */
    @Test
    public void testAfterCommitCommonNotifiesPartitionFirstLoad() {
        CommitTxnResponse response = buildEmptyResponse();

        TxnUtil.afterCommitCommon(response, Lists.newArrayList(TABLET_ID_1));

        Assert.assertNotNull("setNewPartitionLoaded should be called",
                capturedNewPartitionLoaded.get());
    }

    /**
     * Test: updateVersion with partition data returns correct first-load map and
     * processes partition version update path without error.
     */
    @Test
    public void testUpdateVersionWithPartitions() {
        // Set up mock CloudPartition
        CloudPartition mockPartition = new CloudPartition();

        // Mock OlapTable globally
        new MockUp<OlapTable>() {
            @Mock
            public CloudPartition getPartition(long partitionId) {
                if (partitionId == PARTITION_ID) {
                    return mockPartition;
                }
                return null;
            }

            @Mock
            public boolean isManagedTable() {
                return true;
            }

            @Mock
            public long getId() {
                return TABLE_ID;
            }

            @Mock
            public void versionWriteLock() {
            }

            @Mock
            public void versionWriteUnlock() {
            }

            @Mock
            public void setCachedTableVersion(long version) {
            }
        };

        OlapTable fakeTable = new OlapTable();

        // Mock Database globally
        new MockUp<Database>() {
            @Mock
            public org.apache.doris.catalog.Table getTableNullable(long tableId) {
                if (tableId == TABLE_ID) {
                    return fakeTable;
                }
                return null;
            }
        };

        Database fakeDb = new Database();

        // Mock InternalCatalog globally
        new MockUp<InternalCatalog>() {
            @Mock
            public Database getDbNullable(long dbId) {
                if (dbId == DB_ID) {
                    return fakeDb;
                }
                return null;
            }
        };

        InternalCatalog fakeCatalog = new InternalCatalog();

        new MockUp<Env>() {
            @Mock
            public InternalCatalog getInternalCatalog() {
                return fakeCatalog;
            }

            @Mock
            public AnalysisManager getAnalysisManager() {
                return new AnalysisManager();
            }

            @Mock
            public EventProcessor getEventProcessor() {
                return new EventProcessor();
            }
        };

        new MockUp<CloudFEVersionSynchronizer>() {
            @Mock
            public void pushVersionAsync(long dbId, List<Pair<OlapTable, Long>> tableVersions,
                    Map<CloudPartition, Pair<Long, Long>> partitionVersionMap) {
            }
        };

        new MockUp<CloudEnv>() {
            @Mock
            public CloudFEVersionSynchronizer getCloudFEVersionSynchronizer() {
                return new CloudFEVersionSynchronizer();
            }
        };

        CommitTxnResponse response = buildResponseWithPartitions();

        Map<Long, List<Long>> result = TxnUtil.updateVersion(response);

        // version == 5 (> 2) so no first-load partitions
        Assert.assertNotNull("result should not be null", result);
        Assert.assertTrue("version > 2 should not produce first-load entries", result.isEmpty());
    }

    /**
     * Test: updateVersion returns empty map when no partitions in response.
     */
    @Test
    public void testUpdateVersionEmptyPartitions() {
        CommitTxnResponse response = buildEmptyResponse();

        Map<Long, List<Long>> result = TxnUtil.updateVersion(response);

        Assert.assertTrue("should return empty map for empty response", result.isEmpty());
    }

    /**
     * Test: updateVersion identifies first-load partitions (version == 2).
     */
    @Test
    public void testUpdateVersionFirstLoadPartition() {
        CloudPartition mockPartition = new CloudPartition();

        new MockUp<OlapTable>() {
            @Mock
            public CloudPartition getPartition(long partitionId) {
                return mockPartition;
            }

            @Mock
            public boolean isManagedTable() {
                return true;
            }

            @Mock
            public long getId() {
                return TABLE_ID;
            }

            @Mock
            public void versionWriteLock() {
            }

            @Mock
            public void versionWriteUnlock() {
            }

            @Mock
            public void setCachedTableVersion(long version) {
            }
        };

        OlapTable fakeTable = new OlapTable();

        new MockUp<Database>() {
            @Mock
            public org.apache.doris.catalog.Table getTableNullable(long tableId) {
                return fakeTable;
            }
        };

        Database fakeDb = new Database();

        new MockUp<InternalCatalog>() {
            @Mock
            public Database getDbNullable(long dbId) {
                return fakeDb;
            }
        };

        InternalCatalog fakeCatalog = new InternalCatalog();

        new MockUp<Env>() {
            @Mock
            public InternalCatalog getInternalCatalog() {
                return fakeCatalog;
            }

            @Mock
            public AnalysisManager getAnalysisManager() {
                return new AnalysisManager();
            }

            @Mock
            public EventProcessor getEventProcessor() {
                return new EventProcessor();
            }
        };

        new MockUp<CloudFEVersionSynchronizer>() {
            @Mock
            public void pushVersionAsync(long dbId, List<Pair<OlapTable, Long>> tableVersions,
                    Map<CloudPartition, Pair<Long, Long>> partitionVersionMap) {
            }
        };

        new MockUp<CloudEnv>() {
            @Mock
            public CloudFEVersionSynchronizer getCloudFEVersionSynchronizer() {
                return new CloudFEVersionSynchronizer();
            }
        };

        // Build response with version == 2 (first load)
        TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
        txnInfoBuilder.setDbId(DB_ID);
        txnInfoBuilder.setTxnId(TXN_ID);
        txnInfoBuilder.addTableIds(TABLE_ID);

        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .setTxnInfo(txnInfoBuilder.build())
                .addPartitionIds(PARTITION_ID)
                .addVersions(2)  // version == 2 means first load
                .addTableIds(TABLE_ID)
                .setVersionUpdateTimeMs(System.currentTimeMillis())
                .build();

        Map<Long, List<Long>> result = TxnUtil.updateVersion(response);

        Assert.assertTrue("should contain TABLE_ID as first-load",
                result.containsKey(TABLE_ID));
        Assert.assertTrue("should contain PARTITION_ID as first-load partition",
                result.get(TABLE_ID).contains(PARTITION_ID));
    }
}
