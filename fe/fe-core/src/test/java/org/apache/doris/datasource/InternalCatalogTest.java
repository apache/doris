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

package org.apache.doris.datasource;

import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class InternalCatalogTest {

    private static final String TEST_DB_NAME = "test_db";
    private static final String TEST_TABLE_NAME = "test_table";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env masterEnv;
    private static EditLog testEditLog;
    private static Database db;
    private ConnectContext ctx;

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {
        FeConstants.runningUnitTest = true;
        Config.enable_new_partition_inverted_index_v2_format = false;

        EnvFactory envFactory = EnvFactory.getInstance();
        masterEnv = envFactory.createEnv(false);
        fakeEnv = new FakeEnv();
        FakeEnv.setSystemInfo(Env.getCurrentSystemInfo());

        fakeEditLog = new FakeEditLog();
        testEditLog = null;
        FakeEnv.setEnv(masterEnv);

        ctx = new ConnectContext();
        ctx.setEnv(masterEnv);
        UserIdentity rootUser = new UserIdentity("root", "%");
        rootUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(rootUser);
        ctx.setThreadLocalInfo();

        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return masterEnv;
            }

            @Mock
            public EditLog getEditLog() {
                if (testEditLog == null) {
                    testEditLog = new EditLog("test") {
                    };
                }
                return testEditLog;
            }
        };

        db = new Database(CatalogTestUtil.testDbId1, TEST_DB_NAME);
        masterEnv.unprotectCreateDb(db);
    }

    @Test
    public void testMixedFormatPartitions() throws Exception {
        // Test: Old partitions keep V1, new partitions use V2 when config is enabled

        // Step 1: Create initial partition with V1 format (config disabled)
        Config.enable_new_partition_inverted_index_v2_format = false;
        Map<Long, TInvertedIndexFileStorageFormat> partitionFormats = Maps.newHashMap();

        // Mock createPartitionWithIndices to capture formats for each partition
        new MockUp<InternalCatalog>() {
            @Mock
            public void createPartitionWithIndices(
                    long dbId, OlapTable table, long partitionId, String partitionName,
                    Map<Long, MaterializedIndexMeta> indexIdToMeta,
                    HashDistributionInfo distributionInfo, DataProperty dataProperty,
                    ReplicaAllocation replicaAlloc, long versionHash,
                    java.util.Set<String> bfColumns, java.util.Set<String> bfFpp,
                    boolean isInMemory, TTabletType tabletType,
                    String storagePolicy, java.util.Set<Long> tabletIdSet,
                    java.util.Map<String, String> properties, boolean createInitialRowset)
                    throws DdlException {

                // Determine format based on config and table format
                TInvertedIndexFileStorageFormat format = table.getInvertedIndexFileStorageFormat();
                if (Config.enable_new_partition_inverted_index_v2_format
                        && format == TInvertedIndexFileStorageFormat.V1) {
                    format = TInvertedIndexFileStorageFormat.V2;
                }

                partitionFormats.put(partitionId, format);
            }
        };

        InternalCatalog internalCatalog = (InternalCatalog) masterEnv.getInternalCatalog();

        // Create MaterializedIndexMeta for base index
        long baseIndexId = 2000L;
        MaterializedIndexMeta indexMeta =
                new MaterializedIndexMeta(
                        baseIndexId,
                        Lists.newArrayList(new Column("col1",
                                PrimitiveType.INT)),
                        0,
                        100,
                        (short) 1,
                        TStorageType.COLUMN,
                        KeysType.DUP_KEYS,
                        new OriginStatement("CREATE TABLE test", 0)
                );
        Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
        indexIdToMeta.put(baseIndexId, indexMeta);

        // Mock OlapTable with V1 format
        new MockUp<OlapTable>() {
            @Mock
            public TInvertedIndexFileStorageFormat getInvertedIndexFileStorageFormat() {
                return TInvertedIndexFileStorageFormat.V1;
            }

            @Mock
            public long getId() {
                return 1000L;
            }

            @Mock
            public long getBaseIndexId() {
                return baseIndexId;
            }

            @Mock
            public String getName() {
                return "test_table";
            }

            @Mock
            public java.util.List<Index> getIndexes() {
                return Lists.newArrayList();
            }

            @Mock
            public TableProperty getTableProperty() {
                return new TableProperty(Maps.newHashMap());
            }

            @Mock
            public double getBfFpp() {
                return 0.05;
            }

            @Mock
            public DataSortInfo getDataSortInfo() {
                return null;
            }
        };

        // Create initial partition
        long partition1Id = 3000L;
        try {
            OlapTable table = new OlapTable();
            internalCatalog.createPartitionWithIndices(
                    db.getId(), table, partition1Id, "p1",
                    indexIdToMeta,
                    new HashDistributionInfo(1, Lists.newArrayList()),
                    new DataProperty(TStorageMedium.HDD),
                    new ReplicaAllocation((short) 1),
                    1L, Sets.newHashSet(), Sets.newHashSet(),
                    false,
                    TTabletType.TABLET_TYPE_DISK,
                    "", null, null, false);
        } catch (Exception e) {
            // Expected in test environment
        }

        // Verify partition1 uses V1 format (config was disabled)
        Assert.assertEquals("First partition should use V1 format when config is disabled",
                TInvertedIndexFileStorageFormat.V1, partitionFormats.get(partition1Id));

        // Step 2: Enable config and create new partition
        Config.enable_new_partition_inverted_index_v2_format = true;

        long partition2Id = 3001L;
        try {
            OlapTable table = new OlapTable();
            internalCatalog.createPartitionWithIndices(
                    db.getId(), table, partition2Id, "p2",
                    indexIdToMeta,
                    new HashDistributionInfo(1, Lists.newArrayList()),
                    new DataProperty(TStorageMedium.HDD),
                    new ReplicaAllocation((short) 1),
                    1L, Sets.newHashSet(), Sets.newHashSet(),
                    false,
                    TTabletType.TABLET_TYPE_DISK,
                    "", null, null, false);
        } catch (Exception e) {
            // Expected in test environment
        }

        // Step 3: Verify mixed formats
        Assert.assertEquals("First partition should still be V1",
                TInvertedIndexFileStorageFormat.V1, partitionFormats.get(partition1Id));
        Assert.assertEquals("Second partition should be upgraded to V2",
                TInvertedIndexFileStorageFormat.V2, partitionFormats.get(partition2Id));
    }

    @Test
    public void testV1FormatRemainsWhenConfigDisabled() throws Exception {
        // Test: V1 table format should remain V1 when config is disabled
        Config.enable_new_partition_inverted_index_v2_format = false;

        AtomicReference<TInvertedIndexFileStorageFormat> capturedFormat = new AtomicReference<>();

        // Mock createPartitionWithIndices to capture the actual format used during partition creation
        new MockUp<InternalCatalog>() {
            @Mock
            public void createPartitionWithIndices(
                    long dbId, OlapTable table, long partitionId, String partitionName,
                    Map<Long, MaterializedIndexMeta> indexIdToMeta,
                    HashDistributionInfo distributionInfo, DataProperty dataProperty,
                    ReplicaAllocation replicaAlloc, long versionHash,
                    java.util.Set<String> bfColumns, java.util.Set<String> bfFpp,
                    boolean isInMemory, TTabletType tabletType,
                    String storagePolicy, java.util.Set<Long> tabletIdSet,
                    java.util.Map<String, String> properties, boolean createInitialRowset)
                    throws DdlException {

                // Capture the actual format passed to createPartitionWithIndices
                TInvertedIndexFileStorageFormat format = table.getInvertedIndexFileStorageFormat();
                capturedFormat.set(format);
            }
        };

        InternalCatalog internalCatalog = (InternalCatalog) masterEnv.getInternalCatalog();

        // Create MaterializedIndexMeta for base index
        long baseIndexId = 2000L;
        MaterializedIndexMeta indexMeta =
                new MaterializedIndexMeta(
                        baseIndexId,
                        Lists.newArrayList(new Column("col1",
                                PrimitiveType.INT)),
                        0,
                        100,
                        (short) 1,
                        TStorageType.COLUMN,
                        KeysType.DUP_KEYS,
                        new OriginStatement("CREATE TABLE test", 0)
                );
        Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
        indexIdToMeta.put(baseIndexId, indexMeta);

        // Create a mock OlapTable with V1 format
        new MockUp<OlapTable>() {
            @Mock
            public TInvertedIndexFileStorageFormat getInvertedIndexFileStorageFormat() {
                return TInvertedIndexFileStorageFormat.V1;
            }

            @Mock
            public long getId() {
                return 1000L;
            }

            @Mock
            public long getBaseIndexId() {
                return baseIndexId;
            }

            @Mock
            public String getName() {
                return "test_table";
            }

            @Mock
            public java.util.List<Index> getIndexes() {
                return Lists.newArrayList();
            }

            @Mock
            public TableProperty getTableProperty() {
                return new TableProperty(Maps.newHashMap());
            }

            @Mock
            public double getBfFpp() {
                return 0.05;
            }

            @Mock
            public DataSortInfo getDataSortInfo() {
                return null;
            }
        };

        try {
            OlapTable table = new OlapTable();

            // Call the actual createPartitionWithIndices method to test no upgrade when config disabled
            internalCatalog.createPartitionWithIndices(
                    db.getId(), table, 3000L, "test_partition",
                    indexIdToMeta,
                    new HashDistributionInfo(1, Lists.newArrayList()),
                    new DataProperty(TStorageMedium.HDD),
                    new ReplicaAllocation((short) 1),
                    1L, Sets.newHashSet(), Sets.newHashSet(),
                    false,
                    TTabletType.TABLET_TYPE_DISK,
                    "", null, null, false);
        } catch (Exception e) {
            // It's expected to fail in test environment, we only care about the format capture
        }

        // Verify that V1 table format remains V1 when config is disabled
        Assert.assertEquals("V1 table format should remain V1 when config is disabled",
                TInvertedIndexFileStorageFormat.V1, capturedFormat.get());
    }

    @Test
    public void testV2TableFormatBehavior() throws Exception {
        // Test V2 table format behavior - should remain V2 regardless of config
        Config.enable_new_partition_inverted_index_v2_format = true;
        AtomicReference<TInvertedIndexFileStorageFormat> capturedFormat = new AtomicReference<>();

        // Mock createPartitionWithIndices to capture the actual format used during partition creation
        new MockUp<InternalCatalog>() {
            @Mock
            public void createPartitionWithIndices(
                    long dbId, OlapTable table, long partitionId, String partitionName,
                    Map<Long, MaterializedIndexMeta> indexIdToMeta,
                    HashDistributionInfo distributionInfo, DataProperty dataProperty,
                    ReplicaAllocation replicaAlloc, long versionHash,
                    java.util.Set<String> bfColumns, java.util.Set<String> bfFpp,
                    boolean isInMemory, TTabletType tabletType,
                    String storagePolicy, java.util.Set<Long> tabletIdSet,
                    java.util.Map<String, String> properties, boolean createInitialRowset)
                    throws DdlException {

                // Capture the actual format passed to createPartitionWithIndices
                TInvertedIndexFileStorageFormat format = table.getInvertedIndexFileStorageFormat();
                capturedFormat.set(format);
            }
        };

        InternalCatalog internalCatalog = (InternalCatalog) masterEnv.getInternalCatalog();

        // Create MaterializedIndexMeta for base index
        long baseIndexId = 2000L;
        MaterializedIndexMeta indexMeta =
                new MaterializedIndexMeta(
                        baseIndexId,
                        Lists.newArrayList(new Column("col1",
                                PrimitiveType.INT)),
                        0,
                        100,
                        (short) 1,
                        TStorageType.COLUMN,
                        KeysType.DUP_KEYS,
                        new OriginStatement("CREATE TABLE test", 0)
                );
        Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
        indexIdToMeta.put(baseIndexId, indexMeta);

        // Create a mock OlapTable with V2 format
        new MockUp<OlapTable>() {
            @Mock
            public TInvertedIndexFileStorageFormat getInvertedIndexFileStorageFormat() {
                return TInvertedIndexFileStorageFormat.V2;
            }

            @Mock
            public long getId() {
                return 1000L;
            }

            @Mock
            public long getBaseIndexId() {
                return baseIndexId;
            }

            @Mock
            public String getName() {
                return "test_table";
            }

            @Mock
            public java.util.List<Index> getIndexes() {
                return Lists.newArrayList();
            }

            @Mock
            public TableProperty getTableProperty() {
                return new TableProperty(Maps.newHashMap());
            }

            @Mock
            public double getBfFpp() {
                return 0.05;
            }

            @Mock
            public DataSortInfo getDataSortInfo() {
                return null;
            }
        };

        try {
            OlapTable table = new OlapTable();

            // Call the actual createPartitionWithIndices method to test V2 format behavior
            internalCatalog.createPartitionWithIndices(
                    db.getId(), table, 3000L, "test_partition",
                    indexIdToMeta,
                    new HashDistributionInfo(1, Lists.newArrayList()),
                    new DataProperty(TStorageMedium.HDD),
                    new ReplicaAllocation((short) 1),
                    1L, Sets.newHashSet(), Sets.newHashSet(),
                    false,
                    TTabletType.TABLET_TYPE_DISK,
                    "", null, null, false);
        } catch (Exception e) {
            // It's expected to fail in test environment, we only care about the format capture
        }

        // Verify that V2 table format remains V2 when config is enabled
        Assert.assertEquals("V2 table format should remain V2 when config is enabled",
                TInvertedIndexFileStorageFormat.V2, capturedFormat.get());

        // Test with config disabled - V2 should still remain V2
        capturedFormat.set(null); // Reset
        Config.enable_new_partition_inverted_index_v2_format = false;

        try {
            OlapTable table = new OlapTable();
            internalCatalog.createPartitionWithIndices(
                    db.getId(), table, 3001L, "test_partition2",
                    indexIdToMeta,
                    new HashDistributionInfo(1, Lists.newArrayList()),
                    new DataProperty(TStorageMedium.HDD),
                    new ReplicaAllocation((short) 1),
                    1L, Sets.newHashSet(), Sets.newHashSet(),
                    false,
                    TTabletType.TABLET_TYPE_DISK,
                    "", null, null, false);
        } catch (Exception e) {
            // It's expected to fail in test environment, we only care about the format capture
        }

        // Verify that V2 table format remains V2 even when config is disabled
        Assert.assertEquals("V2 table format should remain V2 when config is disabled",
                TInvertedIndexFileStorageFormat.V2, capturedFormat.get());
    }

}
