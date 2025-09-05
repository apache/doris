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

package org.apache.doris.cloud.datasource;

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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudEnvFactory;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.ComputeGroup;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;
import org.apache.doris.utframe.MockedMetaServerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class CloudInternalCatalogTest {

    private static final String TEST_DB_NAME = "test_cloud_db";
    private static final String TEST_TABLE_NAME = "test_cloud_table";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env masterEnv;
    private static EditLog testEditLog;
    private static Database db;
    private ConnectContext ctx;

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, UserException {
        FeConstants.runningUnitTest = true;
        Config.enable_new_partition_inverted_index_v2_format = false;

        // Mock VersionHelper globally to avoid all meta service calls
        new MockUp<VersionHelper>() {
            @Mock
            public long getVersionFromMeta(long dbId, long tableId, long partitionId) {
                return 1L;
            }

            @Mock
            public long getVisibleVersion(long dbId, long tableId, long partitionId) {
                return 1L;
            }
        };

        // Mock CloudPartition globally to avoid meta service calls
        new MockUp<CloudPartition>() {
            @Mock
            public long getVisibleVersion() {
                return 1L;
            }
        };

        // Setup for MetaServiceProxy mock
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
                Cloud.BeginTxnResponse.Builder beginTxnResponseBuilder = Cloud.BeginTxnResponse.newBuilder();
                beginTxnResponseBuilder.setTxnId(1000)
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"));
                return beginTxnResponseBuilder.build();
            }

            @Mock
            public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                Cloud.TxnInfoPB.Builder txnInfoBuilder = Cloud.TxnInfoPB.newBuilder();
                txnInfoBuilder.setDbId(CatalogTestUtil.testDbId1);
                txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
                txnInfoBuilder.setLabel("test_label");
                txnInfoBuilder.setListenerId(-1);
                Cloud.CommitTxnResponse.Builder commitTxnResponseBuilder = Cloud.CommitTxnResponse.newBuilder();
                commitTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(txnInfoBuilder.build());
                return commitTxnResponseBuilder.build();
            }

            @Mock
            public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request) {
                Cloud.CheckTxnConflictResponse.Builder checkTxnConflictResponseBuilder =
                        Cloud.CheckTxnConflictResponse.newBuilder();
                checkTxnConflictResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"))
                        .setFinished(true);
                return checkTxnConflictResponseBuilder.build();
            }

            @Mock
            public Cloud.GetClusterResponse getCluster(Cloud.GetClusterRequest request) {
                Cloud.GetClusterResponse.Builder getClusterResponseBuilder = Cloud.GetClusterResponse.newBuilder();
                Cloud.ClusterPB.Builder clusterBuilder = Cloud.ClusterPB.newBuilder();
                clusterBuilder.setClusterId("test_id").setClusterName("test_group");

                Cloud.NodeInfoPB.Builder node1 = Cloud.NodeInfoPB.newBuilder();
                node1.setCloudUniqueId("test_cloud")
                        .setName("host1")
                        .setIp("host1")
                        .setHost("host1")
                        .setHeartbeatPort(123)
                        .setEditLogPort(125)
                        .setStatus(Cloud.NodeStatusPB.NODE_STATUS_RUNNING);
                clusterBuilder.addNodes(node1.build());
                getClusterResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"))
                        .addCluster(clusterBuilder.build());
                return getClusterResponseBuilder.build();
            }

            @Mock
            public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) {
                Cloud.CreateTabletsResponse.Builder responseBuilder = Cloud.CreateTabletsResponse.newBuilder();
                responseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"));
                return responseBuilder.build();
            }

            @Mock
            public Cloud.FinishTabletJobResponse finishTabletJob(Cloud.FinishTabletJobRequest request) {
                Cloud.FinishTabletJobResponse.Builder responseBuilder = Cloud.FinishTabletJobResponse.newBuilder();
                responseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"));
                return responseBuilder.build();
            }

            @Mock
            public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request) {
                Cloud.GetCurrentMaxTxnResponse.Builder builder = Cloud.GetCurrentMaxTxnResponse.newBuilder();
                builder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).setMsg("OK"))
                        .setCurrentMaxTxnId(1000);
                return builder.build();
            }
        };

        Config.cloud_unique_id = "test_cloud";
        Config.meta_service_endpoint = MockedMetaServerFactory.METASERVER_DEFAULT_IP + ":" + 20121;

        EnvFactory envFactory = EnvFactory.getInstance();
        masterEnv = envFactory.createEnv(false);
        SystemInfoService cloudSystemInfo = Env.getCurrentSystemInfo();
        fakeEnv = new FakeEnv();
        FakeEnv.setSystemInfo(cloudSystemInfo);

        fakeEditLog = new FakeEditLog();
        testEditLog = null; // Will be set by MockUp
        FakeEnv.setEnv(masterEnv);

        ctx = new ConnectContext();
        ctx.setEnv(masterEnv);
        UserIdentity rootUser = new UserIdentity("root", "%");
        rootUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(rootUser);
        ctx.setThreadLocalInfo();
        ctx.setCloudCluster("test_group");

        Assert.assertTrue(envFactory instanceof CloudEnvFactory);
        Assert.assertTrue(masterEnv instanceof CloudEnv);

        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return masterEnv;
            }

            @Mock
            public EditLog getEditLog() {
                if (testEditLog == null) {
                    testEditLog = new EditLog("test") {
                        // Override to avoid initialization issues
                    };
                }
                return testEditLog;
            }

            @Mock
            public AccessControllerManager getAccessManager() {
                return new AccessControllerManager(masterEnv.getAuth()) {
                    @Override
                    public boolean checkTblPriv(ConnectContext ctx, String ctl, String db, String tbl,
                            PrivPredicate wanted) {
                        return true; // Allow all access for test
                    }
                };
            }
        };

        new MockUp<Auth>() {
            @Mock
            public String getDefaultCloudCluster(String user) {
                return "test_group"; // Return default cluster for test
            }

            @Mock
            public ComputeGroup getComputeGroup(String user) {
                // Return a test compute group for the mock
                return new ComputeGroup("test_id", "test_group", ComputeGroup.ComputeTypeEnum.SQL);
            }
        };

        // Mock cloud environment permissions
        new MockUp<CloudEnv>() {
            @Mock
            public void checkCloudClusterPriv(String cluster) throws Exception {
                // Always allow for tests
            }
        };

        // Mock ConnectContext to avoid compute group permission check
        new MockUp<ConnectContext>() {
            @Mock
            public String getCloudCluster() {
                return "test_group";
            }

            @Mock
            public UserIdentity getCurrentUserIdentity() {
                UserIdentity rootUser = new UserIdentity("root", "%");
                rootUser.setIsAnalyzed();
                return rootUser;
            }
        };

        // Setup CloudSystemInfoService directly like CloudIndexTest
        Assert.assertTrue(Env.getCurrentSystemInfo() instanceof CloudSystemInfoService);
        CloudSystemInfoService systemInfo = (CloudSystemInfoService) Env.getCurrentSystemInfo();
        Backend backend = new Backend(10001L, "host1", 123);
        backend.setAlive(true);
        backend.setBePort(456);
        backend.setHttpPort(789);
        backend.setBrpcPort(321);
        Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
        newTagMap.put(Tag.CLOUD_CLUSTER_STATUS, "NORMAL");
        newTagMap.put(Tag.CLOUD_CLUSTER_NAME, "test_group");
        newTagMap.put(Tag.CLOUD_CLUSTER_ID, "test_id");
        newTagMap.put(Tag.CLOUD_CLUSTER_PUBLIC_ENDPOINT, "");
        newTagMap.put(Tag.CLOUD_CLUSTER_PRIVATE_ENDPOINT, "");
        newTagMap.put(Tag.CLOUD_UNIQUE_ID, "test_cloud");
        backend.setTagMap(newTagMap);
        List<Backend> backends = Lists.newArrayList(backend);
        systemInfo.updateCloudClusterMapNoLock(backends, new ArrayList<>());

        db = new Database(CatalogTestUtil.testDbId1, TEST_DB_NAME);
        masterEnv.unprotectCreateDb(db);
    }

    @Test
    public void testCloudMixedFormatPartitions() throws Exception {
        // Test: Old partitions keep V1, new partitions use V2 when config is enabled

        // Step 1: Create initial partition with V1 format (config disabled)
        Config.enable_new_partition_inverted_index_v2_format = false;
        Map<Long, TInvertedIndexFileStorageFormat> partitionFormats = Maps.newHashMap();

        // Mock sendCreateTabletsRpc to avoid actual meta service calls
        new MockUp<CloudInternalCatalog>() {
            @Mock
            public Cloud.CreateTabletsResponse sendCreateTabletsRpc(Cloud.CreateTabletsRequest.Builder requestBuilder)
                    throws DdlException {
                return Cloud.CreateTabletsResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK)
                                .setMsg("OK"))
                        .build();
            }
        };

        // Mock createTabletMetaBuilder to capture formats for each partition
        new MockUp<CloudInternalCatalog>() {
            @Mock
            public OlapFile.TabletMetaCloudPB.Builder createTabletMetaBuilder(long tableId, long indexId,
                    long partitionId, Tablet tablet, TTabletType tabletType, int schemaHash, KeysType keysType,
                    short shortKeyColumnCount, Set<String> bfColumns, double bfFpp, List<Index> indexes,
                    List<Column> schemaColumns, DataSortInfo dataSortInfo, TCompressionType compressionType,
                    String storagePolicy, boolean isInMemory, boolean isShadow,
                    String tableName, long ttlSeconds, boolean enableUniqueKeyMergeOnWrite,
                    boolean storeRowColumn, int schemaVersion, String compactionPolicy,
                    Long timeSeriesCompactionGoalSizeMbytes, Long timeSeriesCompactionFileCountThreshold,
                    Long timeSeriesCompactionTimeThresholdSeconds, Long timeSeriesCompactionEmptyRowsetsThreshold,
                    Long timeSeriesCompactionLevelThreshold, boolean disableAutoCompaction,
                    List<Integer> rowStoreColumnUniqueIds, boolean enableMowLightDelete,
                    TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat, long pageSize,
                    boolean variantEnableFlattenNested, long storagePageSize) throws DdlException {

                // Track format for each partition
                partitionFormats.put(partitionId, invertedIndexFileStorageFormat);
                return OlapFile.TabletMetaCloudPB.newBuilder();
            }
        };

        CloudInternalCatalog cloudCatalog = (CloudInternalCatalog) masterEnv.getInternalCatalog();

        // Create MaterializedIndexMeta for base index
        long baseIndexId = 2000L;
        MaterializedIndexMeta indexMeta =
                new MaterializedIndexMeta(
                        baseIndexId,
                        Lists.newArrayList(new Column("col1",
                                PrimitiveType.INT)),
                        0, // schema version
                        100, // schema hash
                        (short) 1, // short key column count
                        TStorageType.COLUMN,
                        KeysType.DUP_KEYS,
                        new OriginStatement("CREATE TABLE test", 0) // origin stmt
                );
        Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
        indexIdToMeta.put(baseIndexId, indexMeta);

        // Mock OlapTable with V1 format
        new MockUp<OlapTable>() {
            @Mock
            public TInvertedIndexFileStorageFormat getInvertedIndexFileStorageFormat() {
                return TInvertedIndexFileStorageFormat.V1; // Table has V1 format
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
            public String getStorageVaultId() {
                return "vault_id";
            }

            @Mock
            public String getStorageVaultName() {
                return "vault_name";
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
            cloudCatalog.createPartitionWithIndices(
                    db.getId(), table, partition1Id, "p1",
                    indexIdToMeta, // Pass proper index metadata
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
            cloudCatalog.createPartitionWithIndices(
                    db.getId(), table, partition2Id, "p2",
                    indexIdToMeta, // Pass proper index metadata
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
    public void testCloudV1FormatRemainsWhenConfigDisabled() throws Exception {
        // Test: V1 table format should remain V1 when config is disabled
        Config.enable_new_partition_inverted_index_v2_format = false;

        AtomicReference<TInvertedIndexFileStorageFormat> capturedFormat = new AtomicReference<>();

        // Mock sendCreateTabletsRpc to avoid actual meta service calls
        new MockUp<CloudInternalCatalog>() {
            @Mock
            public Cloud.CreateTabletsResponse sendCreateTabletsRpc(Cloud.CreateTabletsRequest.Builder requestBuilder)
                    throws DdlException {
                return Cloud.CreateTabletsResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK)
                                .setMsg("OK"))
                        .build();
            }
        };

        // Mock createTabletMetaBuilder to capture the actual format used during partition creation
        new MockUp<CloudInternalCatalog>() {
            @Mock
            public OlapFile.TabletMetaCloudPB.Builder createTabletMetaBuilder(long tableId, long indexId,
                    long partitionId, Tablet tablet, TTabletType tabletType, int schemaHash, KeysType keysType,
                    short shortKeyColumnCount, Set<String> bfColumns, double bfFpp, List<Index> indexes,
                    List<Column> schemaColumns, DataSortInfo dataSortInfo, TCompressionType compressionType,
                    String storagePolicy, boolean isInMemory, boolean isShadow,
                    String tableName, long ttlSeconds, boolean enableUniqueKeyMergeOnWrite,
                    boolean storeRowColumn, int schemaVersion, String compactionPolicy,
                    Long timeSeriesCompactionGoalSizeMbytes, Long timeSeriesCompactionFileCountThreshold,
                    Long timeSeriesCompactionTimeThresholdSeconds, Long timeSeriesCompactionEmptyRowsetsThreshold,
                    Long timeSeriesCompactionLevelThreshold, boolean disableAutoCompaction,
                    List<Integer> rowStoreColumnUniqueIds, boolean enableMowLightDelete,
                    TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat, long pageSize,
                    boolean variantEnableFlattenNested, long storagePageSize) throws DdlException {

                // Capture the actual format passed to createTabletMetaBuilder
                capturedFormat.set(invertedIndexFileStorageFormat);
                return OlapFile.TabletMetaCloudPB.newBuilder();
            }
        };

        CloudInternalCatalog cloudCatalog = (CloudInternalCatalog) masterEnv.getInternalCatalog();

        // Create MaterializedIndexMeta for base index
        long baseIndexId = 2000L;
        MaterializedIndexMeta indexMeta =
                new MaterializedIndexMeta(
                        baseIndexId,
                        Lists.newArrayList(new Column("col1",
                                PrimitiveType.INT)),
                        0, // schema version
                        100, // schema hash
                        (short) 1, // short key column count
                        TStorageType.COLUMN,
                        KeysType.DUP_KEYS,
                        new OriginStatement("CREATE TABLE test", 0) // origin stmt
                );
        Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
        indexIdToMeta.put(baseIndexId, indexMeta);

        // Create a mock OlapTable with V1 format
        new MockUp<OlapTable>() {
            @Mock
            public TInvertedIndexFileStorageFormat getInvertedIndexFileStorageFormat() {
                return TInvertedIndexFileStorageFormat.V1; // Table originally has V1 format
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
            public String getStorageVaultId() {
                return "vault_id";
            }

            @Mock
            public String getStorageVaultName() {
                return "vault_name";
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
            cloudCatalog.createPartitionWithIndices(
                    db.getId(), table, 3000L, "test_partition",
                    indexIdToMeta, // Pass proper index metadata
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
    public void testCloudV2TableFormatBehavior() throws Exception {
        // Test V2 table format behavior in cloud mode - should remain V2 regardless of config
        Config.enable_new_partition_inverted_index_v2_format = true;
        AtomicReference<TInvertedIndexFileStorageFormat> capturedFormat = new AtomicReference<>();
        // Mock sendCreateTabletsRpc to avoid actual meta service calls
        new MockUp<CloudInternalCatalog>() {
            @Mock
            public Cloud.CreateTabletsResponse sendCreateTabletsRpc(Cloud.CreateTabletsRequest.Builder requestBuilder)
                    throws DdlException {
                return Cloud.CreateTabletsResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK)
                                .setMsg("OK"))
                        .build();
            }
        };

        // Mock createTabletMetaBuilder to capture the actual format used during partition creation
        new MockUp<CloudInternalCatalog>() {
            @Mock
            public OlapFile.TabletMetaCloudPB.Builder createTabletMetaBuilder(long tableId, long indexId,
                    long partitionId, Tablet tablet, TTabletType tabletType, int schemaHash, KeysType keysType,
                    short shortKeyColumnCount, Set<String> bfColumns, double bfFpp, List<Index> indexes,
                    List<Column> schemaColumns, DataSortInfo dataSortInfo, TCompressionType compressionType,
                    String storagePolicy, boolean isInMemory, boolean isShadow,
                    String tableName, long ttlSeconds, boolean enableUniqueKeyMergeOnWrite,
                    boolean storeRowColumn, int schemaVersion, String compactionPolicy,
                    Long timeSeriesCompactionGoalSizeMbytes, Long timeSeriesCompactionFileCountThreshold,
                    Long timeSeriesCompactionTimeThresholdSeconds, Long timeSeriesCompactionEmptyRowsetsThreshold,
                    Long timeSeriesCompactionLevelThreshold, boolean disableAutoCompaction,
                    List<Integer> rowStoreColumnUniqueIds, boolean enableMowLightDelete,
                    TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat, long pageSize,
                    boolean variantEnableFlattenNested, long storagePageSize) throws DdlException {
                // Capture the actual format passed to createTabletMetaBuilder
                capturedFormat.set(invertedIndexFileStorageFormat);
                return OlapFile.TabletMetaCloudPB.newBuilder();
            }
        };
        CloudInternalCatalog cloudCatalog = (CloudInternalCatalog) masterEnv.getInternalCatalog();
        // Create MaterializedIndexMeta for base index
        long baseIndexId = 2000L;
        MaterializedIndexMeta indexMeta =
                new MaterializedIndexMeta(
                        baseIndexId,
                        Lists.newArrayList(new Column("col1",
                                PrimitiveType.INT)),
                        0, // schema version
                        100, // schema hash
                        (short) 1, // short key column count
                        TStorageType.COLUMN,
                        KeysType.DUP_KEYS,
                        new OriginStatement("CREATE TABLE test", 0) // origin stmt
                );
        Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
        indexIdToMeta.put(baseIndexId, indexMeta);

        // Create a mock OlapTable with V2 format
        new MockUp<OlapTable>() {
            @Mock
            public TInvertedIndexFileStorageFormat getInvertedIndexFileStorageFormat() {
                return TInvertedIndexFileStorageFormat.V2; // Table originally has V2 format
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
            public String getStorageVaultId() {
                return "vault_id";
            }

            @Mock
            public String getStorageVaultName() {
                return "vault_name";
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
            cloudCatalog.createPartitionWithIndices(
                    db.getId(), table, 3000L, "test_partition",
                    indexIdToMeta, // Pass proper index metadata
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
            cloudCatalog.createPartitionWithIndices(
                    db.getId(), table, 3001L, "test_partition2",
                    indexIdToMeta, // Pass proper index metadata
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
