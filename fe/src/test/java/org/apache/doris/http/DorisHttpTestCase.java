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

package org.apache.doris.http;

import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;

abstract public class DorisHttpTestCase {

    public OkHttpClient networkClient = new OkHttpClient.Builder()
            .readTimeout(100, TimeUnit.SECONDS)
            .build();

    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private static HttpServer httpServer;

    public static final String CLUSTER_NAME = "default_cluster";
    public static final String DB_NAME = "testDb";
    public static final String TABLE_NAME = "testTbl";

    private static long testBackendId1 = 1000;
    private static long testBackendId2 = 1001;
    private static long testBackendId3 = 1002;

    private static long testReplicaId1 = 2000;
    private static long testReplicaId2 = 2001;
    private static long testReplicaId3 = 2002;

    private static long testDbId = 100L;
    private static long testTableId = 200L;
    private static long testPartitionId = 201L;
    public static long testIndexId = testTableId; // the base indexid == tableid
    private static long tabletId = 400L;

    public static long testStartVersion = 12;
    public static long testStartVersionHash = 12312;
    public static int testSchemaHash = 93423942;
    public static long testPartitionCurrentVersionHash = 12312;
    public static long testPartitionNextVersionHash = 123123123;

    public static int HTTP_PORT;

    protected static String URI;

    protected String rootAuth = Credentials.basic("root", "");

    @Mocked
    private static EditLog editLog;

    public static OlapTable newTable(String name) {
        Catalog.getCurrentInvertedIndex().clear();
        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        Column k2 = new Column("k2", PrimitiveType.DOUBLE);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);

        Replica replica1 = new Replica(testReplicaId1, testBackendId1, testStartVersion, testStartVersionHash, testSchemaHash, 1024000L, 2000L,
                Replica.ReplicaState.NORMAL, -1, 0, 0, 0);
        Replica replica2 = new Replica(testReplicaId2, testBackendId2, testStartVersion, testStartVersionHash, testSchemaHash, 1024000L, 2000L,
                Replica.ReplicaState.NORMAL, -1, 0, 0, 0);
        Replica replica3 = new Replica(testReplicaId3, testBackendId3, testStartVersion, testStartVersionHash, testSchemaHash, 1024000L, 2000L,
                Replica.ReplicaState.NORMAL, -1, 0, 0, 0);

        // tablet
        Tablet tablet = new Tablet(tabletId);

        // index
        MaterializedIndex baseIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(testDbId, testTableId, testPartitionId, testIndexId, testSchemaHash, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // partition
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(2);
        Partition partition = new Partition(testPartitionId, "testPartition", baseIndex, distributionInfo);
        partition.updateVisibleVersionAndVersionHash(testStartVersion, testStartVersionHash);
        partition.setNextVersion(testStartVersion + 1);
        partition.setNextVersionHash(testPartitionNextVersionHash, testPartitionCurrentVersionHash);

        // table
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId, DataProperty.DEFAULT_HDD_DATA_PROPERTY);
        partitionInfo.setReplicationNum(testPartitionId, (short) 3);
        OlapTable table = new OlapTable(testTableId, name, columns, KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(testIndexId, "testIndex", columns, 0, testSchemaHash, (short) 1, TStorageType.COLUMN,
                KeysType.AGG_KEYS);
        table.setBaseIndexId(testIndexId);
        return table;
    }

    private static EsTable newEsTable(String name) {
        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        Column k2 = new Column("k2", PrimitiveType.DOUBLE);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId + 100, DataProperty.DEFAULT_HDD_DATA_PROPERTY);
        partitionInfo.setReplicationNum(testPartitionId + 100, (short) 3);
        EsTable table = null;
        Map<String, String> props = new HashMap<>();
        props.put(EsTable.HOSTS, "http://node-1:8080");
        props.put(EsTable.USER, "root");
        props.put(EsTable.PASSWORD, "root");
        props.put(EsTable.INDEX, "test");
        props.put(EsTable.TYPE, "doc");
        try {
            table = new EsTable(testTableId + 1, name, columns, props, partitionInfo);
        } catch (DdlException e) {
            e.printStackTrace();
        }
        return table;
    }

    private static Catalog newDelegateCatalog() {
        try {
            Catalog catalog = Deencapsulation.newInstance(Catalog.class);
            PaloAuth paloAuth = new PaloAuth();
            //EasyMock.expect(catalog.getAuth()).andReturn(paloAuth).anyTimes();
            Database db = new Database(testDbId, "default_cluster:testDb");
            OlapTable table = newTable(TABLE_NAME);
            db.createTable(table);
            OlapTable table1 = newTable(TABLE_NAME + 1);
            db.createTable(table1);
            EsTable esTable = newEsTable("es_table");
            db.createTable(esTable);
            new Expectations(catalog) {
                {
                    catalog.getAuth();
                    minTimes = 0;
                    result = paloAuth;

                    catalog.getDb(db.getId());
                    minTimes = 0;
                    result = db;

                    catalog.getDb("default_cluster:" + DB_NAME);
                    minTimes = 0;
                    result = db;

                    catalog.isMaster();
                    minTimes = 0;
                    result = true;

                    catalog.getDb("default_cluster:emptyDb");
                    minTimes = 0;
                    result = null;

                    catalog.getDb(anyString);
                    minTimes = 0;
                    result = new Database();

                    catalog.getDbNames();
                    minTimes = 0;
                    result = Lists.newArrayList("default_cluster:testDb");

                    catalog.getLoadInstance();
                    minTimes = 0;
                    result = new Load();

                    catalog.getEditLog();
                    minTimes = 0;
                    result = editLog;

                    catalog.getClusterDbNames("default_cluster");
                    minTimes = 0;
                    result = Lists.newArrayList("default_cluster:testDb");

                    catalog.changeDb((ConnectContext) any, "blockDb");
                    minTimes = 0;

                    catalog.changeDb((ConnectContext) any, anyString);
                    minTimes = 0;

                    catalog.initDefaultCluster();
                    minTimes = 0;
                }
            };


            return catalog;
        } catch (DdlException e) {
            return null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    private static void assignBackends() {
        Backend backend1 = new Backend(testBackendId1, "node-1", 9308);
        backend1.setBePort(9300);
        Backend backend2 = new Backend(testBackendId2, "node-2", 9308);
        backend2.setBePort(9300);
        Backend backend3 = new Backend(testBackendId3, "node-3", 9308);
        backend3.setBePort(9300);
        Catalog.getCurrentSystemInfo().addBackend(backend1);
        Catalog.getCurrentSystemInfo().addBackend(backend2);
        Catalog.getCurrentSystemInfo().addBackend(backend3);
    }

    @BeforeClass
    public static void initHttpServer() throws IllegalArgException, InterruptedException {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            HTTP_PORT = socket.getLocalPort();
            URI = "http://localhost:" + HTTP_PORT + "/api/" + DB_NAME + "/" + TABLE_NAME;
        } catch (Exception e) {
            throw new IllegalStateException("Could not find a free TCP/IP port to start HTTP Server on");
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (Exception e) {
                }
            }
        }

        httpServer = new HttpServer(HTTP_PORT);
        httpServer.setup();
        httpServer.start();
        // must ensure the http server started before any unit test
        while (!httpServer.isStarted()) {
            Thread.sleep(500);
        }
    }



    @Before
    public void setUp() {
        Catalog catalog = newDelegateCatalog();
        SystemInfoService systemInfoService = new SystemInfoService();
        TabletInvertedIndex tabletInvertedIndex = new TabletInvertedIndex();
        new MockUp<Catalog>() {
            @Mock
            SchemaChangeHandler getSchemaChangeHandler() {
                return new SchemaChangeHandler();
            }
            @Mock
            MaterializedViewHandler getRollupHandler() {
                return new MaterializedViewHandler();
            }
            @Mock
            Catalog getInstance() {
                return catalog;
            }
            @Mock
            Catalog getCurrentCatalog() {
                return catalog;
            }
            @Mock
            SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
            @Mock
            TabletInvertedIndex getCurrentInvertedIndex() {
                return tabletInvertedIndex;
            }
        };
        assignBackends();
        doSetUp();
    }

    @After
    public void tearDown() {
    }

    @AfterClass
    public static void closeHttpServer() {
        httpServer.shutDown();
    }

    public void doSetUp() {

    }

    /**
     * A runnable that can throw any checked exception.
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    public void expectThrowsNoException(ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            throw new AssertionFailedError(e.getMessage());
        }
    }

    /**
     * Checks a specific exception class is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingRunnable runnable) {
        return expectThrows(expectedType, "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown", runnable);
    }

    /**
     * Checks a specific exception class is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedType, String noExceptionMessage, ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (expectedType.isInstance(e)) {
                return expectedType.cast(e);
            }
            AssertionFailedError assertion = new AssertionFailedError("Unexpected exception type, expected " + expectedType.getSimpleName() + " but got " + e);
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError(noExceptionMessage);
    }
}
