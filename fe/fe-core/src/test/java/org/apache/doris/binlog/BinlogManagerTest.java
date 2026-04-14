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

package org.apache.doris.binlog;

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.BinlogGcInfo;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class BinlogManagerTest {
    private Map<Long, List<Long>> frameWork;

    private int dbNum = 2;
    private int tableNumPerDb = 3;

    private long dbBaseId = 10000;
    private long tableBaseId = 100;
    private long baseNum = 10000;
    private long timeNow = baseNum;
    private long ttl = 3;

    private boolean enableDbBinlog = false;

    private MockedConstruction<BinlogConfigCache> mockedBinlogConfigCacheConstruction;
    private MockedConstruction<BinlogConfig> mockedBinlogConfigConstruction;
    private MockedStatic<Env> mockedEnv;
    private MockedConstruction<InternalCatalog> mockedInternalCatalogConstruction;
    private MockedConstruction<Database> mockedDatabaseConstruction;

    @BeforeClass
    public static void beforeClass() {
        Config.enable_feature_binlog = true;
    }

    @Before
    public void setUp() {
        Assert.assertTrue(tableNumPerDb < 100);
        frameWork = Maps.newHashMap();
        for (int dbOff = 1; dbOff <= dbNum; ++dbOff) {
            long dbId = dbOff * dbBaseId;
            List<Long> tableIds = Lists.newArrayList();
            for (int tblOff = 1; tblOff <= tableNumPerDb; ++tblOff) {
                tableIds.add(tableBaseId * tblOff + dbId);
            }
            frameWork.put(dbId, tableIds);
        }

        mockedBinlogConfigConstruction = Mockito.mockConstruction(BinlogConfig.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, context) -> {
                    Mockito.doAnswer(inv -> ttl).when(mock).getTtlSeconds();
                    Mockito.doAnswer(inv -> enableDbBinlog).when(mock).isEnable();
                    Mockito.doReturn(BinlogTestUtils.MAX_BYTES).when(mock).getMaxBytes();
                    Mockito.doReturn(BinlogTestUtils.MAX_HISTORY_NUMS).when(mock).getMaxHistoryNums();
                });

        mockedBinlogConfigCacheConstruction = Mockito.mockConstruction(BinlogConfigCache.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, context) -> {
                    Mockito.doAnswer(inv -> new BinlogConfig()).when(mock)
                            .getDBBinlogConfig(Mockito.anyLong());
                    Mockito.doAnswer(inv -> new BinlogConfig()).when(mock)
                            .getTableBinlogConfig(Mockito.anyLong(), Mockito.anyLong());
                    Mockito.doReturn(true).when(mock)
                            .isEnableTable(Mockito.anyLong(), Mockito.anyLong());
                    Mockito.doAnswer(inv -> enableDbBinlog).when(mock)
                            .isEnableDB(Mockito.anyLong());
                    Mockito.doReturn(false).when(mock)
                            .isAsyncMvTable(Mockito.anyLong(), Mockito.anyLong());
                    Mockito.doReturn(false).when(mock)
                            .isTemporaryTable(Mockito.anyLong(), Mockito.anyLong());
                });

        mockedDatabaseConstruction = Mockito.mockConstruction(Database.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, context) -> {
                    Mockito.doAnswer(inv -> new BinlogConfig()).when(mock).getBinlogConfig();
                });

        mockedInternalCatalogConstruction = Mockito.mockConstruction(InternalCatalog.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS),
                (mock, context) -> {
                    Mockito.doAnswer(inv -> new Database()).when(mock)
                            .getDbNullable(Mockito.anyLong());
                });

        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentInternalCatalog)
                .thenAnswer(inv -> EnvFactory.getInstance().createInternalCatalog());
    }

    @After
    public void tearDown() {
        if (mockedBinlogConfigCacheConstruction != null) {
            mockedBinlogConfigCacheConstruction.close();
        }
        if (mockedBinlogConfigConstruction != null) {
            mockedBinlogConfigConstruction.close();
        }
        if (mockedEnv != null) {
            mockedEnv.close();
        }
        if (mockedInternalCatalogConstruction != null) {
            mockedInternalCatalogConstruction.close();
        }
        if (mockedDatabaseConstruction != null) {
            mockedDatabaseConstruction.close();
        }
    }

    @Test
    public void testGetBinlog()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // reflect BinlogManager
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);

        // init binlog manager & addBinlog
        BinlogManager manager = new BinlogManager();

        // insert table binlogs
        int binlogNum = 10;
        for (int i = 1; i <= binlogNum; ++i) {
            TBinlog binlog = BinlogTestUtils.newBinlog(dbBaseId, tableBaseId, i, i);
            if (i % 2 == 0) {
                binlog.setType(TBinlogType.CREATE_TABLE);
            }
            addBinlog.invoke(manager, binlog, null);

        }

        // test get
        Pair<TStatus, TBinlog> pair;

        // get too old
        pair = manager.getBinlog(dbBaseId, tableBaseId, -99);
        Assert.assertEquals(TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ, pair.first.getStatusCode());
        Assert.assertEquals(TBinlogType.DUMMY, pair.second.getType());

        // get odd commit seq in table level ok
        pair = manager.getBinlog(dbBaseId, tableBaseId, 5);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(5 + 2, pair.second.getCommitSeq());

        // get even commit seq in table level ok
        pair = manager.getBinlog(dbBaseId, tableBaseId, 6);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(6 + 1, pair.second.getCommitSeq());

        // get odd commit seq in db level ok
        pair = manager.getBinlog(dbBaseId, -1, 5);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(5 + 1, pair.second.getCommitSeq());

        // get even commit seq in db level ok
        pair = manager.getBinlog(dbBaseId, -1, 6);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(6 + 1, pair.second.getCommitSeq());

        // get too new
        pair = manager.getBinlog(dbBaseId, tableBaseId, 999);
        Assert.assertEquals(TStatusCode.BINLOG_TOO_NEW_COMMIT_SEQ, pair.first.getStatusCode());
        Assert.assertNull(pair.second);
    }

    @Test
    public void testPersist() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            IOException, NoSuchFieldException {
        // reflect BinlogManager
        // addBinlog method
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);
        // dbBinlogMap
        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);

        // init binlog manager & addBinlog
        BinlogManager originManager = new BinlogManager();

        // insert binlogs
        long commitSeq = baseNum;
        for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
            long dbId = dbEntry.getKey();
            for (long tableId : dbEntry.getValue()) {
                addBinlog.invoke(originManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, commitSeq), null);
                ++commitSeq;
            }
        }

        // init output stream
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(arrayOutputStream);

        // serialize binlogs
        originManager.write(outputStream, 0L);

        // init another binlog manager
        BinlogManager newManager = new BinlogManager();

        // deserialize binlogs
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(arrayOutputStream.toByteArray());
        DataInputStream inputStream = new DataInputStream(arrayInputStream);
        newManager.read(inputStream, 0L);

        // get origin & new dbbinlog's allbinlogs
        Map<Long, DBBinlog> originDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(originManager);
        Map<Long, DBBinlog> newDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(newManager);
        Assert.assertEquals(originDbBinlogMap.size(), newDbBinlogMap.size());
        for (long dbId : frameWork.keySet()) {
            List<TBinlog> originBinlogList = Lists.newArrayList();
            List<TBinlog> newBinlogList = Lists.newArrayList();
            originDbBinlogMap.get(dbId).getAllBinlogs(originBinlogList);
            newDbBinlogMap.get(dbId).getAllBinlogs(newBinlogList);
            Assert.assertEquals(originBinlogList.size(), newBinlogList.size());
            for (int i = 0; i < originBinlogList.size(); ++i) {
                Assert.assertEquals(originBinlogList.get(i).getCommitSeq(),
                        newBinlogList.get(i).getCommitSeq());
            }
        }
    }

    @Test
    public void testReplayGcFromTableLevel() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, NoSuchFieldException {
        try (MockedStatic<BinlogUtils> mockedBinlogUtils = Mockito.mockStatic(BinlogUtils.class,
                Mockito.CALLS_REAL_METHODS)) {
            mockedBinlogUtils.when(() -> BinlogUtils.getExpiredMs(Mockito.anyLong()))
                    .thenAnswer(invocation -> timeNow - (long) invocation.getArgument(0));

            // reflect BinlogManager
            // addBinlog method
            Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
            addBinlog.setAccessible(true);
            // dbBinlogMap
            Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
            dbBinlogMapField.setAccessible(true);

            // init binlog origin & new manager
            BinlogManager originManager = new BinlogManager();
            BinlogManager newManager = new BinlogManager();

            // insert binlogs
            long commitSeq = 0;
            for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
                long dbId = dbEntry.getKey();
                for (long tableId : dbEntry.getValue()) {
                    addBinlog.invoke(originManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, timeNow),
                            null);
                    addBinlog.invoke(newManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, timeNow), null);
                    ++commitSeq;
                }
            }

            // origin manager gc & get BinlogGcInfo
            BinlogGcInfo info = new BinlogGcInfo(originManager.gc());

            // new manager replay gc
            newManager.replayGc(info);

            // get origin & new dbbinlog's allbinlogs
            Map<Long, DBBinlog> originDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(originManager);
            Map<Long, DBBinlog> newDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(newManager);
            Assert.assertEquals(originDbBinlogMap.size(), newDbBinlogMap.size());
            for (long dbId : frameWork.keySet()) {
                List<TBinlog> originBinlogList = Lists.newArrayList();
                List<TBinlog> newBinlogList = Lists.newArrayList();
                originDbBinlogMap.get(dbId).getAllBinlogs(originBinlogList);
                newDbBinlogMap.get(dbId).getAllBinlogs(newBinlogList);
                Assert.assertEquals(originBinlogList.size(), newBinlogList.size());
                for (int i = 0; i < originBinlogList.size(); ++i) {
                    TBinlog originBinlog = originBinlogList.get(i);
                    TBinlog newBinlog = newBinlogList.get(i);
                    Assert.assertEquals(originBinlog.getCommitSeq(), newBinlog.getCommitSeq());
                    if (newBinlog.getType() != TBinlogType.DUMMY) {
                        Assert.assertTrue(newBinlog.getTimestamp() > timeNow - ttl);
                    }
                }
            }
        }
    }

    @Test
    public void testReplayGcFromDbLevel() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, NoSuchFieldException {
        // set dbBinlogEnable
        enableDbBinlog = true;

        try (MockedStatic<BinlogUtils> mockedBinlogUtils = Mockito.mockStatic(BinlogUtils.class,
                Mockito.CALLS_REAL_METHODS)) {
            mockedBinlogUtils.when(() -> BinlogUtils.getExpiredMs(Mockito.anyLong()))
                    .thenAnswer(invocation -> timeNow - (long) invocation.getArgument(0));

            // reflect BinlogManager
            // addBinlog method
            Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
            addBinlog.setAccessible(true);
            // dbBinlogMap
            Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
            dbBinlogMapField.setAccessible(true);

            // init binlog origin & new manager
            BinlogManager originManager = new BinlogManager();
            BinlogManager newManager = new BinlogManager();

            // insert binlogs
            long commitSeq = baseNum;
            for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
                long dbId = dbEntry.getKey();
                for (long tableId : dbEntry.getValue()) {
                    ++commitSeq;
                    addBinlog.invoke(originManager,
                            BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, commitSeq), null);
                    addBinlog.invoke(newManager,
                            BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, commitSeq), null);
                }
            }
            timeNow = commitSeq;

            // origin manager gc & get BinlogGcInfo
            BinlogGcInfo info = new BinlogGcInfo(originManager.gc());

            // new manager replay gc
            newManager.replayGc(info);

            // get origin & new dbbinlog's allbinlogs
            Map<Long, DBBinlog> originDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(originManager);
            Map<Long, DBBinlog> newDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(newManager);
            Assert.assertEquals(originDbBinlogMap.size(), newDbBinlogMap.size());
            for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
                long dbId = dbEntry.getKey();
                List<TBinlog> originBinlogList = Lists.newArrayList();
                List<TBinlog> newBinlogList = Lists.newArrayList();
                originDbBinlogMap.get(dbId).getAllBinlogs(originBinlogList);
                newDbBinlogMap.get(dbId).getAllBinlogs(newBinlogList);
                Assert.assertEquals(originBinlogList.size(), newBinlogList.size());
                for (int i = 0; i < originBinlogList.size(); ++i) {
                    TBinlog originBinlog = originBinlogList.get(i);
                    TBinlog newBinlog = newBinlogList.get(i);
                    Assert.assertEquals(originBinlog.getCommitSeq(), newBinlog.getCommitSeq());
                    if (newBinlog.getType() != TBinlogType.DUMMY) {
                        Assert.assertTrue(newBinlog.getCommitSeq() > timeNow - ttl);
                    }
                }
            }
        }
    }
}
