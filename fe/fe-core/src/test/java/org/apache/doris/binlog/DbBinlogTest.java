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

import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class DbBinlogTest {
    private long dbId = 10000L;
    private long baseTableId = 20000L;
    private int tableNum = 5;
    private int gcTableNum = 2;
    private List<Long> tableIds;

    private int totalBinlogNum = 10;
    private int expiredBinlogNum = 3;
    private long baseNum = 30000L;

    @Before
    public void setUp() {
        // check args valid
        Assert.assertTrue(totalBinlogNum > 0);
        Assert.assertTrue(gcTableNum <= tableNum);
        Assert.assertTrue(expiredBinlogNum <= totalBinlogNum);

        // gen tableIds
        tableIds = Lists.newArrayList();
        for (int i = 0; i < tableNum; ++i) {
            tableIds.add(baseTableId + i);
        }

        new MockUp<BinlogUtils>() {
            @Mock
            public long getExpiredMs(long direct) {
                return direct;
            }
        };
    }

    @Test
    public void testTableTtlGcCommonCase() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        for (int i = 0; i < tableNum; ++i) {
            String key = String.format("%d_%d", dbId, baseTableId + i);
            if (i <= gcTableNum) {
                ttlMap.put(key, expiredTime);
            } else {
                ttlMap.put(key, 0L);
            }
        }
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        binlogConfigCache.addDbBinlogConfig(dbId, false, 0L);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        Long[] tableLastCommitInfo = new Long[tableNum];
        long maxGcTableId = baseTableId + gcTableNum;
        long expiredCommitSeq = -1;
        for (int i = 0; i < totalBinlogNum; ++i) {
            long tableId = baseTableId + (i / tableNum);
            long commitSeq = baseNum + i;
            if (tableId <= maxGcTableId) {
                expiredCommitSeq = commitSeq;
            }
            tableLastCommitInfo[i / tableNum] = commitSeq;
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, baseNum);
            testBinlogs.add(binlog);
        }

        // init DbBinlog
        DBBinlog dbBinlog = null;

        // insert binlogs
        for (int i = 0; i < totalBinlogNum; ++i) {
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, testBinlogs.get(i));
            }
            dbBinlog.addBinlog(testBinlogs.get(i), null);
        }

        // trigger gc
        BinlogTombstone tombstone = dbBinlog.gc();

        // check binlog status
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTableIds().get(0) <= baseTableId + gcTableNum) {
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                Assert.assertEquals(1, binlog.getTableRef());
            }
        }

        // check dummy binlog
        List<TBinlog> allBinlogs = Lists.newArrayList();
        dbBinlog.getAllBinlogs(allBinlogs);
        for (TBinlog binlog : allBinlogs) {
            if (binlog.getType() != TBinlogType.DUMMY) {
                break;
            }
            long belong = binlog.getBelong();
            if (belong < 0) {
                Assert.assertEquals(expiredCommitSeq, binlog.getCommitSeq());
            } else if (belong <= maxGcTableId) {
                int offset = (int) (belong - baseTableId);
                Assert.assertEquals((long) tableLastCommitInfo[offset], binlog.getCommitSeq());
            } else {
                Assert.assertEquals(-1, binlog.getCommitSeq());
            }
        }

        // check tombstone
        Assert.assertFalse(tombstone.isDbBinlogTomstone());
        Assert.assertEquals(expiredCommitSeq, tombstone.getCommitSeq());
    }

    @Test
    public void testTableTtlGcBinlogMultiRefCase() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        for (int i = 0; i < tableNum; ++i) {
            String key = String.format("%d_%d", dbId, baseTableId + i);
            if (i < tableNum - 1) {
                ttlMap.put(key, expiredTime);
            } else {
                ttlMap.put(key, 0L);
            }
        }
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        binlogConfigCache.addDbBinlogConfig(dbId, false, 0L);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        for (int i = 0; i < totalBinlogNum; ++i) {
            // generate tableIds
            long tableId = baseTableId + (i / (tableNum - 1));
            long additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            while (tableId == additionalTableId) {
                additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            }
            List<Long> tableIds = Lists.newArrayList(tableId, additionalTableId);
            // init commitSeq
            long commitSeq = baseNum + i;

            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableIds, commitSeq, baseNum);
            testBinlogs.add(binlog);
        }

        // init dbBinlog
        DBBinlog dbBinlog = null;

        // ad additional ref & add to dbBinlog
        for (int i = 0; i < totalBinlogNum; ++i) {
            TBinlog binlog = testBinlogs.get(i);
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, binlog);
            }
            dbBinlog.addBinlog(binlog, null);
        }

        // trigger gc
        dbBinlog.gc();

        // check binlog status
        long unGcTableId = baseTableId + tableNum - 1;
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTableIds().contains(unGcTableId)) {
                Assert.assertEquals(1, binlog.getTableRef());
            } else {
                Assert.assertEquals(0, binlog.getTableRef());
            }
        }
    }

    @Test
    public void testTableCommitSeqGc() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        binlogConfigCache.addDbBinlogConfig(dbId, true, expiredTime);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        for (int i = 0; i < totalBinlogNum; ++i) {
            // generate tableIds
            long tableId = baseTableId + (i / (tableNum - 1));
            long additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            while (tableId == additionalTableId) {
                additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            }
            List<Long> tableIds = Lists.newArrayList(tableId, additionalTableId);
            // init stamp
            long stamp = baseNum + i;

            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableIds, stamp, stamp);
            testBinlogs.add(binlog);
        }

        // init dbBinlog
        DBBinlog dbBinlog = null;

        // ad additional ref & add to dbBinlog
        for (int i = 0; i < totalBinlogNum; ++i) {
            TBinlog binlog = testBinlogs.get(i);
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, binlog);
            }
            dbBinlog.addBinlog(binlog, null);
        }

        // trigger gc
        dbBinlog.gc();

        // check binlog status
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTimestamp() <= expiredTime) {
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                Assert.assertTrue(binlog.getTableRef() != 0);
            }
        }
    }

    @Test
    public void testAddBinlog() throws NoSuchFieldException, IllegalAccessException {
        // set max value num
        int maxValue = 12;

        // mock up
        new MockUp<BinlogConfigCache>() {
            @Mock
            boolean isEnableDB(long dbId) {
                return true;
            }

            @Mock
            boolean isEnableTable(long dbId, long tableId) {
                return true;
            }
        };

        // reflect field
        Field allBinlogsField = DBBinlog.class.getDeclaredField("allBinlogs");
        allBinlogsField.setAccessible(true);
        Field tableBinlogMapField = DBBinlog.class.getDeclaredField("tableBinlogMap");
        tableBinlogMapField.setAccessible(true);


        for (int i = 0; i <= maxValue; ++i) {
            TBinlogType type = TBinlogType.findByValue(i);
            if (type == TBinlogType.DUMMY) {
                continue;
            }
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, baseTableId, 1, 1);
            binlog.setType(type);
            DBBinlog dbBinlog = new DBBinlog(new BinlogConfigCache(), binlog);

            dbBinlog.addBinlog(binlog, null);

            TreeSet<TBinlog> allbinlogs = (TreeSet<TBinlog>) allBinlogsField.get(dbBinlog);
            Map<Long, TableBinlog> tableBinlogMap = (Map<Long, TableBinlog>) tableBinlogMapField.get(dbBinlog);
            Assert.assertTrue(allbinlogs.contains(binlog));
            switch (type) {
                case CREATE_TABLE:
                case DROP_TABLE: {
                    Assert.assertTrue(tableBinlogMap.isEmpty());
                    break;
                }
                default: {
                    Assert.assertTrue(tableBinlogMap.containsKey(baseTableId));
                    break;
                }
            }
        }
    }
}
