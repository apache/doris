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

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TableBinlogTest {
    private long dbId = 10000;
    private long tableId = 20000;

    private int totalBinlogNum = 10;
    private int expiredBinlogNum = 3;
    private long baseNum = 30000L;

    @Before
    public void setUp() {
        // check args valid
        Assert.assertTrue(expiredBinlogNum <= totalBinlogNum);
    }

    @Test
    public void testTtlGc() {
        // mock BinlogUtils
        new MockUp<BinlogUtils>() {
            @Mock
            public long getExpiredMs(long direct) {
                return direct;
            }
        };

        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        BinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(dbId, tableId, expiredTime);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        for (int i = 0; i < totalBinlogNum; ++i) {
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableId, baseNum + i, baseNum + i);
            testBinlogs.add(binlog);
        }

        // init TableBinlog
        TableBinlog tableBinlog = null;

        // insert binlogs
        for (int i = 0; i < totalBinlogNum; ++i) {
            if (tableBinlog == null) {
                tableBinlog = new TableBinlog(binlogConfigCache, testBinlogs.get(i), dbId, tableId);
            }
            tableBinlog.addBinlog(testBinlogs.get(i));
        }

        // trigger ttlGc
        BinlogTombstone tombstone = tableBinlog.ttlGc();

        // check binlog status
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTimestamp() <= expiredTime) {
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                Assert.assertEquals(1, binlog.getTableRef());
            }
        }

        // check tombstone
        Assert.assertFalse(tombstone.isDbBinlogTomstone());
        Assert.assertEquals(expiredTime, tombstone.getCommitSeq());

        // check dummy
        TBinlog dummy = tableBinlog.getDummyBinlog();
        Assert.assertEquals(expiredTime, dummy.getCommitSeq());
    }

    @Test
    public void testCommitSeqGc() {
        // init base data
        BinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(dbId, tableId, 0);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        for (int i = 0; i < totalBinlogNum; ++i) {
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableId, baseNum + i, baseNum + i);
            testBinlogs.add(binlog);
        }

        // init TableBinlog
        TableBinlog tableBinlog = null;

        // insert binlogs
        for (int i = 0; i < totalBinlogNum; ++i) {
            if (tableBinlog == null) {
                tableBinlog = new TableBinlog(binlogConfigCache, testBinlogs.get(i), dbId, tableId);
            }
            tableBinlog.addBinlog(testBinlogs.get(i));
        }

        // trigger ttlGc
        long expiredCommitSeq = baseNum + expiredBinlogNum;
        BinlogTombstone tombstone = tableBinlog.commitSeqGc(expiredCommitSeq);

        // check binlog status
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTimestamp() <= expiredCommitSeq) {
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                Assert.assertEquals(1, binlog.getTableRef());
            }
        }

        // check tombstone
        Assert.assertFalse(tombstone.isDbBinlogTomstone());
        Assert.assertEquals(expiredCommitSeq, tombstone.getCommitSeq());

        // check dummy
        TBinlog dummy = tableBinlog.getDummyBinlog();
        Assert.assertEquals(expiredCommitSeq, dummy.getCommitSeq());
    }
}
