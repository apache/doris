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
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BinlogTestUtils {

    public static final long MAX_BYTES = 0x7fffffffffffffffL;
    public static final long MAX_HISTORY_NUMS = 0x7fffffffffffffffL;

    public static BinlogConfig newTestBinlogConfig(boolean enableBinlog, long expiredTime) {
        return new BinlogConfig(enableBinlog, expiredTime, MAX_BYTES, MAX_HISTORY_NUMS);
    }

    public static BinlogConfigCache newMockBinlogConfigCache(long dbId, long tableId, long expiredTime) {
        BinlogConfig binlogConfig = newTestBinlogConfig(true, expiredTime);
        return new MockBinlogConfigCache(
                Collections.singletonMap(String.format("%d_%d", dbId, tableId), binlogConfig));
    }

    public static MockBinlogConfigCache newMockBinlogConfigCache(Map<String, Long> ttlMap) {
        Map<String, BinlogConfig> configMap = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : ttlMap.entrySet()) {
            configMap.put(entry.getKey(), newTestBinlogConfig(true, entry.getValue()));
        }
        return new MockBinlogConfigCache(configMap);
    }

    public static TBinlog newBinlog(long dbId, long tableId, long commitSeq, long timestamp) {
        TBinlog binlog = new TBinlog();
        binlog.setDbId(dbId);
        binlog.setTableIds(Collections.singletonList(tableId));
        binlog.setType(TBinlogType.ALTER_JOB);
        binlog.setCommitSeq(commitSeq);
        binlog.setTimestamp(timestamp);
        binlog.setTableRef(0);
        binlog.setBelong(-1);
        return binlog;
    }

    public static TBinlog newBinlog(long dbId, List<Long> tableIds, long commitSeq, long timestamp) {
        TBinlog binlog = new TBinlog();
        binlog.setDbId(dbId);
        binlog.setTableIds(tableIds);
        binlog.setType(TBinlogType.ALTER_JOB);
        binlog.setCommitSeq(commitSeq);
        binlog.setTimestamp(timestamp);
        binlog.setTableRef(0);
        binlog.setBelong(-1);
        return binlog;
    }
}
