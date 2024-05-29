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

import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import java.util.TreeSet;

public class BinlogUtils {
    public static Pair<TStatus, TBinlog> getBinlog(TreeSet<TBinlog> binlogs, long prevCommitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        TBinlog firstBinlog = binlogs.first();

        // all commitSeq > commitSeq
        if (firstBinlog.getCommitSeq() > prevCommitSeq) {
            status.setStatusCode(TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ);
            return Pair.of(status, firstBinlog);
        }

        // find first binlog whose commitSeq > commitSeq
        TBinlog guard = new TBinlog();
        guard.setCommitSeq(prevCommitSeq);
        TBinlog binlog = binlogs.higher(guard);

        // all commitSeq <= prevCommitSeq
        if (binlog == null) {
            status.setStatusCode(TStatusCode.BINLOG_TOO_NEW_COMMIT_SEQ);
            return Pair.of(status, null);
        } else {
            return Pair.of(status, binlog);
        }
    }

    public static Pair<TStatus, Long> getBinlogLag(TreeSet<TBinlog> binlogs, long prevCommitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        TBinlog firstBinlog = binlogs.first();

        if (firstBinlog.getCommitSeq() > prevCommitSeq) {
            return Pair.of(status, Long.valueOf(binlogs.size()));
        }

        // find first binlog whose commitSeq > commitSeq
        TBinlog guard = new TBinlog();
        guard.setCommitSeq(prevCommitSeq);
        TBinlog binlog = binlogs.higher(guard);

        // all prevCommitSeq <= commitSeq
        if (binlog == null) {
            return Pair.of(status, 0L);
        } else {
            return Pair.of(status, Long.valueOf(binlogs.tailSet(binlog).size()));
        }
    }

    public static TBinlog newDummyBinlog(long dbId, long tableId) {
        TBinlog dummy = new TBinlog();
        dummy.setCommitSeq(-1);
        dummy.setTimestamp(-1);
        dummy.setType(TBinlogType.DUMMY);
        dummy.setDbId(dbId);
        dummy.setBelong(tableId);
        return dummy;
    }

    // Compute the expired timestamp in milliseconds.
    public static long getExpiredMs(long ttlSeconds) {
        long currentSeconds = System.currentTimeMillis() / 1000;
        if (currentSeconds < ttlSeconds) {
            return Long.MIN_VALUE;
        }

        long expireSeconds = currentSeconds - ttlSeconds;
        return expireSeconds * 1000;
    }

    public static String convertTimeToReadable(long time) {
        return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(time));
    }

    public static long getApproximateMemoryUsage(TBinlog binlog) {
        /* object layout: header + body + padding */
        final long objSize = 80; // 9 fields and 1 header
        String data = binlog.getData();
        return objSize + binlog.getTableIdsSize() * 8 + (data == null ? 0 : data.length());
    }
}
