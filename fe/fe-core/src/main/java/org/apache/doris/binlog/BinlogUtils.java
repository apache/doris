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
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import java.util.TreeSet;

public class BinlogUtils {
    public static Pair<TStatus, TBinlog> getBinlog(TreeSet<TBinlog> binlogs, long commitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        TBinlog firstBinlog = binlogs.first();

        // all commitSeq > commitSeq
        if (firstBinlog.getCommitSeq() > commitSeq) {
            status.setStatusCode(TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ);
            return Pair.of(status, firstBinlog);
        }

        // find first binlog whose commitSeq > commitSeq
        TBinlog guard = new TBinlog();
        guard.setCommitSeq(commitSeq);
        TBinlog binlog = binlogs.higher(guard);

        // all commitSeq <= commitSeq
        if (binlog == null) {
            status.setStatusCode(TStatusCode.BINLOG_TOO_NEW_COMMIT_SEQ);
            return Pair.of(status, null);
        } else {
            return Pair.of(status, binlog);
        }
    }
}
