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

public class BinlogLagInfo {
    private long lag;
    private long firstCommitSeq;
    private long lastCommitSeq;
    private long firstCommitTs;
    private long lastCommitTs;
    private long nextCommitSeq;
    private long nextCommitTs;

    public BinlogLagInfo(long lag, long firstCommitSeq, long lastCommitSeq, long firstCommitTs, long lastCommitTs,
            long nextCommitSeq, long nextCommitTs) {
        this.lag = lag;
        this.firstCommitSeq = firstCommitSeq;
        this.lastCommitSeq = lastCommitSeq;
        this.firstCommitTs = firstCommitTs;
        this.lastCommitTs = lastCommitTs;
        this.nextCommitSeq = nextCommitSeq;
        this.nextCommitTs = nextCommitTs;
    }

    public BinlogLagInfo() {
        lag = 0;
        firstCommitSeq = 0;
        lastCommitSeq = 0;
        firstCommitTs = 0;
        lastCommitTs = 0;
        nextCommitSeq = 0;
        nextCommitTs = 0;
    }

    public long getNextCommitSeq() {
        return nextCommitSeq;
    }

    public long getNextCommitTs() {
        return nextCommitTs;
    }

    public long getLag() {
        return lag;
    }

    public long getFirstCommitSeq() {
        return firstCommitSeq;
    }

    public long getLastCommitSeq() {
        return lastCommitSeq;
    }

    public long getFirstCommitTs() {
        return firstCommitTs;
    }

    public long getLastCommitTs() {
        return lastCommitTs;
    }

}
