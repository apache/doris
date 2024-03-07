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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TGroupCommitMode;

import com.google.common.base.Preconditions;

import java.util.List;

public class GroupCommitBlockSink extends OlapTableSink {
    private String groupCommit;
    private double maxFilterRatio;

    public GroupCommitBlockSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
            boolean singleReplicaLoad, String groupCommit, double maxFilterRatio) {
        super(dstTable, tupleDescriptor, partitionIds, singleReplicaLoad);
        this.groupCommit = groupCommit;
        this.maxFilterRatio = maxFilterRatio;
    }

    protected TDataSinkType getDataSinkType() {
        return TDataSinkType.GROUP_COMMIT_BLOCK_SINK;
    }

    @Override
    protected TDataSink toThrift() {
        TGroupCommitMode groupCommitMode = parseGroupCommit(groupCommit);
        Preconditions.checkNotNull(groupCommitMode, "Group commit is: " + groupCommit);
        tDataSink.olap_table_sink.setGroupCommitMode(groupCommitMode);
        tDataSink.olap_table_sink.setMaxFilterRatio(maxFilterRatio);
        return tDataSink;
    }

    public static TGroupCommitMode parseGroupCommit(String groupCommit) {
        if (groupCommit == null) {
            return null;
        } else if (groupCommit.equalsIgnoreCase("async_mode")) {
            return TGroupCommitMode.ASYNC_MODE;
        } else if (groupCommit.equalsIgnoreCase("sync_mode")) {
            return TGroupCommitMode.SYNC_MODE;
        } else if (groupCommit.equalsIgnoreCase("off_mode")) {
            return TGroupCommitMode.OFF_MODE;
        } else {
            return null;
        }
    }
}
