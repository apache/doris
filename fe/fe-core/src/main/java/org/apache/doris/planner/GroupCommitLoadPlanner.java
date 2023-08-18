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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// Used to generate a plan fragment for a group commit load.
// we only support OlapTable now.
public class GroupCommitLoadPlanner extends StreamLoadPlanner {
    private static final Logger LOG = LogManager.getLogger(GroupCommitLoadPlanner.class);

    public GroupCommitLoadPlanner(Database db, OlapTable destTable,
            LoadTaskInfo taskInfo) {
        super(db, destTable, taskInfo);
    }

    @Override
    protected ScanNode createScanNode(TUniqueId loadId, TupleDescriptor scanTupleDesc)
            throws AnalysisException, DdlException {
        return new GroupCommitScanNode(new PlanNodeId(0), scanTupleDesc, db.getId(), destTable.getId(),
                taskInfo.getTxnId());
    }

    @Override
    protected OlapTableSink getOlapTableSink(List<Long> partitionIds) {
        return new GroupCommitOlapTableSink(destTable, tupleDesc, partitionIds, Config.enable_single_replica_load);
    }
}

