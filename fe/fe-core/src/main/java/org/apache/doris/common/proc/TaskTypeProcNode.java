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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class TaskTypeProcNode implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BackendId").add("FailedNum").add("TotalNum")
            .build();

    private TTaskType type;

    public TaskTypeProcNode(TTaskType type) {
        this.type = type;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        int totalFailedNum = 0;
        int totalTaskNum = 0;
        List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(false);
        for (Long backendId : backendIds) {
            int failedNum = AgentTaskQueue.getTaskNum(backendId, type, true);
            int taskNum = AgentTaskQueue.getTaskNum(backendId, type, false);
            List<String> row = Lists.newArrayList();
            row.add(backendId.toString());
            row.add(String.valueOf(failedNum));
            row.add(String.valueOf(taskNum));

            result.addRow(row);

            totalFailedNum += failedNum;
            totalTaskNum += taskNum;
        }

        List<String> sumRow = Lists.newArrayList();
        sumRow.add("Total");
        sumRow.add(String.valueOf(totalFailedNum));
        sumRow.add(String.valueOf(totalTaskNum));
        result.addRow(sumRow);

        return result;
    }

    @Override
    public ProcNodeInterface lookup(String backendIdStr) throws AnalysisException {
        long backendId = -1L;
        try {
            backendId = Long.valueOf(backendIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid backend id format: " + backendIdStr);
        }

        return new TaskFailedProcNode(backendId, type);
    }

}
