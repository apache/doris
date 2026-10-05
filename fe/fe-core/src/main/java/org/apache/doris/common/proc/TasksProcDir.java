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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class TasksProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TaskType").add("FailedNum").add("TotalNum")
            .build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String typeName) throws AnalysisException {
        if (Strings.isNullOrEmpty(typeName)) {
            throw new AnalysisException("type name is null");
        }

        TTaskType type = null;
        try {
            type = TTaskType.valueOf(typeName);
        } catch (Exception e) {
            throw new AnalysisException("invalid type name: " + typeName);
        }

        return new TaskTypeProcNode(type);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        int totalFailedNum = 0;
        int totalTaskNum = 0;
        for (TTaskType type : TTaskType.values()) {
            int failedNum = AgentTaskQueue.getTaskNum(-1, type, true);
            int taskNum = AgentTaskQueue.getTaskNum(-1, type, false);
            List<String> row = Lists.newArrayList();
            row.add(type.name());
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
}
