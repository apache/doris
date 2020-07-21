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
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class TaskFailedProcNode implements ProcNodeInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TaskSignature").add("FailedTimes")
            .build();

    private long backendId;
    private TTaskType type;

    public TaskFailedProcNode(long backendId, TTaskType type) {
        this.backendId = backendId;
        this.type = type;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<AgentTask> tasks = AgentTaskQueue.getFailedTask(backendId, type);

        for (AgentTask task : tasks) {
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(task.getSignature()));
            row.add(String.valueOf(task.getFailedTimes()));
            result.addRow(row);
        }

        return result;
    }

}
