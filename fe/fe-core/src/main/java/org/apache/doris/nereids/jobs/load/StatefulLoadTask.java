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

package org.apache.doris.nereids.jobs.load;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * for general load task state
 */
public abstract class StatefulLoadTask {

    private TaskType taskType;
    private MergeType mergeType = MergeType.APPEND;

    public StatefulLoadTask(TaskType initialTaskType) {
        this.taskType = initialTaskType;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public MergeType getMergeType() {
        return mergeType;
    }

    /**
     * merge type
     */
    enum MergeType {
        MERGE,
        APPEND,
        DELETE
    }

    /**
     * task type
     */
    enum TaskType {
        PENDING,
        LOADING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    public abstract void run(StmtExecutor executor, ConnectContext ctx) throws Exception;

    public abstract void onFinished();

    public abstract void onFailed();
}
