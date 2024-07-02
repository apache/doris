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

package org.apache.doris.job.scheduler;

import org.apache.doris.job.common.JobType;

import lombok.Data;

import java.util.List;

@Data
public class TaskDispatchEvent {

    private Long lastCompletedTaskId;

    private Long groupId;

    private JobType jobType;

    private TaskDispatchOperate taskDispatchOperate;

    private List<Long> groupIds;

    public TaskDispatchEvent(Long lastCompletedTaskId, Long groupId, TaskDispatchOperate taskDispatchOperate) {
        this.lastCompletedTaskId = lastCompletedTaskId;
        this.groupId = groupId;
        this.taskDispatchOperate = taskDispatchOperate;
    }

    public TaskDispatchEvent(Long lastCompletedTaskId, List<Long> groupIds, TaskDispatchOperate taskDispatchOperate) {
        this.lastCompletedTaskId = lastCompletedTaskId;
        this.taskDispatchOperate = taskDispatchOperate;
        this.groupIds = groupIds;
    }


}
