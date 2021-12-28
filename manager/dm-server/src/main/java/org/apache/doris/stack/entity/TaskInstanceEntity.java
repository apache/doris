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

package org.apache.doris.stack.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.model.response.TaskInstanceResp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * task instance entity
 **/
@Entity
@Table(name = "task_instance")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskInstanceEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name = "process_id")
    private int processId;

    @Column(name = "task_json", columnDefinition = "LONGTEXT")
    private String taskJson;

    @Column(name = "host", nullable = false)
    private String host;

    @Enumerated(EnumType.STRING)
    @Column(name = "process_type")
    private ProcessTypeEnum processType;

    @Enumerated(EnumType.STRING)
    @Column(name = "task_type", nullable = false)
    private TaskTypeEnum taskType;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ExecutionStatus status;

    @Column(name = "start_time", nullable = false)
    private Date startTime;

    @Column(name = "end_time")
    private Date endTime;

    @Column(name = "executor_id")
    private String executorId;

    @Column(name = "result", columnDefinition = "LONGTEXT")
    private String result;

    @Enumerated(EnumType.ORDINAL)
    @Column(name = "finish", nullable = false)
    private Flag finish;

    @Enumerated(EnumType.ORDINAL)
    @Column(name = "skip", nullable = false)
    private Flag skip;

    public TaskInstanceEntity(int processId, String host, ProcessTypeEnum processType, TaskTypeEnum taskType, ExecutionStatus status) {
        this.processId = processId;
        this.host = host;
        this.processType = processType;
        this.taskType = taskType;
        this.status = status;
        this.startTime = new Date();
        this.finish = Flag.NO;
        this.skip = Flag.NO;
    }

    public TaskInstanceEntity(int processId, String host, ProcessTypeEnum processType, ExecutionStatus status) {
        this.processId = processId;
        this.host = host;
        this.processType = processType;
        this.startTime = new Date();
        this.finish = Flag.NO;
        this.skip = Flag.NO;
        this.status = status;
    }

    public TaskInstanceEntity(String host) {
        this.host = host;
        this.startTime = new Date();
        this.finish = Flag.NO;
        this.skip = Flag.NO;
    }

    public TaskInstanceResp transToModel() {
        TaskInstanceResp taskResp = new TaskInstanceResp();
        taskResp.setId(id);
        taskResp.setProcessId(processId);
        taskResp.setHost(host);
        taskResp.setProcessType(processType);
        taskResp.setTaskType(taskType);
        taskResp.setStatus(status);
        taskResp.setStartTime(startTime);
        taskResp.setEndTime(endTime);
        taskResp.setFinish(finish);
        taskResp.setSkip(skip);
        return taskResp;
    }
}
