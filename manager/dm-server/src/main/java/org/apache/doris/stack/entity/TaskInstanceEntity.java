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
import org.apache.doris.stack.constants.TaskTypeEnum;

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

    @Column(name = "process_id", nullable = false)
    private int processId;

    @Column(name = "host", nullable = false)
    private String host;

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

    @Column(name = "result")
    private String result;

    public TaskInstanceEntity(int processId, String host, TaskTypeEnum taskType, ExecutionStatus status) {
        this.processId = processId;
        this.host = host;
        this.taskType = taskType;
        this.status = status;
        this.startTime = new Date();
    }

    public TaskInstanceEntity(int processId, String host) {
        this.processId = processId;
        this.host = host;
        this.startTime = new Date();
    }
}
