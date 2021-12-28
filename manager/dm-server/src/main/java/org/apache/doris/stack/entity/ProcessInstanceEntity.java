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
import org.apache.doris.stack.model.response.CurrentProcessResp;

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
 * process instance entity
 **/
@Entity
@Table(name = "process_instance")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessInstanceEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name = "cluster_id", nullable = false)
    private int clusterId;

    @Column(name = "user_id", nullable = false)
    private int userId;

    @Enumerated(EnumType.STRING)
    @Column(name = "process_type", nullable = false)
    private ProcessTypeEnum processType;

    @Column(name = "create_time", nullable = false)
    private Date createTime;

    @Column(name = "update_time", nullable = false)
    private Date updateTime;

    @Enumerated(EnumType.ORDINAL)
    @Column(name = "finish", nullable = false)
    private Flag finish;

    @Column(name = "package_url", length = 1024)
    private String packageUrl;

    @Column(name = "install_dir", length = 1024)
    private String installDir;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ExecutionStatus status;

    public ProcessInstanceEntity(int clusterId, int userId, ProcessTypeEnum processType, String packageUrl, String installDir) {
        this.clusterId = clusterId;
        this.userId = userId;
        this.processType = processType;
        this.installDir = installDir;
        this.packageUrl = packageUrl;
        this.createTime = new Date();
        this.updateTime = new Date();
        this.finish = Flag.NO;
        this.status = ExecutionStatus.RUNNING;
    }

    public CurrentProcessResp transToCurrentResp() {
        CurrentProcessResp processResp = new CurrentProcessResp();
        processResp.setId(id);
        processResp.setClusterId(clusterId);
        processResp.setProcessType(processType);
        processResp.setProcessStep(processType.getCode());
        processResp.setCreateTime(createTime);
        processResp.setUpdateTime(updateTime);
        processResp.setStatus(status);
        return processResp;
    }
}
