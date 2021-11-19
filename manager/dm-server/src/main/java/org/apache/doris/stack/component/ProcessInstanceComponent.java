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

package org.apache.doris.stack.component;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.dao.ProcessInstanceRepository;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Optional;

@Component
@Slf4j
public class ProcessInstanceComponent {

    @Autowired
    private ProcessInstanceRepository processInstanceRepository;

    /**
     * save process and return id
     */
    public int saveProcess(ProcessInstanceEntity processInstance) {
        checkHasUnfinishProcess(processInstance.getUserId(), processInstance.getId());
        return processInstanceRepository.save(processInstance).getId();
    }

    /**
     * check process is finished
     */
    public void checkProcessFinish(int processId) {
        Optional<ProcessInstanceEntity> optional = processInstanceRepository.findById(processId);
        Preconditions.checkArgument(optional.isPresent(), "install process is not exist");
        ProcessInstanceEntity processEntity = optional.get();
        if (Flag.YES.equals(processEntity.getFinish())) {
            throw new ServerException("install process " + processId + " is already finish");
        }
    }

    public void checkHasUnfinishProcess(int userId, int processId) {
        //query whether there is a process currently being installed
        ProcessInstanceEntity processEntity = processInstanceRepository.queryProcessByuserId(userId);
        if (processEntity != null && processEntity.getId() != processId) {
            throw new ServerException("You already have an installation in the current environment!");
        }
    }

    public ProcessInstanceEntity refreshProcess(int processId, ProcessTypeEnum processType) {
        ProcessInstanceEntity processInstance = queryProcessById(processId);
        Preconditions.checkArgument(processInstance != null, "install process is not exist");
        processInstance.setProcessType(processType);
        return processInstanceRepository.save(processInstance);
    }

    public ProcessInstanceEntity queryProcessByuserId(int userId) {
        return processInstanceRepository.queryProcessByuserId(userId);
    }

    /**
     * query process by id
     */
    public ProcessInstanceEntity queryProcessById(int processId) {
        Optional<ProcessInstanceEntity> optional = processInstanceRepository.findById(processId);
        if (optional.isPresent()) {
            return optional.get();
        }
        return null;
    }

    /**
     * finish process
     */
    public void finishProcess(ProcessInstanceEntity processInstance) {
        processInstance.setFinish(Flag.YES);
        processInstance.setUpdateTime(new Date());
        processInstanceRepository.save(processInstance);
    }
}
