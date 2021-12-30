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

package org.apache.doris.stack.dao;

import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TaskInstanceRepository extends JpaRepository<TaskInstanceEntity, Integer> {

    @Query("select f from TaskInstanceEntity f where f.processId = :processId")
    List<TaskInstanceEntity> queryTasksByProcessId(@Param("processId") int processId);

    @Query("select f from TaskInstanceEntity f where f.processId = :processId and f.processType = :processType")
    List<TaskInstanceEntity> queryTasksByProcessStep(@Param("processId") int processId, @Param("processType") ProcessTypeEnum processType);

    @Query("select f from TaskInstanceEntity f where f.processId = :processId and host = :host and processType = :processType and taskType = :taskType")
    TaskInstanceEntity queryTask(@Param("processId") int processId, @Param("host") String host, @Param("processType") ProcessTypeEnum processType, @Param("taskType") TaskTypeEnum taskType);

    @Query("select f from TaskInstanceEntity f where f.processId = :processId and taskType = :taskType order by f.id asc")
    List<TaskInstanceEntity> queryTasks(@Param("processId") int processId, @Param("taskType") TaskTypeEnum taskType);
}
