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

import org.apache.doris.stack.entity.AgentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

/**
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 *
 * @Author: songchuanyuan@baidu.com
 * @Description：
 * @Date: 2021/8/18
 */
public interface AgentRepository extends JpaRepository<AgentEntity, Integer> {

    @Query("select f from AgentEntity f where f.host in (:hosts)")
    List<AgentEntity> queryAgentNodes(@Param("hosts") List<String> hosts);

    @Query("select f from AgentEntity f where f.host = :host and f.port = :port")
    List<AgentEntity> agentInfo(@Param("host")String host, @Param("port")Integer port);

}
