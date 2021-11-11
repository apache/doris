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

import org.apache.doris.stack.entity.AgentRoleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface AgentRoleRepository extends JpaRepository<AgentRoleEntity, Integer> {

    @Query("select f from AgentRoleEntity f where f.role = :role and f.clusterId = :clusterId")
    List<AgentRoleEntity> queryAgentByRole(@Param("role") String role, @Param("clusterId") int clusterId);

    @Query("select f from AgentRoleEntity f where f.host = :host")
    List<AgentRoleEntity> queryAgentByHost(@Param("host") String host);

    @Query("select f from AgentRoleEntity f where f.host = :host and f.role = :role")
    AgentRoleEntity queryByHostRole(@Param("host") String host, @Param("role") String role);

    @Query("select f from AgentRoleEntity f where f.clusterId = :clusterId")
    List<AgentRoleEntity> queryAgentRoles(@Param("clusterId") int clusterId);
}
