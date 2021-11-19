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

package org.apache.doris.stack.service;

import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.model.request.AgentInstallReq;
import org.apache.doris.stack.model.request.AgentRegister;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * server
 */
public interface ServerProcess {

    /**
     * install agent
     */
    int installAgent(HttpServletRequest request, HttpServletResponse response, AgentInstallReq agentInstallReq) throws Exception;

    /**
     * agent list
     */
    List<AgentEntity> agentList(int clusterId);

    List<AgentRoleEntity> roleList(int clusterId);

    List<AgentRoleEntity> agentRole(String host);

    void heartbeat(String host, Integer port);

    boolean register(AgentRegister agent);
}
