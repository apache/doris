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
package org.apache.doris.manager.server.service;

import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.server.entity.AgentEntity;
import org.apache.doris.manager.server.model.req.SshInfo;

import java.util.List;

/**
 * server
 */
public interface ServerProcess {


    void initAgent(SshInfo sshInfo);

    /**
     * install agent
     */
    void startAgent(SshInfo sshInfo);

    /**
     * agent list
     */
    List<AgentEntity> agentList();

    /**
     * update agent status batch
     */
    int updateBatchAgentStatus(List<AgentEntity> agents);

    String agentRole(String host);

    void heartbeat(String host, Integer port);

    RResult register(String host, Integer port);
}
