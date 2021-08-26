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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.dao.AgentRepository;
import org.apache.doris.stack.dao.AgentRoleRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.req.AgentRegister;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Component
@Slf4j
public class AgentRoleComponent {

    @Autowired
    private AgentRoleRepository agentRoleRepository;

    public List<AgentRoleEntity> queryAgentRoles() {
        return agentRoleRepository.findAll();
    }

    public List<AgentRoleEntity> queryAgentByRole(String role) {
        if (StringUtils.isBlank(role)) {
            return Lists.newArrayList();
        }
        return agentRoleRepository.queryAgentByRole(role);
    }

    public List<AgentRoleEntity> queryAgentByHost(String host) {
        if (StringUtils.isBlank(host)) {
            return Lists.newArrayList();
        }
        return agentRoleRepository.queryAgentByHost(host);
    }

    public AgentRoleEntity registerAgentRole(AgentRoleRegister agentReg) {
        AgentRoleEntity agentRoleEntity = new AgentRoleEntity(agentReg.getHost(), agentReg.getRole(), agentReg.getInstallDir());
        return agentRoleRepository.save(agentRoleEntity);
    }


}
