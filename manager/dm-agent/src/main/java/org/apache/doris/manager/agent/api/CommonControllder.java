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

package org.apache.doris.manager.agent.api;

import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.agent.service.BeService;
import org.apache.doris.manager.agent.service.BrokerService;
import org.apache.doris.manager.agent.service.FeService;
import org.apache.doris.manager.agent.service.Service;
import org.apache.doris.manager.agent.service.ServiceContext;
import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
@RequestMapping
public class CommonControllder {

    @GetMapping("/context/agent")
    public RResult agentContext() {
        return RResult.success(AgentContext.getContext());
    }

    @GetMapping("/context/service")
    public RResult serviceContext() {
        return RResult.success(ServiceContext.getServiceMap());
    }

    @GetMapping("/service/config")
    public RResult getServiceConfig(@RequestParam String serviceRole) {
        Service service = ServiceContext.getServiceMap().get(ServiceRole.findByName(serviceRole));
        if (Objects.isNull(service)) {
            return RResult.error("service not installed");
        }
        return RResult.success(service.getConfig());
    }

    @PostMapping("/service/register")
    public RResult registerService(@RequestBody AgentRoleRegister register) {
        if (Objects.isNull(register.getHost()) || !register.getHost().equals(AgentContext.getAgentIp())) {
            return RResult.error("param host is null or param host not equal agent ip");
        }

        Service service = ServiceContext.getServiceMap().get(ServiceRole.findByName(register.getRole()));
        if (Objects.nonNull(service)) {
            return RResult.error("service already registered");
        }

        if (ServiceRole.findByName(register.getRole()) == ServiceRole.FE) {
            FeService feService = new FeService(register.getInstallDir());
            ServiceContext.register(feService);
        } else if (ServiceRole.findByName(register.getRole()) == ServiceRole.BE) {
            BeService beService = new BeService(register.getInstallDir());
            ServiceContext.register(beService);
        } else if (ServiceRole.findByName(register.getRole()) == ServiceRole.BROKER) {
            BrokerService brokerService = new BrokerService(register.getInstallDir());
            ServiceContext.register(brokerService);
        } else {
            return RResult.error("unkown service role");
        }

        return RResult.success();
    }
}
