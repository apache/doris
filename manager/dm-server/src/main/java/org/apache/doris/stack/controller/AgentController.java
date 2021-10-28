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

package org.apache.doris.stack.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.model.request.BeJoinReq;
import org.apache.doris.stack.model.request.DeployConfigReq;
import org.apache.doris.stack.model.request.DorisExecReq;
import org.apache.doris.stack.model.request.DorisInstallReq;
import org.apache.doris.stack.service.AgentProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Agent API")
@RestController
@RequestMapping("/api/agent")
public class AgentController {

    @Autowired
    private AgentProcess agentProcess;

    /**
     * install doris service
     */
    @ApiOperation(value = "install doris service")
    @RequestMapping(value = "/installService", method = RequestMethod.POST)
    public RResult installService(HttpServletRequest request, HttpServletResponse response,
                                  @RequestBody DorisInstallReq installReq) throws Exception {
        agentProcess.installService(request, response, installReq);
        return RResult.success();
    }

    /**
     * deploy config
     */
    @ApiOperation(value = "deploy doris config")
    @RequestMapping(value = "/deployConfig", method = RequestMethod.POST)
    public RResult deployConfig(HttpServletRequest request, HttpServletResponse response,
                                @RequestBody DeployConfigReq deployConfigReq) throws Exception {
        agentProcess.deployConfig(request, response, deployConfigReq);
        return RResult.success();
    }

    /**
     * request agent:start stop fe/be
     */
    @ApiOperation(value = "start or stop service")
    @RequestMapping(value = "/execute", method = RequestMethod.POST)
    public RResult execute(HttpServletRequest request, HttpServletResponse response,
                           @RequestBody DorisExecReq dorisExec) throws Exception {
        agentProcess.execute(request, response, dorisExec);
        return RResult.success();
    }

    /**
     * join be to cluster
     */
    @ApiOperation(value = "join be to cluster")
    @RequestMapping(value = "/joinBe", method = RequestMethod.POST)
    public RResult joinBe(HttpServletRequest request, HttpServletResponse response,
                          @RequestBody BeJoinReq beJoinReq) throws Exception {
        agentProcess.joinBe(request, response, beJoinReq);
        return RResult.success();
    }

    /**
     * register role service (be/fe)
     */
    @ApiOperation(value = "register role service (be/fe)")
    @RequestMapping(value = "/register", method = RequestMethod.POST)
    public RResult register(@RequestBody AgentRoleRegister agentReg) {
        boolean register = agentProcess.register(agentReg);
        return RResult.success(register);
    }
}
