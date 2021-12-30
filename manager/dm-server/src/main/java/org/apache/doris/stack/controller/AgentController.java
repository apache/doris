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
import org.apache.doris.stack.model.request.BuildClusterReq;
import org.apache.doris.stack.model.request.DeployConfigReq;
import org.apache.doris.stack.model.request.DorisExecReq;
import org.apache.doris.stack.model.request.DorisInstallReq;
import org.apache.doris.stack.model.request.DorisStartReq;
import org.apache.doris.stack.service.AgentProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

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
     * Start the service when installing the cluster
     */
    @ApiOperation(value = "Start the service when installing the cluster")
    @RequestMapping(value = "/startService", method = RequestMethod.POST)
    public RResult startService(HttpServletRequest request, HttpServletResponse response,
                                @RequestBody DorisStartReq dorisStart) throws Exception {
        agentProcess.startService(request, response, dorisStart);
        return RResult.success();
    }

    /**
     * build cluster add backend,add fe,add broker
     */
    @ApiOperation(value = "build cluster")
    @RequestMapping(value = "/buildCluster", method = RequestMethod.POST)
    public RResult buildCluster(HttpServletRequest request, HttpServletResponse response,
                          @RequestBody BuildClusterReq buildClusterReq) throws Exception {
        agentProcess.buildCluster(request, response, buildClusterReq);
        return RResult.success();
    }

    /**
     * register role service (be/fe/broker)
     */
    @ApiOperation(value = "register role service (be/fe/broker)")
    @RequestMapping(value = "/register", method = RequestMethod.POST)
    public RResult register(@RequestBody AgentRoleRegister agentReg) {
        boolean register = agentProcess.register(agentReg);
        return RResult.success(register);
    }

    /**
     * execute command: START/STOP
     */
    @ApiOperation(value = "execute command:START/STOP")
    @RequestMapping(value = "/execute", method = RequestMethod.POST)
    public RResult execute(@RequestBody DorisExecReq dorisExec) {
        List<Integer> taskIds = agentProcess.execute(dorisExec);
        return RResult.success(taskIds);
    }

    /**
     * query log
     * type:fe.log/fe.out/be.log/be.out
     */
    @ApiOperation(value = "query service log,type:fe.log|fe.out|be.log|be.out")
    @RequestMapping(value = "/log", method = RequestMethod.GET)
    public RResult log(@RequestParam String host, @RequestParam String type) {
        return RResult.success(agentProcess.log(host, type));
    }

    /**
     * query hardware info
     */
    @ApiOperation(value = "query hardware info")
    @RequestMapping(value = "/hardware/{clusterId}", method = RequestMethod.GET)
    public RResult hardwareInfo(@PathVariable int clusterId) {
        return RResult.success(agentProcess.hardwareInfo(clusterId));
    }

}
