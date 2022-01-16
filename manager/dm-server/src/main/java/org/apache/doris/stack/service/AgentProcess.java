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

import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.manager.common.domain.HardwareInfo;
import org.apache.doris.stack.model.request.BuildClusterReq;
import org.apache.doris.stack.model.request.DeployConfigReq;
import org.apache.doris.stack.model.request.DorisExecReq;
import org.apache.doris.stack.model.request.DorisInstallReq;
import org.apache.doris.stack.model.request.DorisStartReq;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * server agent
 **/
public interface AgentProcess {

    /**
     * install doris
     */
    void installService(HttpServletRequest request, HttpServletResponse response, DorisInstallReq installReq) throws Exception;

    /**
     * deploy config
     */
    void deployConfig(HttpServletRequest request, HttpServletResponse response, DeployConfigReq deployConfigReq) throws Exception;

    /**
     * start service
     */
    void startService(HttpServletRequest request, HttpServletResponse response, DorisStartReq dorisStart) throws Exception;

    void buildCluster(HttpServletRequest request, HttpServletResponse response, BuildClusterReq buildClusterReq) throws Exception;

    boolean register(AgentRoleRegister agentReg);

    List<Integer> execute(DorisExecReq dorisExec);

    Object log(String host, String type);

    List<HardwareInfo> hardwareInfo(int clusterId);
}
