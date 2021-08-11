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
package org.apache.doris.manager.server.controller;

import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.server.model.req.DorisExecReq;
import org.apache.doris.manager.server.model.req.DorisInstallReq;
import org.apache.doris.manager.server.model.req.TaskInfoReq;
import org.apache.doris.manager.server.model.req.TaskLogReq;
import org.apache.doris.manager.server.service.ServerAgent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/agent")
public class AgentController {

    @Autowired
    private ServerAgent serverAgent;

    /**
     * install doris
     */
    @RequestMapping(value = "/installDoris", method = RequestMethod.POST)
    public RResult install(@RequestBody DorisInstallReq installReq) {
        return RResult.success(serverAgent.install(installReq));
    }

    /**
     * request agent:start stop fe/be
     */
    @RequestMapping(value = "/execute", method = RequestMethod.POST)
    public RResult execute(@RequestBody DorisExecReq dorisExec) {
        return RResult.success(serverAgent.execute(dorisExec));
    }

    /**
     * request task detail
     */
    @RequestMapping(value = "/task", method = RequestMethod.POST)
    public RResult taskInfo(@RequestBody TaskInfoReq taskInfo) {
        return serverAgent.taskInfo(taskInfo);
    }

    /**
     * request task stdout log
     */
    @RequestMapping(value = "/stdlog", method = RequestMethod.POST)
    public RResult taskStdlog(@RequestBody TaskLogReq taskInfo) {
        return serverAgent.taskStdlog(taskInfo);
    }

    /**
     * request task error log
     */
    @RequestMapping(value = "/errlog", method = RequestMethod.POST)
    public RResult taskErrlog(@RequestBody TaskLogReq taskInfo) {
        return serverAgent.taskErrlog(taskInfo);
    }

}
