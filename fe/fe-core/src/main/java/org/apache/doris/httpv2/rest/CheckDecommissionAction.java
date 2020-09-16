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

package org.apache.doris.httpv2.rest;

import org.apache.doris.alter.SystemHandler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * calc row count from replica to table
 * fe_host:fe_http_port/api/check_decommission?host_ports=host:port,host2:port2...
 * return:
 * {
 * 	"msg": "OK",
 * 	"code": 0,
 * 	"data": ["192.168.10.11:9050", "192.168.10.11:9050"],
 * 	"count": 0
 * }
 */
@RestController
public class CheckDecommissionAction extends RestBaseController {

    public static final String HOST_PORTS = "host_ports";

    @RequestMapping(path = "/api/check_decommission", method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response) {
        //check user auth
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.OPERATOR);

        String hostPorts = request.getParameter(HOST_PORTS);
        if (Strings.isNullOrEmpty(hostPorts)) {
            return ResponseEntityBuilder.badRequest("No host:port specified");
        }

        String[] hostPortArr = hostPorts.split(",");
        if (hostPortArr.length == 0) {
            return ResponseEntityBuilder.badRequest("No host:port specified");
        }

        List<Pair<String, Integer>> hostPortPairs = Lists.newArrayList();
        for (String hostPort : hostPortArr) {
            Pair<String, Integer> pair;
            try {
                pair = SystemInfoService.validateHostAndPort(hostPort);
            } catch (AnalysisException e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }
            hostPortPairs.add(pair);
        }

        try {
            List<Backend> backends = SystemHandler.checkDecommission(hostPortPairs);
            List<String> backendsList = backends.stream().map(b -> b.getHost() + ":" + b.getHeartbeatPort()).collect(Collectors.toList());
            return ResponseEntityBuilder.ok(backendsList);
        } catch (DdlException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }
}
