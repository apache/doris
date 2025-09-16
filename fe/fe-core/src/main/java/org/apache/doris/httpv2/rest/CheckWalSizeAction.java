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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * cal wal size of specific be
 * fe_host:fe_http_port/api/get_wal_size?host_ports=host:port,host2:port2...
 * return:
 * {
 * "msg": "OK",
 * "code": 0,
 * "data": ["192.168.10.11:9050:1", "192.168.10.11:9050:0"],
 * "count": 0
 * }
 */

@RestController
public class CheckWalSizeAction extends RestBaseController {
    public static final String HOST_PORTS = "host_ports";

    @RequestMapping(path = "/api/get_wal_size", method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response) {
        // check user auth
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.OPERATOR);

        String hostPorts = request.getParameter(HOST_PORTS);
        List<Backend> backends = new ArrayList<>();
        if (Strings.isNullOrEmpty(hostPorts)) {
            try {
                backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList();
            } catch (AnalysisException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
        } else {
            String[] hostPortArr = hostPorts.split(",");
            if (hostPortArr.length == 0) {
                return ResponseEntityBuilder.badRequest("No host:port specified");
            }
            List<HostInfo> hostInfos = new ArrayList<>();
            for (String hostPort : hostPortArr) {
                try {
                    HostInfo hostInfo = SystemInfoService.getHostAndPort(hostPort);
                    hostInfos.add(hostInfo);
                } catch (AnalysisException e) {
                    return ResponseEntityBuilder.badRequest(e.getMessage());
                }
            }
            try {
                backends = getBackends(hostInfos);
            } catch (DdlException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
        }

        List<String> backendsList = new ArrayList<>();
        for (Backend backend : backends) {
            long size = Env.getCurrentEnv().getGroupCommitManager().getAllWalQueueSize(backend);
            backendsList.add(backend.getHost() + ":" + backend.getHeartbeatPort() + ":" + size);
        }
        return ResponseEntityBuilder.ok(backendsList);
    }

    private List<Backend> getBackends(List<HostInfo> hostInfos) throws DdlException {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Backend> backends = Lists.newArrayList();
        // check if exist
        for (HostInfo hostInfo : hostInfos) {
            Backend backend = infoService.getBackendWithHeartbeatPort(hostInfo.getHost(),
                    hostInfo.getPort());
            if (backend == null) {
                throw new DdlException("Backend does not exist["
                        + hostInfo.getHost()
                        + ":" + hostInfo.getPort() + "]");
            }
            backends.add(backend);
        }
        return backends;
    }
}
