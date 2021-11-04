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

package org.apache.doris.httpv2.rest.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Frontend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Used to return the cluster information for the manager.
 */
@RestController
@RequestMapping("/rest/v2/manager/cluster")
public class ClusterAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(ClusterAction.class);


    // Returns mysql and http connection information for the cluster.
    // {
    //		"mysql":[
    //			""
    //		],
    //		"http":[
    //			""
    //		]
    //	}
    @RequestMapping(path = "/cluster_info/conn_info", method = RequestMethod.GET)
    public Object clusterInfo(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, List<String>> result = Maps.newHashMap();
        List<String> frontends = Catalog.getCurrentCatalog().getFrontends(null)
                .stream().filter(Frontend::isAlive)
                .map(Frontend::getHost)
                .collect(Collectors.toList());

        result.put("mysql", frontends.stream().map(ip -> ip + ":" + Config.query_port).collect(Collectors.toList()));
        result.put("http", frontends.stream().map(ip -> ip + ":" + Config.http_port).collect(Collectors.toList()));
        return ResponseEntityBuilder.ok(result);
    }

    /**
     * add backend like execute alter system add backend "host:port"
     */
    @RequestMapping(path = "/add_backends", method = RequestMethod.POST)
    public Object addBackends(HttpServletRequest request, HttpServletResponse response,
                              @RequestBody Map<String, Integer> hostPorts) throws AnalysisException {
        executeCheckPassword(request, response);
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "NODE");
        }

        if (!Catalog.getCurrentCatalog().isMaster()) {
            return forwardToMaster(request, hostPorts);
        } else {
            Map<String, Boolean> result = Maps.newHashMap();
            for (Map.Entry<String, Integer> backend : hostPorts.entrySet()) {
                List<Pair<String, Integer>> newBackend = Lists.newArrayList();
                newBackend.add(Pair.create(backend.getKey(), backend.getValue()));
                try {
                    Catalog.getCurrentSystemInfo().addBackends(newBackend, false);
                    result.put(backend.getKey(), true);
                } catch (UserException e) {
                    LOG.error("Failed to add backend node: {}:{}", backend.getKey(), backend.getValue(), e);
                    result.put(backend.getKey(), false);
                }
            }
            return ResponseEntityBuilder.ok(result);
        }
    }

    private Object forwardToMaster(HttpServletRequest request, Map<String, Integer> hostPorts) {
        Pair<String, Integer> hostPort = Pair.create(Catalog.getCurrentCatalog().getMasterIp(), Catalog.getCurrentCatalog().getMasterHttpPort());
        String url = HttpUtils.concatUrl(hostPort, request.getRequestURI(), Maps.newHashMap());
        try {
            Map<String, String> headers = Maps.newHashMap();
            headers.put(NodeAction.AUTHORIZATION, request.getHeader(NodeAction.AUTHORIZATION));
            headers.put("Content-Type", "application/json; charset=utf-8");
            String response = HttpUtils.doPost(url, headers, hostPorts);
            return new ObjectMapper().readValue(response, Map.class);
        } catch (Exception e) {
            return ResponseEntityBuilder.internalError("Failed to forward request to master: " + e.getMessage());
        }
    }
}
