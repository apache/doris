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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Frontend;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * Used to return the cluster information for the manager.
 */
@RestController
@RequestMapping(path = {"/rest/v2/manager/cluster", "/rest/v2/manager/compute_group"})
public class ClusterAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ClusterAction.class);

    // Returns mysql and http connection information for the cluster.
    // {
    //   "mysql":[
    //     ""
    //   ],
    //   "http":[
    //     ""
    //   ]
    // }
    @RequestMapping(path = {"/cluster_info/conn_info", "/compute_group_info/conn_info"}, method = RequestMethod.GET)
    public Object clusterInfo(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, List<String>> result = Maps.newHashMap();
        List<String> frontends = Env.getCurrentEnv().getFrontends(null)
                .stream().filter(Frontend::isAlive)
                .map(Frontend::getHost)
                .collect(Collectors.toList());

        result.put("mysql", frontends.stream().map(ip -> NetUtils
                .getHostPortInAccessibleFormat(ip, Config.query_port)).collect(Collectors.toList()));
        result.put("http", frontends.stream().map(ip -> NetUtils
                .getHostPortInAccessibleFormat(ip, Config.http_port)).collect(Collectors.toList()));
        result.put("arrow flight sql server", frontends.stream().map(
                ip -> NetUtils.getHostPortInAccessibleFormat(ip, Config.arrow_flight_sql_port))
                .collect(Collectors.toList()));
        return ResponseEntityBuilder.ok(result);
    }

    public static class BeClusterInfo {
        public volatile String host;
        public volatile int heartbeatPort;
        public volatile int bePort;
        public volatile int httpPort;
        public volatile int brpcPort;
        public volatile long currentFragmentNum = 0;
        public volatile long lastFragmentUpdateTime = 0;

        @Override
        public String toString() {
            return "BeClusterInfo{"
                + "host='" + host + '\''
                + ", heartbeatPort=" + heartbeatPort
                + ", bePort=" + bePort
                + ", httpPort=" + httpPort
                + ", brpcPort=" + brpcPort
                + ", currentFragmentNum=" + currentFragmentNum
                + ", lastFragmentUpdateTime=" + lastFragmentUpdateTime
                + '}';
        }
    }

    @RequestMapping(path = {"/cluster_info/cloud_cluster_status", "/compute_group_info/compute_group_status"},
            method = RequestMethod.GET)
    public Object cloudClusterInfo(HttpServletRequest request, HttpServletResponse response) {
        ResponseEntity ret = null;
        try {
            if (!Env.getCurrentEnv().isMaster()) {
                ret = ResponseEntityBuilder.badRequest("this api just use in cloud master fe");
            } else {
                executeCheckPassword(request, response);
                checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

                // Key: cluster_name Value: be status
                Map<String, List<BeClusterInfo>> result = Maps.newHashMap();

                ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdToBackend()
                        .forEach((clusterId, backends) -> {
                            List<BeClusterInfo> bis = backends.stream().map(backend -> {
                                BeClusterInfo bi = new BeClusterInfo();
                                bi.host = backend.getHost();
                                bi.heartbeatPort = backend.getHeartbeatPort();
                                bi.bePort = backend.getBePort();
                                bi.httpPort = backend.getHttpPort();
                                bi.brpcPort = backend.getBrpcPort();
                                bi.currentFragmentNum = backend.getBackendStatus().currentFragmentNum;
                                bi.lastFragmentUpdateTime = backend.getBackendStatus().lastFragmentUpdateTime;
                                return bi;
                            }).collect(Collectors.toList());
                            result.put(clusterId, bis);
                        });

                ret = ResponseEntityBuilder.ok(result);
            }
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {}, response {}", request.getRequestURI(), ret);
            }
        }
        return ret;
    }
}
