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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
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
                              @RequestBody Map<String, Integer> hostPorts) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, String> failed = Maps.newHashMap();
        for (Map.Entry<String, Integer> backend : hostPorts.entrySet()) {
            List<Pair<String, Integer>> newBackend = Lists.newArrayList();
            newBackend.add(Pair.create(backend.getKey(), backend.getValue()));
            try {
                Catalog.getCurrentSystemInfo().addBackends(newBackend, false);
            } catch (UserException e) {
                LOG.error("Failed to add backend node: {}:{}", backend.getKey(), backend.getValue(), e);
                failed.put(backend.getKey(), e.getMessage());
            }
        }
        return ResponseEntityBuilder.ok(failed);
    }
}
