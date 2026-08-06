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

package org.apache.doris.httpv2.restv2;

import org.apache.doris.cluster.ClusterGuard;
import org.apache.doris.cluster.ClusterGuardException;
import org.apache.doris.cluster.ClusterGuardFactory;
import org.apache.doris.common.Pair;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.httpv2.rest.manager.NodeAction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * HTTP API to check and manage the cluster guard status.
 *
 * GET  /rest/v2/api/cluster_guard/status  — query current cluster guard status
 * POST /rest/v2/api/cluster_guard/reload  — reload cluster guard on all FE nodes (or single node)
 */
@RestController
@RequestMapping("/rest/v2")
public class ClusterGuardAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ClusterGuardAction.class);

    private static final String IS_ALL_NODE_PARA = "is_all_node";

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

    /**
     * Query the current cluster guard status.
     * Returns the opaque JSON from {@link ClusterGuard#getGuardInfo()}.
     */
    @RequestMapping(path = "/api/cluster_guard/status", method = {RequestMethod.GET})
    public Object getGuardStatus(HttpServletRequest request, HttpServletResponse response) {
        checkWithCookie(request, response, false);

        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Map<String, Object> info = GSON.fromJson(guard.getGuardInfo(), MAP_TYPE);
        return ResponseEntityBuilder.ok(info);
    }

    /**
     * Reload the cluster guard on all FE nodes (default) or just the current node.
     *
     * POST /rest/v2/api/cluster_guard/reload
     * POST /rest/v2/api/cluster_guard/reload?is_all_node=false
     */
    @RequestMapping(path = "/api/cluster_guard/reload", method = {RequestMethod.POST})
    public Object reloadGuard(HttpServletRequest request, HttpServletResponse response,
            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true")
                    boolean isAllNode) {
        executeCheckPassword(request, response);

        if (isAllNode) {
            return reloadOnAllFe(request);
        }
        return reloadOnLocal();
    }

    private Object reloadOnLocal() {
        String dorisHome = System.getenv("DORIS_HOME");
        try {
            ClusterGuardFactory.getGuard().onStartup(dorisHome);
            LOG.info("Cluster guard reloaded successfully via HTTP API.");
        } catch (ClusterGuardException e) {
            LOG.warn("Failed to reload cluster guard: {}", e.getMessage(), e);
            return ResponseEntityBuilder.okWithCommonError("Failed to reload cluster guard: " + e.getMessage());
        }

        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Map<String, Object> info = GSON.fromJson(guard.getGuardInfo(), MAP_TYPE);
        return ResponseEntityBuilder.ok(info);
    }

    private Object reloadOnAllFe(HttpServletRequest request) {
        List<Pair<String, Integer>> frontends = HttpUtils.getFeList();
        String authorization = request.getHeader(NodeAction.AUTHORIZATION);
        ImmutableMap<String, String> header = ImmutableMap.<String, String>builder()
                .put(NodeAction.AUTHORIZATION, authorization).build();

        String httpPath = "/rest/v2/api/cluster_guard/reload";
        Map<String, String> arguments = Maps.newHashMap();
        arguments.put(IS_ALL_NODE_PARA, "false");

        Map<String, Object> allResults = Maps.newLinkedHashMap();
        boolean hasFailure = false;

        for (Pair<String, Integer> ipPort : frontends) {
            String nodeKey = ipPort.first + ":" + ipPort.second;
            String url = HttpUtils.concatUrl(ipPort, httpPath, arguments);
            try {
                String resp = HttpUtils.doPost(url, header, null);
                JsonObject jsonObj = JsonParser.parseString(resp).getAsJsonObject();
                int code = jsonObj.get("code").getAsInt();
                if (code == HttpUtils.REQUEST_SUCCESS_CODE) {
                    Map<String, Object> nodeResult = Maps.newLinkedHashMap();
                    nodeResult.put("status", "success");
                    if (jsonObj.has("data") && !jsonObj.get("data").isJsonNull()) {
                        nodeResult.put("data", jsonObj.get("data").toString());
                    }
                    allResults.put(nodeKey, nodeResult);
                } else {
                    hasFailure = true;
                    Map<String, String> nodeResult = Maps.newLinkedHashMap();
                    nodeResult.put("status", "failed");
                    String msg = jsonObj.has("data") ? jsonObj.get("data").getAsString()
                            : jsonObj.get("msg").getAsString();
                    nodeResult.put("message", msg);
                    allResults.put(nodeKey, nodeResult);
                }
            } catch (Exception e) {
                hasFailure = true;
                LOG.warn("Failed to reload cluster guard on FE node {}: {}", nodeKey, e.getMessage(), e);
                Map<String, String> nodeResult = Maps.newLinkedHashMap();
                nodeResult.put("status", "failed");
                nodeResult.put("message", "Request failed: " + e.getMessage());
                allResults.put(nodeKey, nodeResult);
            }
        }

        if (hasFailure) {
            return ResponseEntityBuilder.internalError(allResults);
        }
        return ResponseEntityBuilder.ok(allResults);
    }
}
