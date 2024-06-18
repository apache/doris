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

package org.apache.doris.httpv2.controller;

import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.httpv2.rest.manager.NodeAction;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Frontend;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/rest/v1")
public class SessionController extends RestBaseController {

    private static final List<String> SESSION_TABLE_HEADER = Lists.newArrayList();
    private static final Logger LOG = LogManager.getLogger(SessionController.class);

    static {
        SESSION_TABLE_HEADER.add("CurrentConnected");
        SESSION_TABLE_HEADER.add("Id");
        SESSION_TABLE_HEADER.add("User");
        SESSION_TABLE_HEADER.add("Host");
        SESSION_TABLE_HEADER.add("LoginTime");
        SESSION_TABLE_HEADER.add("Catalog");
        SESSION_TABLE_HEADER.add("Db");
        SESSION_TABLE_HEADER.add("Command");
        SESSION_TABLE_HEADER.add("Time");
        SESSION_TABLE_HEADER.add("State");
        SESSION_TABLE_HEADER.add("QueryId");
        SESSION_TABLE_HEADER.add("Info");
        SESSION_TABLE_HEADER.add("FE");
        SESSION_TABLE_HEADER.add("CloudCluster");
    }

    @RequestMapping(path = "/session/all", method = RequestMethod.GET)
    public Object allSession(HttpServletRequest request) {
        Map<String, Object> result = Maps.newHashMap();
        result.put("column_names", SESSION_TABLE_HEADER);
        List<Map<String, String>> sessionInfo = Env.getCurrentEnv().getFrontends(null)
                .stream()
                .filter(Frontend::isAlive)
                .map(frontend -> {
                    try {
                        return Env.getCurrentEnv().getSelfNode().getHost().equals(frontend.getHost())
                            ? getSessionInfo()
                            : getOtherSessionInfo(request, frontend);
                    } catch (IOException e) {
                        LOG.warn("", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        result.put("rows", sessionInfo);
        ResponseEntity entity = ResponseEntityBuilder.ok(result);
        ((ResponseBody) entity.getBody()).setCount(sessionInfo.size());
        return entity;
    }

    @RequestMapping(path = "/session", method = RequestMethod.GET)
    public Object session() {
        Map<String, Object> result = Maps.newHashMap();
        result.put("column_names", SESSION_TABLE_HEADER);
        result.put("rows", getSessionInfo());
        ResponseEntity entity = ResponseEntityBuilder.ok(result);
        ((ResponseBody) entity.getBody()).setCount(result.size());
        return entity;
    }

    private List<Map<String, String>> getSessionInfo() {
        List<ConnectContext.ThreadInfo> threadInfos = ExecuteEnv.getInstance().getScheduler()
                .listConnection("root", false);
        long nowMs = System.currentTimeMillis();
        return threadInfos.stream()
                .map(info -> info.toRow(-1, nowMs))
                .map(row -> {
                    Map<String, String> record = new HashMap<>();
                    for (int i = 0; i < row.size(); i++) {
                        record.put(SESSION_TABLE_HEADER.get(i), row.get(i));
                    }
                    return record;
                })
                .collect(Collectors.toList());
    }

    private List<Map<String, String>> getOtherSessionInfo(HttpServletRequest request,
                                                          Frontend frontend) throws IOException {
        Map<String, String> header = Maps.newHashMap();
        header.put(NodeAction.AUTHORIZATION, request.getHeader(NodeAction.AUTHORIZATION));
        String res = HttpUtils.doGet(String.format("http://%s:%s/rest/v1/session",
                frontend.getHost(), Env.getCurrentEnv().getMasterHttpPort()), header);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap = objectMapper.readValue(res,
            new TypeReference<Map<String, Object>>() {});
        List<Map<String, String>> maps = (List<Map<String, String>>)
                ((Map<String, Object>) jsonMap.get("data")).get("rows");
        return maps;
    }
}
