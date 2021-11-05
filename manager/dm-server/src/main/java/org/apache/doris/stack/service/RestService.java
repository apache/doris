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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.connector.HttpClientPoolManager;
import org.apache.doris.stack.model.palo.PaloResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

/**
 * rest to doris
 **/
@Component
@Slf4j
public class RestService {

    private static final String FRONTENDS_API = "http://%s:%s/rest/v2/manager/node/frontends";

    private static final String ADD_BACKENDS_API = "http://%s:%s/rest/v2/manager/cluster/add_backends";

    private static final int REQUEST_SUCCESS_CODE = 0;

    private static final String AUTH_HEADER_KEY = "Authorization";

    @Autowired
    private HttpClientPoolManager poolManager;

    public static void setAuthHeader(HttpServletRequest request, Map<String, String> headers) {
        headers.put(AUTH_HEADER_KEY, request.getHeader(AUTH_HEADER_KEY));
    }

    public static void setPostHeader(Map<String, String> headers) {
        headers.put("Content-Type", "application/json; charset=utf-8");
    }

    public List<Map<String, String>> frontends(String host, int port, Map<String, String> headers) {
        List<Map<String, String>> result = Lists.newArrayList();
        String url = String.format(FRONTENDS_API, host, port);
        PaloResponseEntity paloResponseEntity = null;
        try {
            paloResponseEntity = poolManager.doGet(url, headers);
        } catch (Exception e) {
            log.error("request frontends list failed:", e);
            return result;
        }
        if (REQUEST_SUCCESS_CODE != paloResponseEntity.getCode()) {
            return result;
        }
        Response response = JSON.parseObject(paloResponseEntity.getData(), Response.class);
        for (List<String> row : response.rows) {
            List<String> columnNames = response.columnNames;
            if (row.size() != columnNames.size()) {
                continue;
            }
            Map<String, String> res = Maps.newHashMap();
            for (int i = 0; i < columnNames.size(); i++) {
                res.put(columnNames.get(i), row.get(i));
            }
            result.add(res);
        }
        return result;
    }

    public Map<String, Boolean> addBackends(String host, int port, Map<String, Integer> hostPorts, Map<String, String> headers) {
        Map<String, Boolean> result = Maps.newHashMap();
        String url = String.format(ADD_BACKENDS_API, host, port);
        PaloResponseEntity paloResponseEntity = null;
        try {
            paloResponseEntity = poolManager.doPost(url, headers, hostPorts);
        } catch (Exception e) {
            log.error("request backends list failed:{}", JSON.toJSONString(paloResponseEntity), e);
            return result;
        }
        if (REQUEST_SUCCESS_CODE != paloResponseEntity.getCode()) {
            return result;
        }
        result = JSON.parseObject(paloResponseEntity.getData(), new TypeReference<Map<String, Boolean>>() {
        });
        return result;
    }

    public static class Response {

        @JsonProperty(value = "column_names")
        public List<String> columnNames;

        public List<List<String>> rows;

        public Response(List<String> columnNames, List<List<String>> rows) {
            this.columnNames = columnNames;
            this.rows = rows;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("columnNames: ").append(columnNames);
            sb.append(", rows: ").append(rows);
            return sb.toString();
        }
    }
}
