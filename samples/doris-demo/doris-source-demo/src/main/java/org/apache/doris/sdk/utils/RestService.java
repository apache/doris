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

package org.apache.doris.sdk.utils;

import org.apache.doris.sdk.model.QueryPlan;
import org.apache.doris.sdk.model.Tablet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class RestService {

    private final String QUERY_PLAN = "_query_plan";
    private final String API_PREFIX = "/api";
    private final String sql;
    private final String table;
    private final String database;
    private final String username;
    private final String password;
    private final String feNode;
    private String queryPlan;
    private List<Long> tablets;
    private String beInfo;

    public RestService(Map<String, String> params) throws IOException {
        this.database = params.get("database");
        this.sql = params.get("sql");
        this.table = params.get("table");
        this.username = params.get("username");
        this.password = params.get("password");
        this.feNode = params.get("fe_host") + ":" + params.get("fe_http_port");
        buildQuery();
    }

    public String getQueryPlan() {
        return queryPlan;
    }

    private QueryPlan parseQueryPlan(String response) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(response, QueryPlan.class);
    }

    private String getConnectionPost(HttpRequestBase request, String user, String passwd) throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        String authEncoding = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        InputStream content = ((HttpPost) request).getEntity().getContent();

        String res = IOUtils.toString(content, Charset.defaultCharset());
        conn.setDoOutput(true);
        conn.setDoInput(true);
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        out.print(res);
        out.flush();

        return parseResponse(conn);
    }

    private String parseResponse(HttpURLConnection connection) throws IOException {
        if (connection.getResponseCode() != HttpStatus.SC_OK) {
            throw new IOException("Failed to get response from Doris");
        }
        StringBuilder result = new StringBuilder();
        try (Scanner scanner = new Scanner(connection.getInputStream(), "utf-8")) {
            while (scanner.hasNext()) {
                result.append(scanner.next());
            }
            return result.toString();
        }
    }

    private void buildQuery() throws IOException {
        HttpPost httpPost = buildPostEntity();
        String resStr = send(httpPost);
        QueryPlan queryPlan = parseQueryPlan(resStr);
        this.queryPlan = queryPlan.getOpaqued_query_plan();
        this.tablets = queryPlan.getPartitions().keySet().stream().map(Long::new).collect(Collectors.toList());
        Tablet tablet = queryPlan.getPartitions().get(tablets.get(0).toString());
        this.beInfo = tablet.getRoutings().get(0);
    }

    private HttpPost buildPostEntity() {
        HttpPost httpPost = new HttpPost(getUriStr() + QUERY_PLAN);
        String entity = "{\"sql\": \"" + this.sql + "\"}";
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);
        return httpPost;
    }

    public String getBeInfo() {
        return beInfo;
    }

    public List<Long> getTablets() {
        return tablets;
    }

    private String send(HttpRequestBase request) throws IOException {
        String response = getConnectionPost(request, username, password);

        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(response, Map.class);
        if (map.containsKey("code") && map.containsKey("msg")) {
            Object data = map.get("data");
            return mapper.writeValueAsString(data);
        } else {
            return response;
        }
    }

    private String getUriStr() throws IllegalArgumentException {
        return "http://" + feNode + API_PREFIX + "/" + database + "/" + table + "/";
    }

}
