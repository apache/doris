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

package org.apache.doris.cdcclient.utils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static utility class for executing DDL schema changes on the Doris FE via HTTP. */
public class SchemaChangeManager {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeManager.class);
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String COLUMN_EXISTS_MSG = "Can not add column which already exists";
    private static final String COLUMN_NOT_EXISTS_MSG = "Column does not exists";

    private SchemaChangeManager() {}

    /**
     * Execute a list of DDL statements on FE. Each statement is sent independently.
     *
     * <p>Idempotent errors (ADD COLUMN when column already exists, DROP COLUMN when column does not
     * exist) are logged as warnings and silently skipped, so retries on a different BE after a
     * failed commitOffset do not cause infinite failures.
     *
     * @param feAddr Doris FE address (host:port)
     * @param db target database
     * @param token FE auth token
     * @param sqls DDL statements to execute
     */
    public static void executeDdls(String feAddr, String db, String token, List<String> sqls)
            throws IOException {
        if (sqls == null || sqls.isEmpty()) {
            LOG.warn("No DDL statements to execute");
            return;
        }
        for (String stmt : sqls) {
            stmt = stmt.trim();
            if (stmt.isEmpty()) {
                continue;
            }
            LOG.info("Executing DDL on FE {}: {}", feAddr, stmt);
            execute(feAddr, db, token, stmt);
        }
    }

    /**
     * Execute a single SQL statement via the FE query API.
     *
     * <p>Idempotent errors are swallowed with a warning; all other errors throw {@link
     * IOException}.
     */
    public static void execute(String feAddr, String db, String token, String sql)
            throws IOException {
        HttpPost post = buildHttpPost(feAddr, db, token, sql);
        String responseBody = handleResponse(post);
        LOG.info("Executed DDL {} with response: {}", sql, responseBody);
        parseResponse(sql, responseBody);
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    private static HttpPost buildHttpPost(String feAddr, String db, String token, String sql)
            throws IOException {
        String url = String.format(SCHEMA_CHANGE_API, feAddr, db);
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("stmt", sql);
        String body = OBJECT_MAPPER.writeValueAsString(bodyMap);

        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json;charset=UTF-8");
        post.setHeader("Authorization", HttpUtil.getAuthHeader());
        post.setHeader("token", token);
        post.setEntity(new StringEntity(body, "UTF-8"));
        return post;
    }

    private static String handleResponse(HttpPost request) throws IOException {
        try (CloseableHttpClient client = HttpUtil.getHttpClient();
                CloseableHttpResponse response = client.execute(request)) {
            String responseBody =
                    response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "";
            LOG.debug("HTTP [{}]: {}", request.getURI(), responseBody);
            return responseBody;
        }
    }

    /**
     * Parse the FE response. Idempotent errors are logged as warnings and skipped; all other errors
     * throw.
     *
     * <p>Idempotent conditions (can occur when a previous commitOffset failed and a fresh BE
     * re-detects and re-executes the same DDL):
     *
     * <ul>
     *   <li>ADD COLUMN — "Can not add column which already exists": column was already added.
     *   <li>DROP COLUMN — "Column does not exists": column was already dropped.
     * </ul>
     */
    private static void parseResponse(String sql, String responseBody) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        JsonNode code = root.get("code");
        if (code != null && code.asInt() == 0) {
            return;
        }

        String msg = root.path("msg").asText("");

        if (msg.contains(COLUMN_EXISTS_MSG)) {
            LOG.warn("[DDL-IDEMPOTENT] Skipped ADD COLUMN (column already exists). SQL: {}", sql);
            return;
        }
        if (msg.contains(COLUMN_NOT_EXISTS_MSG)) {
            LOG.warn("[DDL-IDEMPOTENT] Skipped DROP COLUMN (column already absent). SQL: {}", sql);
            return;
        }

        LOG.warn("DDL execution failed. SQL: {}. Response: {}", sql, responseBody);
        throw new IOException("Failed to execute schema change: " + responseBody);
    }
}
