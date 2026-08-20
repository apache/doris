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
import org.apache.http.client.methods.HttpGet;
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
    private static final String TABLE_SCHEMA_API = "http://%s/api/%s/%s/_schema";
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
     * @param schemaChanges schema changes to execute
     */
    public static void executeChanges(
            String feAddr, String db, String token, List<SchemaChangeOperation> schemaChanges)
            throws IOException {
        if (schemaChanges == null || schemaChanges.isEmpty()) {
            LOG.info("No DDL statements to execute");
            return;
        }
        for (SchemaChangeOperation operation : schemaChanges) {
            LOG.info("Executing DDL on FE {}: {}", feAddr, operation.getSql());
            execute(feAddr, db, token, operation);
        }
    }

    /**
     * Execute a single SQL statement via the FE query API.
     *
     * <p>Known idempotent errors are swallowed directly. For other failures, the current Doris
     * schema is checked before the failure is propagated.
     */
    public static void execute(
            String feAddr, String db, String token, SchemaChangeOperation operation)
            throws IOException {
        HttpPost post = buildHttpPost(feAddr, db, token, operation.getSql());
        try {
            String responseBody = handleResponse(post);
            LOG.info("Executed DDL {} with response: {}", operation.getSql(), responseBody);
            parseResponse(operation, responseBody);
        } catch (Exception ddlFailure) {
            try {
                if (isAlreadyApplied(feAddr, db, token, operation)) {
                    LOG.warn(
                            "[DDL-IDEMPOTENT] Doris schema already reflects {} {}. SQL: {}",
                            operation.getType(),
                            operation.getColumnName(),
                            operation.getSql());
                    return;
                }
            } catch (IOException schemaFailure) {
                ddlFailure.addSuppressed(schemaFailure);
            }
            throw ddlFailure;
        }
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

    private static boolean isAlreadyApplied(
            String feAddr, String db, String token, SchemaChangeOperation operation)
            throws IOException {
        String url = String.format(TABLE_SCHEMA_API, feAddr, db, operation.getTableName());
        HttpGet request = new HttpGet(url);
        request.setHeader("Authorization", HttpUtil.getAuthHeader());
        request.setHeader("token", token);

        String responseBody;
        try (CloseableHttpClient client = HttpUtil.getHttpClient();
                CloseableHttpResponse response = client.execute(request)) {
            responseBody =
                    response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "";
        }

        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        JsonNode data = root.path("data");
        JsonNode properties = data.path("properties");
        if (root.path("code").asInt(-1) != 0
                || data.path("status").asInt(-1) != 200
                || !properties.isArray()) {
            throw new IOException("Failed to query Doris table schema: " + responseBody);
        }

        boolean columnExists = false;
        for (JsonNode property : properties) {
            if (operation.getColumnName().equalsIgnoreCase(property.path("name").asText())) {
                columnExists = true;
                break;
            }
        }
        return operation.getType() == SchemaChangeOperation.Type.ADD ? columnExists : !columnExists;
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
    private static void parseResponse(SchemaChangeOperation operation, String responseBody)
            throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        JsonNode code = root.get("code");
        if (code != null && code.asInt() == 0) {
            return;
        }

        String msg = root.path("msg").asText("");
        String data = root.path("data").asText("");

        if (operation.getType() == SchemaChangeOperation.Type.ADD
                && (msg.contains(COLUMN_EXISTS_MSG) || data.contains(COLUMN_EXISTS_MSG))) {
            LOG.warn(
                    "[DDL-IDEMPOTENT] Skipped ADD COLUMN (column already exists). SQL: {}",
                    operation.getSql());
            return;
        }
        if (operation.getType() == SchemaChangeOperation.Type.DROP
                && (msg.contains(COLUMN_NOT_EXISTS_MSG) || data.contains(COLUMN_NOT_EXISTS_MSG))) {
            LOG.warn(
                    "[DDL-IDEMPOTENT] Skipped DROP COLUMN (column already absent). SQL: {}",
                    operation.getSql());
            return;
        }

        LOG.warn("DDL execution failed. SQL: {}. Response: {}", operation.getSql(), responseBody);
        throw new IOException("Failed to execute schema change: " + responseBody);
    }
}
