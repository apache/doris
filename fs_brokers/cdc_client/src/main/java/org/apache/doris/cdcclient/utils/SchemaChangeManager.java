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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static utility class for executing DDL schema changes on the Doris FE via HTTP. */
public class SchemaChangeManager {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeManager.class);
    private static final String SCHEMA_CHANGE_API = "http://%s/api/streaming/schema_change";
    private static final String TABLE_SCHEMA_API = "http://%s/api/streaming/schema/%s/%s";
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
     * @param jobId streaming job ID used by FE to resolve the creator identity
     * @param schemaChanges schema changes to execute
     */
    public static void executeChanges(
            String feAddr,
            String db,
            String token,
            String jobId,
            List<SchemaChangeOperation> schemaChanges)
            throws IOException {
        if (schemaChanges == null || schemaChanges.isEmpty()) {
            LOG.info("No DDL statements to execute");
            return;
        }
        for (int i = 0; i < schemaChanges.size(); i++) {
            SchemaChangeOperation operation = schemaChanges.get(i);
            LOG.info("Executing DDL on FE {}: {}", feAddr, operation.getSql());
            try {
                execute(feAddr, db, token, jobId, operation);
            } catch (Exception failure) {
                String message =
                        "Failed to execute schema change. SQL: "
                                + operation.getSql()
                                + ". Reason: "
                                + ExceptionUtils.getRootCauseMessage(failure);
                if (i + 1 < schemaChanges.size()) {
                    List<String> remainingSqls =
                            schemaChanges.subList(i + 1, schemaChanges.size()).stream()
                                    .map(SchemaChangeOperation::getSql)
                                    .toList();
                    message += ". Remaining SQLs: " + remainingSqls;
                }
                IOException error = new IOException(message);
                // FE receives the root message; keep the SQL context and retain the original stack.
                error.addSuppressed(failure);
                throw error;
            }
        }
    }

    /**
     * Execute a single SQL statement via the FE streaming schema change API.
     *
     * <p>Known idempotent errors are swallowed directly. For other failures, the current Doris
     * schema is checked before the failure is propagated.
     */
    public static void execute(
            String feAddr, String db, String token, String jobId, SchemaChangeOperation operation)
            throws IOException {
        HttpPost post = buildHttpPost(feAddr, token, jobId, operation.getSql());
        try {
            String responseBody = handleResponse(post);
            LOG.info("Executed DDL {} with response: {}", operation.getSql(), responseBody);
            parseResponse(operation, responseBody);
        } catch (Exception ddlFailure) {
            try {
                if (isAlreadyApplied(feAddr, db, token, jobId, operation)) {
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

    /**
     * Check whether the target can accept an unsupported source schema change. Extra target columns
     * are allowed for historical replay. Types and keys are deliberately not compared, since a
     * manually maintained target may use different types and keys.
     */
    public static void validateTargetSchemas(
            String feAddr,
            String db,
            String token,
            String jobId,
            Map<TableId, TableChanges.TableChange> updatedSchemas,
            Map<String, String> sourceConfig)
            throws IOException {
        Map<String, String> targetTableMappings =
                ConfigUtil.parseAllTargetTableMappings(sourceConfig);
        Map<String, Set<String>> excludedColumns = ConfigUtil.parseAllExcludeColumns(sourceConfig);
        List<String> differences = new ArrayList<>();
        for (Map.Entry<TableId, TableChanges.TableChange> entry : updatedSchemas.entrySet()) {
            TableId tableId = entry.getKey();
            Table sourceTable = entry.getValue().getTable();
            String targetTable = targetTableMappings.getOrDefault(tableId.table(), tableId.table());
            Set<String> targetColumns =
                    fetchTargetColumnNames(feAddr, db, token, jobId, targetTable);
            Set<String> excluded =
                    excludedColumns.getOrDefault(tableId.table(), Collections.emptySet());
            List<String> missingColumns = new ArrayList<>();
            for (Column column : sourceTable.columns()) {
                if (!excluded.contains(column.name()) && !targetColumns.contains(column.name())) {
                    missingColumns.add(column.name());
                }
            }
            if (!missingColumns.isEmpty()) {
                differences.add(db + "." + targetTable + ": missing columns " + missingColumns);
            }
        }
        if (!differences.isEmpty()) {
            throw new IOException(String.join("; ", differences));
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    private static HttpPost buildHttpPost(String feAddr, String token, String jobId, String sql)
            throws IOException {
        String url = String.format(SCHEMA_CHANGE_API, feAddr);
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("stmt", sql);
        String body = OBJECT_MAPPER.writeValueAsString(bodyMap);

        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json;charset=UTF-8");
        post.setHeader("token", token);
        post.setHeader("jobId", jobId);
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
            String feAddr, String db, String token, String jobId, SchemaChangeOperation operation)
            throws IOException {
        boolean columnExists =
                fetchTargetColumnNames(feAddr, db, token, jobId, operation.getTableName())
                        .contains(operation.getColumnName());
        return operation.getType() == SchemaChangeOperation.Type.ADD ? columnExists : !columnExists;
    }

    private static Set<String> fetchTargetColumnNames(
            String feAddr, String db, String token, String jobId, String tableName)
            throws IOException {
        String url = String.format(TABLE_SCHEMA_API, feAddr, db, tableName);
        HttpGet request = new HttpGet(url);
        request.setHeader("token", token);
        request.setHeader("jobId", jobId);

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

        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (JsonNode property : properties) {
            columnNames.add(property.path("name").asText());
        }
        return columnNames;
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
        throw new IOException(
                data.isEmpty() ? "Failed to execute schema change: " + responseBody : data);
    }
}
