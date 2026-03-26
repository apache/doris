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

package org.apache.doris.datasource.deltalake.unity;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * REST client for Databricks Unity Catalog API.
 *
 * <p>Provides methods to interact with Unity Catalog's metadata discovery APIs
 * (catalogs, schemas, tables) and the temporary credentials API for
 * secure data access.
 *
 * <p>Authentication is done via Databricks Personal Access Token (PAT).
 *
 * <p>All list APIs handle pagination automatically by following {@code next_page_token}.
 */
public class UnityCatalogRestClient implements Closeable {
    private static final Logger LOG = LogManager.getLogger(UnityCatalogRestClient.class);

    private static final String API_PREFIX = "/api/2.1/unity-catalog";
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final int DEFAULT_MAX_RESULTS = 100;

    private final String baseUrl;
    private final String authToken;
    private final OkHttpClient httpClient;
    private final Gson gson;

    /**
     * Create a Unity Catalog REST client.
     *
     * @param host             Databricks workspace URL (e.g., "https://dbc-xxx.cloud.databricks.com")
     * @param token            Databricks Personal Access Token
     * @param connectTimeoutMs HTTP connection timeout in milliseconds
     * @param readTimeoutMs    HTTP read timeout in milliseconds
     */
    public UnityCatalogRestClient(String host, String token, long connectTimeoutMs, long readTimeoutMs) {
        // Normalize host URL: remove trailing slash
        this.baseUrl = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
        this.authToken = token;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
                .build();
        this.gson = new Gson();
    }

    // ========== Schema (Database) APIs ==========

    /**
     * List all schemas in a Unity Catalog.
     *
     * @param catalogName the Unity Catalog name
     * @return list of schema names
     */
    public List<String> listSchemas(String catalogName) {
        List<String> schemaNames = new ArrayList<>();
        String pageToken = null;

        do {
            Map<String, String> params = new HashMap<>();
            params.put("catalog_name", catalogName);
            params.put("max_results", String.valueOf(DEFAULT_MAX_RESULTS));
            if (pageToken != null) {
                params.put("page_token", pageToken);
            }

            JsonObject response = executeGet("/schemas", params);
            JsonArray schemas = response.getAsJsonArray("schemas");
            if (schemas != null) {
                for (JsonElement elem : schemas) {
                    JsonObject schema = elem.getAsJsonObject();
                    String name = schema.get("name").getAsString();
                    schemaNames.add(name);
                }
            }

            pageToken = getNextPageToken(response);
        } while (pageToken != null);

        return schemaNames;
    }

    /**
     * Check if a schema exists.
     *
     * @param catalogName the Unity Catalog name
     * @param schemaName  the schema name
     * @return true if the schema exists
     */
    public boolean schemaExists(String catalogName, String schemaName) {
        try {
            String fullName = catalogName + "." + schemaName;
            executeGet("/schemas/" + urlEncode(fullName), Collections.emptyMap());
            return true;
        } catch (UnityCatalogException e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    // ========== Table APIs ==========

    /**
     * List all table names in a schema, optionally filtering by data source format.
     *
     * @param catalogName      the Unity Catalog name
     * @param schemaName       the schema name
     * @param dataSourceFormat filter by this format (e.g., "DELTA"), or null for all
     * @return list of table names
     */
    public List<String> listTableNames(String catalogName, String schemaName, String dataSourceFormat) {
        List<String> tableNames = new ArrayList<>();
        String pageToken = null;

        do {
            Map<String, String> params = new HashMap<>();
            params.put("catalog_name", catalogName);
            params.put("schema_name", schemaName);
            params.put("max_results", String.valueOf(DEFAULT_MAX_RESULTS));
            if (pageToken != null) {
                params.put("page_token", pageToken);
            }

            JsonObject response = executeGet("/tables", params);
            JsonArray tables = response.getAsJsonArray("tables");
            if (tables != null) {
                for (JsonElement elem : tables) {
                    JsonObject table = elem.getAsJsonObject();
                    // Filter by data source format if specified
                    if (dataSourceFormat != null) {
                        JsonElement formatElem = table.get("data_source_format");
                        if (formatElem == null
                                || !dataSourceFormat.equalsIgnoreCase(formatElem.getAsString())) {
                            continue;
                        }
                    }
                    tableNames.add(table.get("name").getAsString());
                }
            }

            pageToken = getNextPageToken(response);
        } while (pageToken != null);

        return tableNames;
    }

    /**
     * Get detailed table information.
     *
     * @param fullName the fully qualified table name (catalog.schema.table)
     * @return table info
     */
    public TableInfo getTable(String fullName) {
        Map<String, String> params = new HashMap<>();
        params.put("include_delta_metadata", "true");
        JsonObject response = executeGet("/tables/" + urlEncode(fullName), params);
        return gson.fromJson(response, TableInfo.class);
    }

    /**
     * Check if a table exists.
     *
     * @param fullName the fully qualified table name (catalog.schema.table)
     * @return true if the table exists
     */
    public boolean tableExists(String fullName) {
        try {
            executeGet("/tables/" + urlEncode(fullName), Collections.emptyMap());
            return true;
        } catch (UnityCatalogException e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    // ========== Temporary Credentials API ==========

    /**
     * Generate temporary table credentials for direct data file access.
     *
     * <p>Requires:
     * <ul>
     *   <li>Metastore-level {@code external_access_enabled = true}</li>
     *   <li>The caller has {@code EXTERNAL_USE_SCHEMA} privilege</li>
     * </ul>
     *
     * @param tableId   the table UUID (from TableInfo.tableId)
     * @param operation "READ" or "READ_WRITE"
     * @return temporary credentials with cloud-provider-specific tokens
     */
    public TemporaryCredentials generateTempTableCredentials(String tableId, String operation) {
        JsonObject body = new JsonObject();
        body.addProperty("table_id", tableId);
        body.addProperty("operation", operation);
        JsonObject response = executePost("/temporary-table-credentials", body);
        return gson.fromJson(response, TemporaryCredentials.class);
    }

    // ========== HTTP execution layer ==========

    private JsonObject executeGet(String path, Map<String, String> params) {
        String url = baseUrl + API_PREFIX + path;
        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
        }

        Request request = new Request.Builder()
                .url(urlBuilder.build())
                .addHeader("Authorization", "Bearer " + authToken)
                .addHeader("Accept", "application/json")
                .get()
                .build();

        return executeRequest(request);
    }

    private JsonObject executePost(String path, JsonObject body) {
        String url = baseUrl + API_PREFIX + path;
        RequestBody requestBody = RequestBody.create(gson.toJson(body), JSON);

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + authToken)
                .addHeader("Accept", "application/json")
                .post(requestBody)
                .build();

        return executeRequest(request);
    }

    private JsonObject executeRequest(Request request) {
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";

            if (!response.isSuccessful()) {
                String errorMsg = String.format(
                        "Unity Catalog API request failed: %s %s -> HTTP %d: %s",
                        request.method(), request.url(), response.code(), responseBody);
                LOG.warn(errorMsg);
                throw new UnityCatalogException(response.code(), errorMsg);
            }

            if (responseBody.isEmpty()) {
                return new JsonObject();
            }
            return gson.fromJson(responseBody, JsonObject.class);
        } catch (UnityCatalogException e) {
            throw e;
        } catch (IOException e) {
            throw new UnityCatalogException(0,
                    "Failed to execute Unity Catalog API request: " + request.url(), e);
        }
    }

    private String getNextPageToken(JsonObject response) {
        JsonElement tokenElem = response.get("next_page_token");
        if (tokenElem != null && !tokenElem.isJsonNull()) {
            String token = tokenElem.getAsString();
            return token.isEmpty() ? null : token;
        }
        return null;
    }

    private static String urlEncode(String value) {
        // Unity Catalog full_name contains dots (e.g., "catalog.schema.table")
        // These need to be URL-encoded for path parameters
        try {
            return java.net.URLEncoder.encode(value, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            // UTF-8 is always supported
            return value;
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    // ========== Data Model Classes ==========

    /**
     * Represents table information from Unity Catalog API.
     */
    public static class TableInfo {
        @SerializedName("name")
        private String name;

        @SerializedName("full_name")
        private String fullName;

        @SerializedName("table_id")
        private String tableId;

        @SerializedName("table_type")
        private String tableType;

        @SerializedName("data_source_format")
        private String dataSourceFormat;

        @SerializedName("storage_location")
        private String storageLocation;

        @SerializedName("storage_credential_name")
        private String storageCredentialName;

        @SerializedName("columns")
        private List<ColumnInfo> columns;

        @SerializedName("properties")
        private Map<String, String> properties;

        @SerializedName("comment")
        private String comment;

        @SerializedName("owner")
        private String owner;

        public String getName() {
            return name;
        }

        public String getFullName() {
            return fullName;
        }

        public String getTableId() {
            return tableId;
        }

        public String getTableType() {
            return tableType;
        }

        public String getDataSourceFormat() {
            return dataSourceFormat;
        }

        public String getStorageLocation() {
            return storageLocation;
        }

        public String getStorageCredentialName() {
            return storageCredentialName;
        }

        public List<ColumnInfo> getColumns() {
            return columns;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public String getComment() {
            return comment;
        }
    }

    /**
     * Represents column information from Unity Catalog API.
     */
    public static class ColumnInfo {
        @SerializedName("name")
        private String name;

        @SerializedName("type_name")
        private String typeName;

        @SerializedName("type_text")
        private String typeText;

        @SerializedName("nullable")
        private boolean nullable;

        @SerializedName("partition_index")
        private Integer partitionIndex;

        @SerializedName("comment")
        private String comment;

        public String getName() {
            return name;
        }

        public String getTypeName() {
            return typeName;
        }

        public String getTypeText() {
            return typeText;
        }

        public boolean isNullable() {
            return nullable;
        }

        public Integer getPartitionIndex() {
            return partitionIndex;
        }

        public String getComment() {
            return comment;
        }
    }

    /**
     * Temporary credentials returned by the Unity Catalog API.
     */
    public static class TemporaryCredentials {
        @SerializedName("aws_temp_credentials")
        private AwsTempCredentials awsTempCredentials;

        @SerializedName("azure_user_delegation_sas")
        private AzureUserDelegationSas azureUserDelegationSas;

        @SerializedName("gcp_oauth_token")
        private GcpOAuthToken gcpOAuthToken;

        @SerializedName("expiration_time")
        private long expirationTime;

        @SerializedName("url")
        private String url;

        public AwsTempCredentials getAwsTempCredentials() {
            return awsTempCredentials;
        }

        public AzureUserDelegationSas getAzureUserDelegationSas() {
            return azureUserDelegationSas;
        }

        public GcpOAuthToken getGcpOAuthToken() {
            return gcpOAuthToken;
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        public String getUrl() {
            return url;
        }
    }

    /**
     * AWS temporary credentials (STS session token).
     */
    public static class AwsTempCredentials {
        @SerializedName("access_key_id")
        private String accessKeyId;

        @SerializedName("secret_access_key")
        private String secretAccessKey;

        @SerializedName("session_token")
        private String sessionToken;

        public String getAccessKeyId() {
            return accessKeyId;
        }

        public String getSecretAccessKey() {
            return secretAccessKey;
        }

        public String getSessionToken() {
            return sessionToken;
        }
    }

    /**
     * Azure user delegation SAS token.
     */
    public static class AzureUserDelegationSas {
        @SerializedName("sas_token")
        private String sasToken;

        public String getSasToken() {
            return sasToken;
        }
    }

    /**
     * GCP OAuth token.
     */
    public static class GcpOAuthToken {
        @SerializedName("oauth_token")
        private String oauthToken;

        public String getOauthToken() {
            return oauthToken;
        }
    }

    /**
     * Exception thrown when a Unity Catalog API request fails.
     */
    public static class UnityCatalogException extends RuntimeException {
        private final int statusCode;

        public UnityCatalogException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        public UnityCatalogException(int statusCode, String message, Throwable cause) {
            super(message, cause);
            this.statusCode = statusCode;
        }

        public int getStatusCode() {
            return statusCode;
        }
    }
}
