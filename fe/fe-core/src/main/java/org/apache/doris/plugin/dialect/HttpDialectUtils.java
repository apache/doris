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

package org.apache.doris.plugin.dialect;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * This class is used to convert sql with different dialects using sql convertor service.
 * The sql convertor service is a http service which is used to convert sql.
 */
public class HttpDialectUtils {
    private static final Logger LOG = LogManager.getLogger(HttpDialectUtils.class);

    private static final ObjectReader CONFIRMED_RESPONSE = new ObjectMapper().readerFor(JsonNode.class)
            .with(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                    DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    public static String convertSql(String targetURL, String originStmt, String dialect,
            String[] features, String config) {
        return convertSql(targetURL, originStmt, dialect, features, config, originStmt);
    }

    @Nullable
    static String convertSql(String targetURL, String originStmt, String dialect,
            String[] features, String config, @Nullable String onFailure) {
        ConvertRequest convertRequest = new ConvertRequest(originStmt, dialect, features, config);

        HttpURLConnection connection = null;
        try {
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setUseCaches(false);
            connection.setDoOutput(true);

            String requestStr = convertRequest.toJson();
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(requestStr.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = connection.getResponseCode();
            if (LOG.isDebugEnabled()) {
                LOG.debug("POST Response Code: {}, post data: {}", responseCode, requestStr);
            }

            if (responseCode == HttpURLConnection.HTTP_OK) {
                if (onFailure == null) {
                    // Parse the response bytes directly. Reading it line by line can change a SQL
                    // literal and turn an invalid response into an apparently successful conversion.
                    try (InputStream response = connection.getInputStream()) {
                        JsonNode result = CONFIRMED_RESPONSE.readTree(response);
                        JsonNode code = result.path("code");
                        if ("v1".equals(result.path("version").textValue()) && code.isNumber()
                                && code.decimalValue().signum() == 0) {
                            return result.path("data").textValue();
                        }
                        return null;
                    }
                }
                try (InputStreamReader inputStreamReader
                        = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8);
                        BufferedReader in = new BufferedReader(inputStreamReader)) {
                    String inputLine;
                    StringBuilder response = new StringBuilder();

                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }

                    Type type = new TypeToken<ConvertResponse>() {
                    }.getType();
                    ConvertResponse result = new Gson().fromJson(response.toString(), type);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("convert response: {}", result);
                    }
                    if (result.code == 0) {
                        if (!"v1".equals(result.version)) {
                            LOG.warn("failed to convert sql, response version is not v1: {}", result.version);
                            return onFailure;
                        }
                        return result.data;
                    } else {
                        LOG.warn("failed to convert sql, response: {}", result);
                        return onFailure;
                    }
                }
            } else {
                LOG.warn("failed to convert sql, response code: {}", responseCode);
                return onFailure;
            }
        } catch (Exception e) {
            LOG.warn("failed to convert sql", e);
            return onFailure;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    @Data
    private static class ConvertRequest {
        private String version;    // CHECKSTYLE IGNORE THIS LINE
        private String sql_query;   // CHECKSTYLE IGNORE THIS LINE
        private String from;    // CHECKSTYLE IGNORE THIS LINE
        private String to;   // CHECKSTYLE IGNORE THIS LINE
        private String source;  // CHECKSTYLE IGNORE THIS LINE
        private String case_sensitive;  // CHECKSTYLE IGNORE THIS LINE
        private String[] enable_sql_convertor_features; // CHECKSTYLE IGNORE THIS LINE
        private String config; // CHECKSTYLE IGNORE THIS LINE

        public ConvertRequest(String originStmt, String dialect, String[] features, String config) {
            this.version = "v1";
            this.sql_query = originStmt;
            this.from = dialect;
            this.to = "doris";
            this.source = "text";
            this.case_sensitive = "0";
            this.enable_sql_convertor_features = features;
            this.config = config;
        }

        public String toJson() {
            return new Gson().toJson(this);
        }
    }

    @Data
    private static class ConvertResponse {
        private String version;    // CHECKSTYLE IGNORE THIS LINE
        private String data;    // CHECKSTYLE IGNORE THIS LINE
        private int code;   // CHECKSTYLE IGNORE THIS LINE
        private String message; // CHECKSTYLE IGNORE THIS LINE

        public String toJson() {
            return new Gson().toJson(this);
        }

        @Override
        public String toString() {
            return toJson();
        }
    }
}
