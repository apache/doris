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

package org.apache.doris.sdk.load.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for Doris stream load operations.
 * Use {@link Builder} to construct instances.
 *
 * <pre>
 * DorisConfig config = DorisConfig.builder()
 *     .endpoints(Arrays.asList("http://fe1:8030", "http://fe2:8030"))
 *     .user("root").password("secret")
 *     .database("mydb").table("mytable")
 *     .format(DorisConfig.defaultJsonFormat())
 *     .retry(DorisConfig.defaultRetry())
 *     .groupCommit(GroupCommitMode.ASYNC)
 *     .build();
 * </pre>
 */
public class DorisConfig {

    private final List<String> endpoints;
    private final String user;
    private final String password;
    private final String database;
    private final String table;
    private final String labelPrefix;
    private final String label;
    private final Format format;
    private final RetryConfig retry;
    private final GroupCommitMode groupCommit;
    private final boolean enableGzip;
    private final Map<String, String> options;

    private DorisConfig(Builder builder) {
        this.endpoints = Collections.unmodifiableList(builder.endpoints);
        this.user = builder.user;
        this.password = builder.password;
        this.database = builder.database;
        this.table = builder.table;
        this.labelPrefix = builder.labelPrefix;
        this.label = builder.label;
        this.format = builder.format;
        this.retry = builder.retry;
        this.groupCommit = builder.groupCommit;
        this.enableGzip = builder.enableGzip;
        this.options = builder.options != null
                ? Collections.unmodifiableMap(new HashMap<>(builder.options))
                : Collections.<String, String>emptyMap();
    }

    // --- Convenience factory methods (mirrors Go SDK) ---

    /** Default JSON format: JSON Lines (one object per line). */
    public static JsonFormat defaultJsonFormat() {
        return new JsonFormat(JsonFormat.Type.OBJECT_LINE);
    }

    /** Default CSV format: comma separator, \n delimiter. */
    public static CsvFormat defaultCsvFormat() {
        return new CsvFormat(",", "\\n");
    }

    /** Default retry config: 6 retries, 1s base interval, 60s total limit. */
    public static RetryConfig defaultRetry() {
        return RetryConfig.defaultRetry();
    }

    public static Builder builder() {
        return new Builder();
    }

    // --- Getters ---

    public List<String> getEndpoints() { return endpoints; }
    public String getUser() { return user; }
    public String getPassword() { return password; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public String getLabelPrefix() { return labelPrefix; }
    public String getLabel() { return label; }
    public Format getFormat() { return format; }
    public RetryConfig getRetry() { return retry; }
    public GroupCommitMode getGroupCommit() { return groupCommit; }
    public boolean isEnableGzip() { return enableGzip; }
    public Map<String, String> getOptions() { return options; }

    // --- Builder ---

    public static class Builder {
        private List<String> endpoints;
        private String user;
        private String password = "";
        private String database;
        private String table;
        private String labelPrefix;
        private String label;
        private Format format;
        private RetryConfig retry = RetryConfig.defaultRetry();
        private GroupCommitMode groupCommit = GroupCommitMode.OFF;
        private boolean enableGzip = false;
        private Map<String, String> options;

        public Builder endpoints(List<String> val) { this.endpoints = val; return this; }
        public Builder user(String val) { this.user = val; return this; }
        public Builder password(String val) { this.password = val; return this; }
        public Builder database(String val) { this.database = val; return this; }
        public Builder table(String val) { this.table = val; return this; }
        public Builder labelPrefix(String val) { this.labelPrefix = val; return this; }
        public Builder label(String val) { this.label = val; return this; }
        public Builder format(Format val) { this.format = val; return this; }
        public Builder retry(RetryConfig val) { this.retry = val; return this; }
        public Builder groupCommit(GroupCommitMode val) { this.groupCommit = val; return this; }
        public Builder enableGzip(boolean val) { this.enableGzip = val; return this; }
        public Builder options(Map<String, String> val) { this.options = val; return this; }

        public DorisConfig build() {
            if (user == null || user.isEmpty()) throw new IllegalArgumentException("user cannot be empty");
            if (database == null || database.isEmpty()) throw new IllegalArgumentException("database cannot be empty");
            if (table == null || table.isEmpty()) throw new IllegalArgumentException("table cannot be empty");
            if (endpoints == null || endpoints.isEmpty()) throw new IllegalArgumentException("endpoints cannot be empty");
            if (format == null) throw new IllegalArgumentException("format cannot be null");
            return new DorisConfig(this);
        }
    }
}
