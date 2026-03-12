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

package org.apache.doris.nereids.lineage;

import org.apache.doris.nereids.trees.plans.commands.Command;

import java.util.Map;

/**
 * Query metadata used by lineage events.
 */
public class LineageContext {

    public static final long UNKNOWN_TIMESTAMP_MS = -1L;
    public static final long UNKNOWN_DURATION_MS = -1L;

    private Class<? extends Command> sourceCommand;
    private String queryId;
    private String queryText;
    private String user;
    private String clientIp;
    private String state;
    // Current session database for the query context; not necessarily the target table database.
    private String database;
    // Current session catalog for the query context; not necessarily the target table catalog.
    private String catalog;
    private long timestampMs = UNKNOWN_TIMESTAMP_MS;
    private long durationMs = UNKNOWN_DURATION_MS;
    // External catalog properties referenced by the query (sanitized).
    private Map<String, Map<String, String>> externalCatalogProperties;

    /**
     * Create an empty lineage context.
     */
    public LineageContext() {
    }

    /**
     * Create a lineage context with query metadata.
     */
    public LineageContext(Class<? extends Command> sourceCommand, String queryId, String queryText, String user,
                          String database, long timestampMs, long durationMs) {
        this.sourceCommand = sourceCommand;
        this.queryId = queryId;
        this.queryText = queryText;
        this.user = user;
        this.database = database;
        this.timestampMs = normalizeTimestamp(timestampMs);
        this.durationMs = normalizeDuration(durationMs);
    }

    /**
     * Get the source command type.
     */
    public Class<? extends Command> getSourceCommand() {
        return sourceCommand;
    }

    /**
     * Set the source command type.
     */
    public void setSourceCommand(Class<? extends Command> sourceCommand) {
        this.sourceCommand = sourceCommand;
    }

    /**
     * Get the query id.
     */
    public String getQueryId() {
        return queryId;
    }

    /**
     * Set the query id.
     */
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    /**
     * Get the original query text.
     */
    public String getQueryText() {
        return queryText;
    }

    /**
     * Set the original query text.
     */
    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    /**
     * Get the executing user.
     */
    public String getUser() {
        return user;
    }

    /**
     * Set the executing user.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Get the client ip.
     */
    public String getClientIp() {
        return clientIp;
    }

    /**
     * Set the client ip.
     */
    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    /**
     * Get the query state.
     */
    public String getState() {
        return state;
    }

    /**
     * Set the query state.
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * Get the database name.
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Set the database name.
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Get the catalog name.
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Set the catalog name.
     */
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    /**
     * Get the event timestamp in milliseconds.
     */
    public long getTimestampMs() {
        return timestampMs;
    }

    /**
     * Set the event timestamp in milliseconds.
     */
    public void setTimestampMs(long timestampMs) {
        this.timestampMs = normalizeTimestamp(timestampMs);
    }

    /**
     * Get the query duration in milliseconds.
     */
    public long getDurationMs() {
        return durationMs;
    }

    /**
     * Set the query duration in milliseconds.
     */
    public void setDurationMs(long durationMs) {
        this.durationMs = normalizeDuration(durationMs);
    }

    private long normalizeTimestamp(long timestampMs) {
        return timestampMs < 0 ? UNKNOWN_TIMESTAMP_MS : timestampMs;
    }

    private long normalizeDuration(long durationMs) {
        return durationMs < 0 ? UNKNOWN_DURATION_MS : durationMs;
    }

    /**
     * Get external catalog properties used by the query.
     */
    public Map<String, Map<String, String>> getExternalCatalogProperties() {
        return externalCatalogProperties;
    }

    /**
     * Set external catalog properties used by the query.
     */
    public void setExternalCatalogProperties(Map<String, Map<String, String>> externalCatalogProperties) {
        this.externalCatalogProperties = externalCatalogProperties;
    }

    @Override
    public String toString() {
        return "LineageContext{"
                + "sourceCommand=" + (sourceCommand == null ? "null" : sourceCommand.getSimpleName())
                + ", queryId='" + queryId + '\''
                + ", database='" + database + '\''
                + ", catalog='" + catalog + '\''
                + ", user='" + user + '\''
                + ", clientIp='" + clientIp + '\''
                + ", state='" + state + '\''
                + ", timestampMs=" + timestampMs
                + ", durationMs=" + durationMs
                + ", externalCatalogProperties=" + externalCatalogProperties
                + '}';
    }
}
