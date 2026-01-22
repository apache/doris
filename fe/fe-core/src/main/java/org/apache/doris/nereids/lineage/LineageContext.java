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

    private Class<? extends Command> sourceCommand;
    private String queryId;
    private String queryText;
    private String user;
    private String database;
    private String catalog;
    private long timestampMs;
    private long durationMs;
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
        this.timestampMs = timestampMs;
        this.durationMs = durationMs;
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
        this.timestampMs = timestampMs;
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
        this.durationMs = durationMs;
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
                + ", timestampMs=" + timestampMs
                + ", durationMs=" + durationMs
                + ", externalCatalogProperties=" + externalCatalogProperties
                + '}';
    }
}
