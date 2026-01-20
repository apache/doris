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

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;

/**
 * Lineage event wrapping the plan and metadata used by lineage plugins.
 */
public class LineageEvent {

    private Plan plan;
    private Class<? extends Command> sourceCommand;
    private LineageInfo lineageInfo;
    private String queryId;
    private String queryText;
    private String user;
    private String database;
    private long timestampMs;
    private long durationMs;

    /**
     * Create an empty lineage event.
     */
    public LineageEvent() {
    }

    /**
     * Create a lineage event with plan and metadata.
     *
     * @param plan the plan to extract lineage from
     * @param sourceCommand the command type that produced the plan
     * @param queryId query id for this event
     * @param queryText original query text
     * @param user user executing the query
     * @param database database name for the query
     * @param timestampMs event timestamp in milliseconds
     * @param durationMs query duration in milliseconds
     */
    public LineageEvent(Plan plan, Class<? extends Command> sourceCommand, String queryId, String queryText,
            String user, String database, long timestampMs, long durationMs) {
        this.plan = plan;
        this.sourceCommand = sourceCommand;
        this.queryId = queryId;
        this.queryText = queryText;
        this.user = user;
        this.database = database;
        this.timestampMs = timestampMs;
        this.durationMs = durationMs;
    }

    /**
     * Get the plan for this event.
     *
     * @return plan
     */
    public Plan getPlan() {
        return plan;
    }

    /**
     * Get the command type that produced this event.
     *
     * @return command class
     */
    public Class<? extends Command> getSourceCommand() {
        return sourceCommand;
    }

    /**
     * Get the lineage info carried by this event.
     *
     * @return lineage info
     */
    public LineageInfo getLineageInfo() {
        return lineageInfo;
    }

    /**
     * Set the lineage info for this event.
     *
     * @param lineageInfo lineage info
     */
    public void setLineageInfo(LineageInfo lineageInfo) {
        this.lineageInfo = lineageInfo;
    }

    /**
     * Get the query id.
     *
     * @return query id
     */
    public String getQueryId() {
        return queryId;
    }

    /**
     * Get the original query text.
     *
     * @return query text
     */
    public String getQueryText() {
        return queryText;
    }

    /**
     * Get the executing user.
     *
     * @return user name
     */
    public String getUser() {
        return user;
    }

    /**
     * Get the database name.
     *
     * @return database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Get the event timestamp in milliseconds.
     *
     * @return timestamp
     */
    public long getTimestampMs() {
        return timestampMs;
    }

    /**
     * Get the query duration in milliseconds.
     *
     * @return duration
     */
    public long getDurationMs() {
        return durationMs;
    }
}
