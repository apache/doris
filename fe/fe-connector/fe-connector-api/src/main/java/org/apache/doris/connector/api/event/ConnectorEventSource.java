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

package org.apache.doris.connector.api.event;

/**
 * A connector's incremental-metadata-change source: it fetches the underlying metastore's native
 * notification events and parses them into connector-neutral {@link MetastoreChangeDescriptor}s.
 *
 * <p>Surfaced via {@link org.apache.doris.connector.api.Connector#getEventSource()} (a
 * capability-probe getter, NOT an {@code instanceof} — a connector without an event source returns
 * {@code null}). The engine runs one connector-agnostic, role-aware background driver that iterates
 * catalogs, calls {@link #pollOnce} on those whose connector exposes a source, and applies the
 * returned descriptors to its own object graph and caches.</p>
 *
 * <p><b>Engine/connector split (Trino-aligned: engine owns HA/replication, plugin owns fetch/parse).</b>
 * The connector owns ONLY the metastore RPC and the message deserialization/merge behind {@link
 * #getCurrentEventId()} and {@link #pollOnce}. The engine owns everything stateful and replicated: the
 * per-catalog cursor (passed in via {@link EventPollRequest}, stored from {@link EventPollResult}),
 * the master/follower role, the edit-log write of the synced cursor, and the follower&#8594;master
 * {@code REFRESH CATALOG} forward. The connector is stateless with respect to the cursor.</p>
 *
 * <p><b>Classloader.</b> The engine calls these methods under a context-classloader pin to this
 * source's own plugin classloader, so the notification RPC and the JSON/GZIP deserialization inside
 * resolve the plugin's bundled metastore classes. Implementations therefore need no pin of their own
 * for the fetch/parse path.</p>
 */
public interface ConnectorEventSource {

    /**
     * The metastore's current (latest) notification event id. Used by the master to cheaply decide
     * whether there is anything new to pull before calling {@link #pollOnce}. Returns {@code -1} if the
     * id cannot be read this cycle.
     */
    long getCurrentEventId();

    /**
     * Fetch and parse one batch of notification events for a catalog, returning connector-neutral
     * descriptors plus the new cursor and an optional full-refresh signal. Never returns source-native
     * types. See {@link EventPollRequest} / {@link EventPollResult} for the role/cursor/refresh
     * semantics. The batch size is the connector's own concern (read from its catalog properties).
     */
    EventPollResult pollOnce(EventPollRequest request);
}
