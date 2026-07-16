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
 * The engine-supplied input to one {@link ConnectorEventSource#pollOnce} call. The engine owns the
 * cursor and the role; the connector owns the fetch strategy those imply.
 *
 * <ul>
 *   <li>{@link #getLastSyncedEventId()} — the last event id this FE has already applied for the
 *       catalog (the engine's per-catalog cursor; {@code -1} means "never synced" &#8594; the connector
 *       should signal a full refresh rather than replay from 0).</li>
 *   <li>{@link #isMaster()} — whether this FE is the master. The master fetches the metastore's
 *       current event id and pulls new events directly; a follower pulls only up to
 *       {@link #getMasterUpperBound()} (what the master has already committed and replicated), so it
 *       never reads past the master's high-water mark.</li>
 *   <li>{@link #getMasterUpperBound()} — the master's committed event id, learned by the follower via
 *       edit-log replay; ignored on the master. {@code -1} means "not yet learned" &#8594; a follower
 *       does nothing this cycle.</li>
 * </ul>
 */
public final class EventPollRequest {

    private final long lastSyncedEventId;
    private final boolean isMaster;
    private final long masterUpperBound;

    public EventPollRequest(long lastSyncedEventId, boolean isMaster, long masterUpperBound) {
        this.lastSyncedEventId = lastSyncedEventId;
        this.isMaster = isMaster;
        this.masterUpperBound = masterUpperBound;
    }

    public long getLastSyncedEventId() {
        return lastSyncedEventId;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public long getMasterUpperBound() {
        return masterUpperBound;
    }

    @Override
    public String toString() {
        return "EventPollRequest{lastSyncedEventId=" + lastSyncedEventId + ", isMaster=" + isMaster
                + ", masterUpperBound=" + masterUpperBound + '}';
    }
}
