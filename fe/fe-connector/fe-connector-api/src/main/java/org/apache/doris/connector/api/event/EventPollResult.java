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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The connector's answer to one {@link ConnectorEventSource#pollOnce} call.
 *
 * <ul>
 *   <li>{@link #getNewCursor()} — the event id the engine should store as its new per-catalog cursor.
 *       This is NOT always {@code lastSyncedEventId + descriptors.size()}: on a first sync or an
 *       events-gap recovery the connector advances the cursor to the metastore's current id WITHOUT
 *       emitting events (paired with {@link #isNeedsFullRefresh()}); on an empty poll it is the
 *       unchanged cursor.</li>
 *   <li>{@link #getDescriptors()} — the already-merged, connector-neutral changes to apply, in order.
 *       Empty when nothing changed or when a full refresh is requested instead.</li>
 *   <li>{@link #isNeedsFullRefresh()} — the connector could not produce an incremental delta (first
 *       sync, or the metastore reported its notification log was trimmed past the cursor). The engine
 *       responds per role: the master invalidates the whole catalog; a follower forwards
 *       {@code REFRESH CATALOG} to the master. The connector owns detecting the source-specific gap
 *       condition; the engine owns the HA response.</li>
 * </ul>
 */
public final class EventPollResult {

    private final long newCursor;
    private final List<MetastoreChangeDescriptor> descriptors;
    private final boolean needsFullRefresh;

    public EventPollResult(long newCursor, List<MetastoreChangeDescriptor> descriptors,
            boolean needsFullRefresh) {
        this.newCursor = newCursor;
        this.descriptors = descriptors == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(descriptors));
        this.needsFullRefresh = needsFullRefresh;
    }

    /** A result carrying incremental changes; the cursor advances to {@code newCursor}. */
    public static EventPollResult ofChanges(long newCursor, List<MetastoreChangeDescriptor> descriptors) {
        return new EventPollResult(newCursor, descriptors, false);
    }

    /** A result requesting a full catalog refresh and seeding the cursor to {@code newCursor}. */
    public static EventPollResult ofFullRefresh(long newCursor) {
        return new EventPollResult(newCursor, Collections.emptyList(), true);
    }

    /** A no-op result: nothing changed, keep the cursor at {@code cursor}. */
    public static EventPollResult ofNothing(long cursor) {
        return new EventPollResult(cursor, Collections.emptyList(), false);
    }

    public long getNewCursor() {
        return newCursor;
    }

    public List<MetastoreChangeDescriptor> getDescriptors() {
        return descriptors;
    }

    public boolean isNeedsFullRefresh() {
        return needsFullRefresh;
    }

    @Override
    public String toString() {
        return "EventPollResult{newCursor=" + newCursor + ", descriptors=" + descriptors.size()
                + ", needsFullRefresh=" + needsFullRefresh + '}';
    }
}
