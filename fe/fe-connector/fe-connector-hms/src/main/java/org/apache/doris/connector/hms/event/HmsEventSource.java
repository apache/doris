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

package org.apache.doris.connector.hms.event;

import org.apache.doris.connector.api.event.ConnectorEventSource;
import org.apache.doris.connector.api.event.EventPollRequest;
import org.apache.doris.connector.api.event.EventPollResult;
import org.apache.doris.connector.api.event.MetastoreChangeDescriptor;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsNotificationEvent;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * The hive connector's {@link ConnectorEventSource}: fetches HMS notification events and parses them
 * into neutral descriptors. Stateless with respect to the cursor — the engine passes the cursor in and
 * stores the returned one, so the same source instance serves both master (fetch-directly) and follower
 * (bounded-by-master) roles.
 *
 * <p>Mirrors the legacy fe-core {@code MetastoreEventsProcessor} fetch decisions exactly: the master
 * reads the metastore's current id to decide whether to pull; a follower pulls only up to the master's
 * committed high-water mark; a first sync or a trimmed notification log (the
 * {@code REPL_EVENTS_MISSING_IN_METASTORE} sentinel) yields a full-refresh signal instead of events.</p>
 */
public class HmsEventSource implements ConnectorEventSource {
    private static final Logger LOG = LogManager.getLogger(HmsEventSource.class);

    private final HmsClient client;
    private final int batchSize;

    public HmsEventSource(HmsClient client, int batchSize) {
        this.client = client;
        this.batchSize = batchSize;
    }

    @Override
    public long getCurrentEventId() {
        return client.getCurrentNotificationEventId();
    }

    @Override
    public EventPollResult pollOnce(EventPollRequest request) {
        if (request.isMaster()) {
            return pollForMaster(request.getLastSyncedEventId());
        }
        return pollForFollower(request.getLastSyncedEventId(), request.getMasterUpperBound());
    }

    private EventPollResult pollForMaster(long lastSyncedEventId) {
        long currentEventId = client.getCurrentNotificationEventId();
        if (lastSyncedEventId < 0) {
            // first pull: seed the cursor to now and rebuild via a full refresh (no events to replay)
            return EventPollResult.ofFullRefresh(currentEventId);
        }
        if (currentEventId == lastSyncedEventId) {
            return EventPollResult.ofNothing(lastSyncedEventId);
        }
        try {
            return toResult(client.getNextNotification(lastSyncedEventId, batchSize), lastSyncedEventId);
        } catch (HmsClientException e) {
            if (isEventsMissing(e)) {
                // the metastore trimmed its log past our cursor; jump to now and full-refresh
                return EventPollResult.ofFullRefresh(currentEventId);
            }
            // transient fetch error (metastore blip): retry the same cursor next cycle without resetting or
            // invalidating. A deterministic PARSE error is a RuntimeException, not an HmsClientException, so it
            // propagates to the engine's self-heal instead of being swallowed here.
            LOG.warn("Failed to fetch HMS notifications from event id {}; will retry", lastSyncedEventId, e);
            return EventPollResult.ofNothing(lastSyncedEventId);
        }
    }

    private EventPollResult pollForFollower(long lastSyncedEventId, long masterUpperBound) {
        // -1 => the master's cursor has not been learned yet (via edit-log replay); do nothing
        if (masterUpperBound == -1L || lastSyncedEventId == masterUpperBound) {
            return EventPollResult.ofNothing(lastSyncedEventId);
        }
        if (lastSyncedEventId < 0) {
            // first pull: seed to the master's committed id and full-refresh (the engine forwards
            // REFRESH CATALOG to the master for a follower)
            return EventPollResult.ofFullRefresh(masterUpperBound);
        }
        // never read past what the master has already committed and replicated
        int maxEvents = (int) Math.min(masterUpperBound - lastSyncedEventId, batchSize);
        try {
            return toResult(client.getNextNotification(lastSyncedEventId, maxEvents), lastSyncedEventId);
        } catch (HmsClientException e) {
            if (isEventsMissing(e)) {
                return EventPollResult.ofFullRefresh(masterUpperBound);
            }
            // transient fetch error: retry the same cursor next cycle (see pollForMaster).
            LOG.warn("Failed to fetch HMS notifications from event id {}; will retry", lastSyncedEventId, e);
            return EventPollResult.ofNothing(lastSyncedEventId);
        }
    }

    private EventPollResult toResult(List<HmsNotificationEvent> events, long lastSyncedEventId) {
        if (events.isEmpty()) {
            return EventPollResult.ofNothing(lastSyncedEventId);
        }
        List<MetastoreChangeDescriptor> descriptors = new ArrayList<>();
        for (HmsNotificationEvent event : events) {
            descriptors.addAll(HmsEventParser.parse(event));
        }
        long newCursor = events.get(events.size() - 1).getEventId();
        return EventPollResult.ofChanges(newCursor, descriptors);
    }

    private static boolean isEventsMissing(HmsClientException e) {
        // ThriftHmsClient.execute wraps the vendored client's
        // IllegalStateException(REPL_EVENTS_MISSING_IN_METASTORE) into an HmsClientException; the
        // sentinel survives in the message chain.
        for (Throwable t = e; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message != null
                    && message.contains(HiveMetaStoreClient.REPL_EVENTS_MISSING_IN_METASTORE)) {
                return true;
            }
        }
        return false;
    }
}
