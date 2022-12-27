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


package org.apache.doris.datasource.hive.event;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This event represents a batch of events of type T. The batch of events is
 * initialized from a single initial event called baseEvent. More events can be added
 * to the batch using {@code addToBatchEvents} method.
 *
 * @param <T> The type of event which is batched by this event.
 */
public class BatchEvent<T extends MetastoreTableEvent> extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(BatchEvent.class);

    private final T baseEvent;
    private final List<T> batchedEvents = Lists.newArrayList();

    protected BatchEvent(T baseEvent) {
        super(baseEvent.event, baseEvent.catalogName);
        this.hmsTbl = baseEvent.hmsTbl;
        this.baseEvent = baseEvent;
        batchedEvents.add(baseEvent);
    }

    /**
     * @param event The event under consideration to be batched into this event. It can
     * be added to the batch if it can be batched into the last event of the current batch.
     * @return true if we can add the event to the current batch; else false.
     */
    @Override
    protected boolean canBeBatched(MetastoreEvent event) {
        Preconditions.checkState(!batchedEvents.isEmpty());
        return batchedEvents.get(batchedEvents.size() - 1).canBeBatched(event);
    }

    @Override
    public MetastoreEvent addToBatchEvents(MetastoreEvent event) {
        Preconditions.checkState(canBeBatched(event));
        batchedEvents.add((T) event);
        return this;
    }

    /**
     * Return the event id of this batch event. We return the last eventId
     * from this batch which is important since it is used to determined the event
     * id for fetching next set of events from metastore.
     */
    @Override
    public long getEventId() {
        Preconditions.checkState(!batchedEvents.isEmpty());
        return batchedEvents.get(batchedEvents.size() - 1).getEventId();
    }

    @Override
    public int getNumberOfEvents() {
        return batchedEvents.size();
    }

    public List<T> getBatchEvents() {
        return batchedEvents;
    }

    /**
     * Gets the event id of the first event in the batch.
     */
    public long getFirstEventId() {
        return batchedEvents.get(0).getEventId();
    }

    /**
     * Gets the event id of the last event in the batch.
     */
    public long getLastEventId() {
        return batchedEvents.get(batchedEvents.size() - 1).getEventId();
    }

    /**
     * Gets the event of the last event in the batch.
     */
    protected MetastoreEvent getLastEvent() {
        return batchedEvents.get(batchedEvents.size() - 1);
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    @Override
    protected boolean existInCache() throws MetastoreNotificationException {
        return baseEvent.existInCache();
    }

    /**
     * The merge rule in batch event is relatively simple, because some events that don't need to be processed
     * have been filtered out during the {@link MetastoreEventFactory#createBatchEvents}. mainly the following two.
     * 1. Multiple ALTER_PARTITION or ALTER_TABLE only process the last one.
     * 2. The first event in the batch event is ADD_PARTITION, and the ALTER events currently only takes the last one.
     */
    protected List<T> mergeBatchEvents(List<T> eventToMerge) {
        List<T> mergedEvents = Lists.newArrayList();
        T first = eventToMerge.get(0);
        T last = eventToMerge.get(eventToMerge.size() - 1);
        if (first.getEventType() == MetastoreEventType.ADD_PARTITION) {
            mergedEvents.add(first);
        }
        mergedEvents.add(last);
        return mergedEvents;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        LOG.info("Start to process batch event for {} events from {} to {}",
                getNumberOfEvents(), getFirstEventId(), getLastEvent());
        if (!baseEvent.existInCache()) {
            return;
        }

        List<T> eventsToProcess = batchedEvents.stream()
                .filter(event -> !event.canBeSkipped())
                .collect(Collectors.toList());

        if (eventsToProcess.isEmpty()) {
            LOG.info("Ignoring events {} since they modify parameters which can be ignored", batchedEvents);
            return;
        }

        if (eventsToProcess.size() > 1) {
            eventsToProcess = mergeBatchEvents(eventsToProcess);
        }

        eventsToProcess.forEach(MetastoreEvent::process);
    }
}
