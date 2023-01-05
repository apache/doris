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

import org.apache.doris.datasource.HMSExternalCatalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Factory class to create various MetastoreEvents.
 */
public class MetastoreEventFactory implements EventFactory {
    private static final Logger LOG = LogManager.getLogger(MetastoreEventFactory.class);

    @Override
    public List<MetastoreEvent> transferNotificationEventToMetastoreEvents(NotificationEvent event,
            String catalogName) {
        Preconditions.checkNotNull(event.getEventType());
        MetastoreEventType metastoreEventType = MetastoreEventType.from(event.getEventType());
        switch (metastoreEventType) {
            case DROP_TABLE:
                return DropTableEvent.getEvents(event, catalogName);
            default:
                // ignore all the unknown events by creating a IgnoredEvent
                return Lists.newArrayList(new IgnoredEvent(event, catalogName));
        }
    }

    List<MetastoreEvent> getMetastoreEvents(List<NotificationEvent> events, HMSExternalCatalog hmsExternalCatalog) {
        List<MetastoreEvent> metastoreEvents = Lists.newArrayList();

        for (NotificationEvent event : events) {
            metastoreEvents.addAll(transferNotificationEventToMetastoreEvents(event, hmsExternalCatalog.getName()));
        }

        List<MetastoreEvent> tobeProcessEvents = metastoreEvents.stream()
                .filter(MetastoreEvent::isSupported)
                .collect(Collectors.toList());

        if (tobeProcessEvents.isEmpty()) {
            LOG.info("The metastore events to process is empty on catalog {}", hmsExternalCatalog.getName());
            return Collections.emptyList();
        }

        return createBatchEvents(tobeProcessEvents);
    }

    /**
     * Create batch event tasks according to HivePartitionName to facilitate subsequent parallel processing.
     * For ADD_PARTITION and DROP_PARTITION, we directly override any events before that partition.
     * For a partition, it is meaningless to process any events before the drop partition.
     */
    List<MetastoreEvent> createBatchEvents(List<MetastoreEvent> events) {
        // now do nothing
        return events;
    }
}
