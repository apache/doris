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
    private final List<String> externalTables;

    public MetastoreEventFactory(List<String> externalTables) {
        this.externalTables = externalTables;
    }

    public boolean needToProcess(String catalogTableName) {
        return externalTables.contains(catalogTableName);
    }

    /**
     * For an {@link AddPartitionEvent} and {@link DropPartitionEvent} drop event,
     * we need to divide it into multiple events according to the number of partitions it processes.
     * It is convenient for creating batch tasks to parallel processing.
     */
    @Override
    public List<MetastoreEvent> get(NotificationEvent event,
            String catalogName) {
        Preconditions.checkNotNull(event.getEventType());
        MetastoreEventType metastoreEventType = MetastoreEventType.from(event.getEventType());
        switch (metastoreEventType) {
            case CREATE_TABLE:
                return CreateTableEvent.getEvents(event, catalogName);
            case ALTER_TABLE:
                return AlterTableEvent.getEvents(event, catalogName);
            case DROP_TABLE:
                return DropTableEvent.getEvents(event, catalogName);
            case ALTER_PARTITION:
                return AlterPartitionEvent.getEvents(event, catalogName);
            case DROP_PARTITION:
                return DropPartitionEvent.getEvents(event, catalogName);
            case INSERT:
                return InsertEvent.getEvents(event, catalogName);
            default:
                // ignore all the unknown events by creating a IgnoredEvent
                return Lists.newArrayList(new IgnoredEvent(event, catalogName));
        }
    }

    List<MetastoreEvent> getFilteredEvents(List<NotificationEvent> events,
            String catalogName) {
        List<MetastoreEvent> metastoreEvents = Lists.newArrayList();

        // Currently, the hive external table needs to be manually created in StarRocks to map with the hms table.
        // Therefore, it's necessary to filter the events pulled this time from the hms instance,
        // and the events of the tables that don't register in the fe MetastoreEventsProcessor need to be filtered out.
        for (NotificationEvent event : events) {
            //            String dbName = event.getDbName();
            //            String tableName = event.getTableName();


            metastoreEvents.addAll(get(event, catalogName));
        }

        List<MetastoreEvent> tobeProcessEvents = metastoreEvents.stream()
                .filter(MetastoreEvent::isSupported)
                .collect(Collectors.toList());

        if (tobeProcessEvents.isEmpty()) {
            LOG.warn("The metastore events to process is empty on catalog {}", catalogName);
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
        return events;
    }
}
