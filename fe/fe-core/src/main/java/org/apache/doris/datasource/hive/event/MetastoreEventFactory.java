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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
            case CREATE_TABLE:
                return CreateTableEvent.getEvents(event, catalogName);
            case DROP_TABLE:
                return DropTableEvent.getEvents(event, catalogName);
            case ALTER_TABLE:
                return AlterTableEvent.getEvents(event, catalogName);
            case CREATE_DATABASE:
                return CreateDatabaseEvent.getEvents(event, catalogName);
            case DROP_DATABASE:
                return DropDatabaseEvent.getEvents(event, catalogName);
            case ALTER_DATABASE:
                return AlterDatabaseEvent.getEvents(event, catalogName);
            case ADD_PARTITION:
                return AddPartitionEvent.getEvents(event, catalogName);
            case DROP_PARTITION:
                return DropPartitionEvent.getEvents(event, catalogName);
            case ALTER_PARTITION:
                return AlterPartitionEvent.getEvents(event, catalogName);
            case INSERT:
                return InsertEvent.getEvents(event, catalogName);
            default:
                // ignore all the unknown events by creating a IgnoredEvent
                return IgnoredEvent.getEvents(event, catalogName);
        }
    }

    List<MetastoreEvent> getMetastoreEvents(List<NotificationEvent> events, HMSExternalCatalog hmsExternalCatalog) {
        List<MetastoreEvent> metastoreEvents = Lists.newArrayList();
        for (NotificationEvent event : events) {
            metastoreEvents.addAll(transferNotificationEventToMetastoreEvents(event, hmsExternalCatalog.getName()));
        }
        return mergeEvents(hmsExternalCatalog.getName(), metastoreEvents);
    }

    /**
     * Merge events to reduce the cost time on event processing, currently mainly handles table events
     * because handle table events is simple and effective.
     * */
    List<MetastoreEvent> mergeEvents(String catalogName, List<MetastoreEvent> events) {
        List<MetastoreEvent> eventsCopy = Lists.newArrayList(events);
        Map<String, List<Integer>> indexMap = Maps.newLinkedHashMap();
        for (int i = 0; i < events.size(); i++) {
            MetastoreEvent event = events.get(i);
            if (!(event instanceof MetastoreTableEvent) || event instanceof CreateTableEvent
                        || (event instanceof AlterTableEvent && ((AlterTableEvent) event).isRename())) {
                continue;
            }

            String groupKey = String.format("%s.%s.%s",
                        event.catalogName, event.dbName, event.tblName);
            if (!indexMap.containsKey(groupKey)) {
                List<Integer> indexList = Lists.newLinkedList();
                indexList.add(i);
                indexMap.put(groupKey, indexList);
                continue;
            }

            List<Integer> indexList = indexMap.get(groupKey);
            if ((event instanceof InsertEvent)
                        || (event instanceof DropTableEvent)
                        || (event instanceof AlterTableEvent)) {
                for (int j = 0; j < indexList.size(); j++) {
                    int candidateIndex = indexList.get(j);
                    if (events.get(candidateIndex) instanceof DropTableEvent
                                && !(event instanceof DropTableEvent)) {
                        continue;
                    }
                    eventsCopy.set(candidateIndex, null);
                    indexList.set(j, null);
                }
                indexList = indexList.stream().filter(Objects::nonNull)
                            .collect(Collectors.toList());
            }
            indexList.add(i);
        }

        List<MetastoreEvent> filteredEvents = eventsCopy.stream().filter(Objects::nonNull)
                    .collect(Collectors.toList());
        LOG.info("Event size on catalog [{}] before merge is [{}], after merge is [{}]",
                    catalogName, events.size(), filteredEvents.size());
        return ImmutableList.copyOf(filteredEvents);
    }
}
