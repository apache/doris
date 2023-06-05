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

import java.util.List;

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
        return createBatchEvents(metastoreEvents);
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
