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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for ADD_PARTITION event type
 */
public class AddPartitionEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(AddPartitionEvent.class);

    private final Partition addedPartition;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AddPartitionEvent(NotificationEvent event,
            Partition addedPartition,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkState(getEventType().equals(MetastoreEventType.ADD_PARTITION));
        if (event.getMessage() == null) {
            throw new IllegalStateException(debugString("Event message is null"));
        }

        try {
            AddPartitionMessage addPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer()
                            .getAddPartitionMessage(event.getMessage());
            this.addedPartition = addedPartition;
            hmsTbl = addPartitionMessage.getTableObj();
            //            hivePartitionNames.clear();
            //            List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
            //                    .map(FieldSchema::getName).collect(Collectors.toList());
            //            hivePartitionNames.add(HivePartitionName.of(dbName, tblName,
            //                    FileUtils.makePartName(partitionColNames, addedPartition.getValues())));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        List<MetastoreEvent> addPartitionEvents = Lists.newArrayList();
        try {
            AddPartitionMessage addPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer()
                            .getAddPartitionMessage(event.getMessage());
            addPartitionMessage.getPartitionObjs().forEach(partition ->
                    addPartitionEvents.add(new AddPartitionEvent(event, partition, catalogName)));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
        return addPartitionEvents;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        throw new UnsupportedOperationException("Unsupported event type: " + getEventType());
    }
}
