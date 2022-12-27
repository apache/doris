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
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for ALTER_PARTITION event type
 */
public class AlterPartitionEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(AlterPartitionEvent.class);

    // the Partition object before alter operation, as parsed from the NotificationEvent
    private final Partition partitionBefore;
    // the Partition object after alter operation, as parsed from the NotificationEvent
    private final Partition partitionAfter;

    private AlterPartitionEvent(NotificationEvent event, String catalogName) {
        super(event, catalogName);
        Preconditions.checkState(getEventType() == MetastoreEventType.ALTER_PARTITION);
        Preconditions.checkNotNull(event.getMessage());
        AlterPartitionMessage alterPartitionMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getAlterPartitionMessage(event.getMessage());

        try {
            partitionBefore = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjBefore());
            partitionAfter = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjAfter());
            hmsTbl = alterPartitionMessage.getTableObj();
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter partition message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new AlterPartitionEvent(event, catalogName));
    }

    @Override
    protected boolean canBeBatched(MetastoreEvent event) {
        return true;
    }

    @Override
    protected MetastoreEvent addToBatchEvents(MetastoreEvent event) {
        BatchEvent<MetastoreTableEvent> batchEvent = new BatchEvent<>(this);
        Preconditions.checkState(batchEvent.canBeBatched(event));
        batchEvent.addToBatchEvents(event);
        return batchEvent;
    }

    @Override
    protected boolean existInCache() {
        return true;
    }

    @Override
    protected boolean canBeSkipped() {
        return false;
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    @Override
    protected void process() throws MetastoreNotificationException {

    }
}
