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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MetastoreEvent for DROP_PARTITION event type
 */
public class DropPartitionEvent extends MetastorePartitionEvent {
    private final Table hmsTbl;
    private final List<String> partitionNames;

    // for test
    public DropPartitionEvent(long eventId, String catalogName, String dbName,
                              String tblName, List<String> partitionNames) {
        super(eventId, catalogName, dbName, tblName, MetastoreEventType.DROP_PARTITION);
        this.partitionNames = partitionNames;
        this.hmsTbl = null;
    }

    private DropPartitionEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(getEventType().equals(MetastoreEventType.DROP_PARTITION));
        Preconditions
                .checkNotNull(event.getMessage(), debugString("Event message is null"));
        try {
            DropPartitionMessage dropPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                            .getDropPartitionMessage(event.getMessage());
            hmsTbl = Preconditions.checkNotNull(dropPartitionMessage.getTableObj());
            List<Map<String, String>> droppedPartitions = dropPartitionMessage.getPartitions();
            partitionNames = new ArrayList<>();
            List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
                    .map(FieldSchema::getName).collect(Collectors.toList());
            droppedPartitions.forEach(partition -> partitionNames.add(
                    getPartitionName(partition, partitionColNames)));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    @Override
    protected boolean willChangePartitionName() {
        return false;
    }

    @Override
    public Set<String> getAllPartitionNames() {
        return ImmutableSet.copyOf(partitionNames);
    }

    public void removePartition(String partitionName) {
        partitionNames.remove(partitionName);
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(
                new DropPartitionEvent(event, catalogName));
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            infoLog("catalogName:[{}],dbName:[{}],tableName:[{}],partitionNames:[{}]", catalogName, dbName, tblName,
                    partitionNames.toString());
            // bail out early if there are not partitions to process
            if (partitionNames.isEmpty()) {
                infoLog("Partition list is empty. Ignoring this event.");
                return;
            }
            Env.getCurrentEnv().getCatalogMgr()
                    .dropExternalPartitions(catalogName, dbName, hmsTbl.getTableName(),
                                partitionNames, eventTime, true);
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }

    @Override
    protected boolean canBeBatched(MetastoreEvent that) {
        if (!isSameTable(that) || !(that instanceof MetastorePartitionEvent)) {
            return false;
        }

        MetastorePartitionEvent thatPartitionEvent = (MetastorePartitionEvent) that;
        // Check if `that` event is a rename event, a rename event can not be batched
        // because the process of `that` event will change the reference relation of this partition
        if (thatPartitionEvent.willChangePartitionName()) {
            return false;
        }

        // `that` event can be batched if this event's partitions contains all the partitions which `that` event has
        // else just remove `that` event's relevant partitions
        for (String partitionName : getAllPartitionNames()) {
            if (thatPartitionEvent instanceof AddPartitionEvent) {
                ((AddPartitionEvent) thatPartitionEvent).removePartition(partitionName);
            } else if (thatPartitionEvent instanceof DropPartitionEvent) {
                ((DropPartitionEvent) thatPartitionEvent).removePartition(partitionName);
            }
        }

        return getAllPartitionNames().containsAll(thatPartitionEvent.getAllPartitionNames());
    }
}
