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
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MetastoreEvent for ADD_PARTITION event type
 */
public class AddPartitionEvent extends MetastorePartitionEvent {
    private final Table hmsTbl;
    private final List<String> partitionNames;

    // for test
    public AddPartitionEvent(long eventId, String catalogName, String dbName,
                             String tblName, List<String> partitionNames) {
        super(eventId, catalogName, dbName, tblName);
        this.partitionNames = partitionNames;
        this.hmsTbl = null;
    }

    private AddPartitionEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(getEventType().equals(MetastoreEventType.ADD_PARTITION));
        Preconditions
                .checkNotNull(event.getMessage(), debugString("Event message is null"));
        try {
            AddPartitionMessage addPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                            .getAddPartitionMessage(event.getMessage());
            hmsTbl = Preconditions.checkNotNull(addPartitionMessage.getTableObj());
            Iterable<Partition> addedPartitions = addPartitionMessage.getPartitionObjs();
            partitionNames = new ArrayList<>();
            List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
                    .map(FieldSchema::getName).collect(Collectors.toList());
            addedPartitions.forEach(partition -> partitionNames.add(
                    FileUtils.makePartName(partitionColNames, partition.getValues())));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new AddPartitionEvent(event, catalogName));
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
                    .addExternalPartitions(catalogName, dbName, hmsTbl.getTableName(), partitionNames, true);
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }
}
