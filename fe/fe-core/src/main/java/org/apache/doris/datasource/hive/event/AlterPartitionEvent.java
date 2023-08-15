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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * MetastoreEvent for ALTER_PARTITION event type
 */
public class AlterPartitionEvent extends MetastorePartitionEvent {
    private final Table hmsTbl;
    private final org.apache.hadoop.hive.metastore.api.Partition partitionAfter;
    private final org.apache.hadoop.hive.metastore.api.Partition partitionBefore;
    private final String partitionNameBefore;
    private final String partitionNameAfter;
    // true if this alter event was due to a rename operation
    private final boolean isRename;

    // for test
    public AlterPartitionEvent(long eventId, String catalogName, String dbName, String tblName,
                                String partitionNameBefore, String partitionNameAfter) {
        super(eventId, catalogName, dbName, tblName);
        this.partitionNameBefore = partitionNameBefore;
        this.partitionNameAfter = partitionNameAfter;
        this.hmsTbl = null;
        this.partitionAfter = null;
        this.partitionBefore = null;
        isRename = !partitionNameBefore.equalsIgnoreCase(partitionNameAfter);
    }

    private AlterPartitionEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(getEventType().equals(MetastoreEventType.ALTER_PARTITION));
        Preconditions
                .checkNotNull(event.getMessage(), debugString("Event message is null"));
        try {
            AlterPartitionMessage alterPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                            .getAlterPartitionMessage(event.getMessage());
            hmsTbl = Preconditions.checkNotNull(alterPartitionMessage.getTableObj());
            partitionBefore = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjBefore());
            partitionAfter = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjAfter());
            List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
                    .map(FieldSchema::getName).collect(Collectors.toList());
            partitionNameBefore = FileUtils.makePartName(partitionColNames, partitionBefore.getValues());
            partitionNameAfter = FileUtils.makePartName(partitionColNames, partitionAfter.getValues());
            isRename = !partitionNameBefore.equalsIgnoreCase(partitionNameAfter);
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new AlterPartitionEvent(event, catalogName));
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            infoLog("catalogName:[{}],dbName:[{}],tableName:[{}],partitionNameBefore:[{}],partitionNameAfter:[{}]",
                    catalogName, dbName, tblName, partitionNameBefore, partitionNameAfter);
            if (isRename) {
                Env.getCurrentEnv().getCatalogMgr()
                        .dropExternalPartitions(catalogName, dbName, tblName,
                                Lists.newArrayList(partitionNameBefore), true);
                Env.getCurrentEnv().getCatalogMgr()
                        .addExternalPartitions(catalogName, dbName, tblName,
                                Lists.newArrayList(partitionNameAfter), true);
            } else {
                Env.getCurrentEnv().getCatalogMgr()
                        .refreshExternalPartitions(catalogName, dbName, hmsTbl.getTableName(),
                                Lists.newArrayList(partitionNameAfter), true);
            }
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }

    @Override
    protected boolean canBeBatched(MetastoreEvent event) {
        return isSameTable(event)
                    && event instanceof AlterPartitionEvent
                    && Objects.equals(partitionBefore, ((AlterPartitionEvent) event).partitionBefore)
                    && Objects.equals(partitionAfter, ((AlterPartitionEvent) event).partitionAfter);
    }
}
