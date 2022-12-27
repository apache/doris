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

/**
 * Metastore event handler for INSERT events. Handles insert events at both table and partition scopes.
 * If partition is null, treat it as ALTER_TABLE event, otherwise as ALTER_PARTITION event.
 */
public class InsertEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(InsertEvent.class);

    // Represents the partition for this insert. Null if the table is unpartitioned.
    //    private final Partition insertPartition;

    private InsertEvent(NotificationEvent event, String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(MetastoreEventType.INSERT.equals(getEventType()));
        try {
            //            hmsTbl = Preconditions.checkNotNull(insertMessage.getTableObj());
            //            insertPartition = insertMessage.getPtnObj();
            //            if (insertPartition != null) {
            //                List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
            //                        .map(FieldSchema::getName).collect(Collectors.toList());
            //                hivePartitionNames.add(HivePartitionName.of(dbName, tblName,
            //                        FileUtils.makePartName(partitionColNames, insertPartition.getValues())));
            //            }
        } catch (Exception e) {
            LOG.warn(
                    "The InsertEvent of the current hive version cannot be parsed,and there will be a corresponding "
                            + "Alter Event in next, InsertEvent is ignored here. {}",
                    e.getMessage());
            throw new MetastoreNotificationException(debugString("Unable to "
                    + "parse insert message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        try {
            return Lists.newArrayList(new InsertEvent(event, catalogName));
        } catch (MetastoreNotificationException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public boolean canBeBatched(MetastoreEvent event) {
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
    protected boolean isSupported() {
        return true;
    }

    public boolean isPartitionTbl() {
        return true;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            return;
        }
    }
}
