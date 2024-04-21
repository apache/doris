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
import org.apache.doris.datasource.MetaIdMappingsLog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;

import java.util.List;

/**
 * MetastoreEvent for DROP_TABLE event type
 */
public class DropTableEvent extends MetastoreTableEvent {
    private final String tableName;

    // for test
    public DropTableEvent(long eventId, String catalogName, String dbName,
                           String tblName) {
        super(eventId, catalogName, dbName, tblName, MetastoreEventType.DROP_TABLE);
        this.tableName = tblName;
    }

    private DropTableEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(MetastoreEventType.DROP_TABLE.equals(getEventType()));
        Preconditions
                .checkNotNull(event.getMessage(), debugString("Event message is null"));
        try {
            JSONDropTableMessage dropTableMessage =
                    (JSONDropTableMessage) MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                            .getDropTableMessage(event.getMessage());
            tableName = dropTableMessage.getTable();
        } catch (Exception e) {
            throw new MetastoreNotificationException(e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new DropTableEvent(event, catalogName));
    }

    @Override
    protected boolean willCreateOrDropTable() {
        return true;
    }

    @Override
    protected boolean willChangeTableName() {
        return false;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            infoLog("catalogName:[{}],dbName:[{}],tableName:[{}]", catalogName, dbName, tableName);
            Env.getCurrentEnv().getCatalogMgr().unregisterExternalTable(dbName, tableName, catalogName, true);
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }

    @Override
    protected boolean canBeBatched(MetastoreEvent that) {
        if (!isSameTable(that)) {
            return false;
        }

        /*
         * Check if `that` event is a rename event, a rename event can not be batched
         * because the process of `that` event will change the reference relation of this table,
         * otherwise it can be batched because this event is a drop-table event
         * and the process of this event will drop the whole table,
         * and `that` event must be a MetastoreTableEvent event otherwise `isSameTable` will return false
         */
        MetastoreTableEvent thatTblEvent = (MetastoreTableEvent) that;
        return !thatTblEvent.willChangeTableName();
    }

    @Override
    protected List<MetaIdMappingsLog.MetaIdMapping> transferToMetaIdMappings() {
        MetaIdMappingsLog.MetaIdMapping metaIdMapping = new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                    MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                    dbName, tblName);
        return ImmutableList.of(metaIdMapping);
    }
}
