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
        super(eventId, catalogName, dbName, tblName);
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
    protected void process() throws MetastoreNotificationException {
        try {
            infoLog("catalogName:[{}],dbName:[{}],tableName:[{}]", catalogName, dbName, tableName);
            Env.getCurrentEnv().getCatalogMgr().dropExternalTable(dbName, tableName, catalogName, true);
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }

    @Override
    protected boolean canBeBatched(MetastoreEvent that) {
        // `that` event must not be a rename table event
        // so merge all events which belong to this table before is ok
        return isSameTable(that);
    }
}
