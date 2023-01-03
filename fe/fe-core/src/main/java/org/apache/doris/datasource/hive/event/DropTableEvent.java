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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for DROP_TABLE event type
 */
public class DropTableEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(DropTableEvent.class);
    private final String dbName;
    private final String tableName;

    private DropTableEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(MetastoreEventType.DROP_TABLE.equals(getEventType()));
        JSONDropTableMessage dropTableMessage =
                (JSONDropTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
                        .getDropTableMessage(event.getMessage());
        try {
            dbName = dropTableMessage.getDB();
            tableName = dropTableMessage.getTable();
        } catch (Exception e) {
            throw new MetastoreNotificationException(debugString(
                    "Could not parse event message. "
                            + "Check if %s is set to true in metastore configuration",
                    MetastoreEventsProcessor.HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new DropTableEvent(event, catalogName));
    }

    @Override
    protected boolean existInCache() {
        return true;
    }

    @Override
    protected boolean canBeSkipped() {
        return false;
    }

    protected boolean isSupported() {
        return true;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            LOG.info("DropTable event process,catalogName:[{}],dbName:[{}],tableName:[{}]", catalogName, dbName,
                    tableName);
            Env.getCurrentEnv().getCatalogMgr().dropExternalTable(dbName, tableName, catalogName);
        } catch (DdlException e) {
            LOG.warn("DropExternalTable failed,dbName:[{}],tableName:[{}],catalogName:[{}].", dbName, tableName,
                    catalogName, e);
        }
    }
}
