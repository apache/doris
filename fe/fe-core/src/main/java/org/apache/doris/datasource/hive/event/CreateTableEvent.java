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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for CREATE_TABLE event type
 */
public class CreateTableEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(CreateTableEvent.class);
    private Table hmsTbl;

    private CreateTableEvent(NotificationEvent event, String catalogName) throws MetastoreNotificationException {
        super(event, catalogName);
        Preconditions.checkArgument(MetastoreEventType.CREATE_TABLE.equals(getEventType()));
        Preconditions.checkNotNull(MetastoreEventType.CREATE_TABLE, debugString("Event message is null"));
        CreateTableMessage createTableMessage =
                MetastoreEventsProcessor.getMessageDeserializer().getCreateTableMessage(event.getMessage());

        try {
            hmsTbl = createTableMessage.getTableObj();
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to deserialize the event message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event, String catalogName) {
        return Lists.newArrayList(
                new CreateTableEvent(event, catalogName));
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            LOG.info("CreateTable event process,catalogName:[{}],dbName:[{}],tableName:[{}]", catalogName, dbName,
                    hmsTbl.getTableName());
            boolean hasExist = Env.getCurrentEnv().getCatalogMgr()
                    .externalTableExist(dbName, hmsTbl.getTableName(), catalogName);
            if (hasExist) {
                LOG.warn(
                        "CreateExternalTable canceled,because table has exist, "
                                + "dbName:[{}],tableName:[{}],catalogName:[{}].",
                        dbName, hmsTbl.getTableName(), catalogName);
                return;
            }
            Env.getCurrentEnv().getCatalogMgr().createExternalTable(dbName, hmsTbl.getTableName(), catalogName);
        } catch (DdlException e) {
            LOG.warn("CreateExternalTable failed,dbName:[{}],tableName:[{}],catalogName:[{}].", dbName,
                    hmsTbl.getTableName(), catalogName, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process create table event"));
        }
    }
}
