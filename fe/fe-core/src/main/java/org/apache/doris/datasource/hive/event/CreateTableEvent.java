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
import org.apache.doris.datasource.ExternalMetaIdMgr;
import org.apache.doris.datasource.MetaIdMappingsLog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;

import java.util.List;
import java.util.Locale;

/**
 * MetastoreEvent for CREATE_TABLE event type
 */
public class CreateTableEvent extends MetastoreTableEvent {
    private final Table hmsTbl;

    // for test
    public CreateTableEvent(long eventId, String catalogName, String dbName, String tblName) {
        super(eventId, catalogName, dbName, tblName, MetastoreEventType.CREATE_TABLE);
        this.hmsTbl = null;
    }

    private CreateTableEvent(NotificationEvent event, String catalogName) throws MetastoreNotificationException {
        super(event, catalogName);
        Preconditions.checkArgument(MetastoreEventType.CREATE_TABLE.equals(getEventType()));
        Preconditions
                .checkNotNull(event.getMessage(), debugString("Event message is null"));
        try {
            CreateTableMessage createTableMessage =
                    MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                            .getCreateTableMessage(event.getMessage());
            hmsTbl = Preconditions.checkNotNull(createTableMessage.getTableObj());
            hmsTbl.setTableName(hmsTbl.getTableName().toLowerCase(Locale.ROOT));
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to deserialize the event message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event, String catalogName) {
        return Lists.newArrayList(new CreateTableEvent(event, catalogName));
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
            infoLog("catalogName:[{}],dbName:[{}],tableName:[{}]", catalogName, dbName, tblName);
            Env.getCurrentEnv().getCatalogMgr()
                    .registerExternalTableFromEvent(dbName, hmsTbl.getTableName(), catalogName, eventTime, true);
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }

    @Override
    protected List<MetaIdMappingsLog.MetaIdMapping> transferToMetaIdMappings() {
        MetaIdMappingsLog.MetaIdMapping metaIdMapping = new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_ADD,
                    MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                    dbName, tblName, ExternalMetaIdMgr.nextMetaId());
        return ImmutableList.of(metaIdMapping);
    }
}
