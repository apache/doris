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
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;

import java.util.List;

/**
 * MetastoreEvent for INSERT event type
 */
public class InsertEvent extends MetastoreTableEvent {
    private final Table hmsTbl;

    // for test
    public InsertEvent(long eventId, String catalogName, String dbName,
                       String tblName) {
        super(eventId, catalogName, dbName, tblName);
        this.hmsTbl = null;
    }

    private InsertEvent(NotificationEvent event, String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(getEventType().equals(MetastoreEventType.INSERT));
        Preconditions
                .checkNotNull(event.getMessage(), debugString("Event message is null"));
        try {
            InsertMessage insertMessage =
                    MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                            .getInsertMessage(event.getMessage());
            hmsTbl = Preconditions.checkNotNull(insertMessage.getTableObj());
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event, String catalogName) {
        return Lists.newArrayList(new InsertEvent(event, catalogName));
    }

    @Override
    protected boolean willCreateOrDropTable() {
        return false;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            infoLog("catalogName:[{}],dbName:[{}],tableName:[{}]", catalogName, dbName, tblName);
            /***
             *  Only when we use hive client to execute a `INSERT INTO TBL SELECT * ...` or `INSERT INTO TBL ...` sql
             *  to a non-partitioned table then the hms will generate an insert event, and there is not
             *  any partition event occurs, but the file cache may has been changed, so we need handle this.
             *  Currently {@link org.apache.doris.datasource.CatalogMgr#refreshExternalTable} do not invalidate
             *  the file cache of this table,
             *  but <a href="https://github.com/apache/doris/pull/17932">this PR</a> has fixed it.
             */
            Env.getCurrentEnv().getCatalogMgr().refreshExternalTable(dbName, tblName, catalogName, true);
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

        // that event must be a MetastoreTableEvent event
        // otherwise `isSameTable` will return false
        return !((MetastoreTableEvent) that).willCreateOrDropTable();
    }
}
