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
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterDatabaseMessage;

import java.security.SecureRandom;
import java.util.List;

/**
 * MetastoreEvent for ALTER_DATABASE event type
 */
public class AlterDatabaseEvent extends MetastoreEvent {

    private final Database dbBefore;
    private final Database dbAfter;

    // true if this alter event was due to a rename operation
    private final boolean isRename;
    private final String dbNameAfter;

    // for test
    public AlterDatabaseEvent(long eventId, String catalogName, String dbName, boolean isRename) {
        super(eventId, catalogName, dbName, null, MetastoreEventType.ALTER_DATABASE);
        this.isRename = isRename;
        this.dbBefore = null;
        this.dbAfter = null;
        this.dbNameAfter = isRename ? (dbName + new SecureRandom().nextInt(10)) : dbName;
    }

    private AlterDatabaseEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(getEventType().equals(MetastoreEventType.ALTER_DATABASE));

        try {
            JSONAlterDatabaseMessage alterDatabaseMessage =
                    (JSONAlterDatabaseMessage) MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                                .getAlterDatabaseMessage(event.getMessage());
            dbBefore = Preconditions.checkNotNull(alterDatabaseMessage.getDbObjBefore());
            dbAfter = Preconditions.checkNotNull(alterDatabaseMessage.getDbObjAfter());
            dbNameAfter = dbAfter.getName();
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter database message"), e);
        }
        // this is a rename event if either dbName of before and after object changed
        isRename = !dbBefore.getName().equalsIgnoreCase(dbAfter.getName());
    }

    private void processRename() throws DdlException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog Databases");
        }
        if (catalog.getDbNullable(dbAfter.getName()) != null) {
            infoLog("AlterExternalDatabase canceled, because dbAfter has exist, "
                            + "catalogName:[{}],dbName:[{}]",
                    catalogName, dbAfter.getName());
            return;
        }
        Env.getCurrentEnv().getCatalogMgr().dropExternalDatabase(dbBefore.getName(), catalogName, true);
        Env.getCurrentEnv().getCatalogMgr().createExternalDatabase(dbAfter.getName(), catalogName, true);

    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new AlterDatabaseEvent(event, catalogName));
    }

    public boolean isRename() {
        return isRename;
    }

    public String getDbNameAfter() {
        return dbNameAfter;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            if (isRename) {
                processRename();
                return;
            }
            // only can change properties,we do nothing
            infoLog("catalogName:[{}],dbName:[{}]", catalogName, dbName);
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }
}
