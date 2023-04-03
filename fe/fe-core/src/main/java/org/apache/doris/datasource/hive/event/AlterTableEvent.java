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
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;

import java.util.List;

/**
 * MetastoreEvent for ALTER_TABLE event type
 */
public class AlterTableEvent extends MetastoreTableEvent {
    // the table object before alter operation
    private final Table tableBefore;
    // the table object after alter operation
    private final Table tableAfter;

    // true if this alter event was due to a rename operation
    private final boolean isRename;

    private AlterTableEvent(NotificationEvent event, String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(MetastoreEventType.ALTER_TABLE.equals(getEventType()));
        Preconditions
                .checkNotNull(event.getMessage(), debugString("Event message is null"));
        try {
            JSONAlterTableMessage alterTableMessage =
                    (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageDeserializer(event.getMessageFormat())
                            .getAlterTableMessage(event.getMessage());
            tableAfter = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
            tableBefore = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter table message"), e);
        }
        // this is a rename event if either dbName or tblName of before and after object changed
        isRename = !tableBefore.getDbName().equalsIgnoreCase(tableAfter.getDbName())
                || !tableBefore.getTableName().equalsIgnoreCase(tableAfter.getTableName());

    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new AlterTableEvent(event, catalogName));
    }


    private void processRename() throws DdlException {
        if (!isRename) {
            return;
        }
        boolean hasExist = Env.getCurrentEnv().getCatalogMgr()
                .externalTableExistInLocal(tableAfter.getDbName(), tableAfter.getTableName(), catalogName);
        if (hasExist) {
            infoLog("AlterExternalTable canceled,because tableAfter has exist, "
                            + "catalogName:[{}],dbName:[{}],tableName:[{}]",
                    catalogName, dbName, tableAfter.getTableName());
            return;
        }
        Env.getCurrentEnv().getCatalogMgr()
                .dropExternalTable(tableBefore.getDbName(), tableBefore.getTableName(), catalogName);
        Env.getCurrentEnv().getCatalogMgr()
                .createExternalTable(tableAfter.getDbName(), tableAfter.getTableName(), catalogName);

    }

    /**
     * If the ALTER_TABLE event is due a table rename, this method removes the old table
     * and creates a new table with the new name. Else, we just refresh table
     */
    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            infoLog("catalogName:[{}],dbName:[{}],tableBefore:[{}],tableAfter:[{}]", catalogName, dbName,
                    tableBefore.getTableName(), tableAfter.getTableName());
            if (isRename) {
                processRename();
                return;
            }
            //The scope of refresh can be narrowed in the future
            Env.getCurrentEnv().getCatalogMgr()
                    .refreshExternalTable(tableBefore.getDbName(), tableBefore.getTableName(), catalogName);
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"));
        }
    }
}
