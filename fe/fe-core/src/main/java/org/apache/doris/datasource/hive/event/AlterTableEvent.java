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

import java.security.SecureRandom;
import java.util.List;
import java.util.Locale;

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
    private final boolean isView;
    private final String tblNameAfter;

    // for test
    public AlterTableEvent(long eventId, String catalogName, String dbName,
                           String tblName, boolean isRename, boolean isView) {
        super(eventId, catalogName, dbName, tblName, MetastoreEventType.ALTER_TABLE);
        this.isRename = isRename;
        this.isView = isView;
        this.tableBefore = null;
        this.tableAfter = null;
        this.tblNameAfter = isRename ? (tblName + new SecureRandom().nextInt(10)) : tblName;
    }

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
            tableAfter.setTableName(tableAfter.getTableName().toLowerCase(Locale.ROOT));
            tableBefore = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
            tblNameAfter = tableAfter.getTableName();
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter table message"), e);
        }
        // this is a rename event if either dbName or tblName of before and after object changed
        isRename = !tableBefore.getDbName().equalsIgnoreCase(tableAfter.getDbName())
                || !tableBefore.getTableName().equalsIgnoreCase(tableAfter.getTableName());
        isView = tableBefore.isSetViewExpandedText() || tableBefore.isSetViewOriginalText();
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 String catalogName) {
        return Lists.newArrayList(new AlterTableEvent(event, catalogName));
    }

    @Override
    protected boolean willCreateOrDropTable() {
        return isRename || isView;
    }

    @Override
    protected boolean willChangeTableName() {
        return isRename;
    }

    private void processRecreateTable() throws DdlException {
        if (!isView) {
            return;
        }
        Env.getCurrentEnv().getCatalogMgr()
                .unregisterExternalTable(tableBefore.getDbName(), tableBefore.getTableName(), catalogName, true);
        Env.getCurrentEnv().getCatalogMgr()
                .registerExternalTableFromEvent(
                            tableAfter.getDbName(), tableAfter.getTableName(), catalogName, eventTime, true);
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
                .unregisterExternalTable(tableBefore.getDbName(), tableBefore.getTableName(), catalogName, true);
        Env.getCurrentEnv().getCatalogMgr()
                .registerExternalTableFromEvent(
                            tableAfter.getDbName(), tableAfter.getTableName(), catalogName, eventTime, true);

    }

    public boolean isRename() {
        return isRename;
    }

    public boolean isView() {
        return isView;
    }

    public String getTblNameAfter() {
        return tblNameAfter;
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
            if (isView) {
                // if this table is a view, `viewExpandedText/viewOriginalText` of this table may be changed,
                // so we need to recreate the table to make sure `remoteTable` will be rebuild
                processRecreateTable();
                return;
            }
            //The scope of refresh can be narrowed in the future
            Env.getCurrentEnv().getRefreshManager()
                    .refreshExternalTableFromEvent(catalogName, tableBefore.getDbName(), tableBefore.getTableName(),
                            eventTime);
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }

    @Override
    protected boolean canBeBatched(MetastoreEvent that) {
        if (!isSameTable(that)) {
            return false;
        }

        // First check if `that` event is a rename event, a rename event can not be batched
        // because the process of `that` event will change the reference relation of this table
        // `that` event must be a MetastoreTableEvent event otherwise `isSameTable` will return false
        MetastoreTableEvent thatTblEvent = (MetastoreTableEvent) that;
        if (thatTblEvent.willChangeTableName()) {
            return false;
        }

        // Then check if the process of this event will create or drop this table,
        // if true then `that` event can be batched
        if (willCreateOrDropTable()) {
            return true;
        }

        // Last, check if the process of `that` event will create or drop this table
        // if false then `that` event can be batched
        return !thatTblEvent.willCreateOrDropTable();
    }
}
