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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogIf;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metastore event handler for INSERT events. Handles insert events at both table
 * and partition scopes.
 */
public class InsertEvent extends MetastoreTableEvent {
    private final String tableName;
    private String partitionName;
    private final HMSExternalTable table;

    private InsertEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkState(getEventType().equals(MetastoreEventType.INSERT));
        Preconditions.checkNotNull(event.getMessage());
        try {
            InsertMessage insertMessage =
                    MetastoreEventsProcessor.getMessageDeserializer()
                            .getInsertMessage(event.getMessage());
            tableName = Preconditions.checkNotNull(insertMessage.getTable());
            //can only get Table from Catalog,because current hive version api can not return 'Table'
            CatalogIf catalog = Preconditions.checkNotNull(Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName));
            DatabaseIf db = Preconditions.checkNotNull(catalog.getDbNullable(getDbName()));
            TableIf tableIf = Preconditions.checkNotNull(db.getTableNullable(tableName));
            table = (HMSExternalTable) tableIf;
            List<Column> partitionColumns = table.getPartitionColumns();
            List<String> partitionColNames = partitionColumns.stream()
                    .map(Column::getName).collect(Collectors.toList());
            Map<String, String> partitionKeyValues = insertMessage.getPartitionKeyValues();
            if (!table.getPartitionColumnNames().isEmpty()) {
                partitionName = getPartitionName(partitionKeyValues, partitionColNames);
            }
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new InsertEvent(event, catalogName));
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            debugLog("catalogName:[{}],dbName:[{}],tableName:[{}]", catalogName, dbName, tblName);
            if (table.getPartitionColumnNames().isEmpty()) {
                Env.getCurrentEnv().getCatalogMgr()
                        .refreshExternalTable(dbName, tableName, catalogName);
            } else {
                Env.getCurrentEnv().getCatalogMgr()
                        .refreshExternalPartitions(catalogName, dbName, tableName,
                                Lists.newArrayList(partitionName));
            }
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"));
        }
    }
}
