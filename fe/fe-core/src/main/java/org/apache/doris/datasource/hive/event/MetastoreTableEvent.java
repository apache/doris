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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.List;
import java.util.Objects;

/**
 * Base class for all the table events
 */
public abstract class MetastoreTableEvent extends MetastoreEvent {

    // for test
    protected MetastoreTableEvent(long eventId, String catalogName, String dbName,
                                  String tblName, MetastoreEventType eventType) {
        super(eventId, catalogName, dbName, tblName, eventType);
    }

    protected MetastoreTableEvent(NotificationEvent event, String catalogName) {
        super(event, catalogName);
        Preconditions.checkNotNull(dbName, "Database name cannot be null");
        Preconditions.checkNotNull(tblName, "Table name cannot be null");
    }

    /**
     * Returns a list of parameters that are set by Hive for tables/partitions that can be
     * ignored to determine if the alter table/partition event is a trivial one.
     */
    private static final List<String> PARAMETERS_TO_IGNORE =
            new ImmutableList.Builder<String>()
                    .add("transient_lastDdlTime")
                    .add("numFilesErasureCoded")
                    .add("numFiles")
                    .add("comment")
                    .build();

    protected boolean isSameTable(MetastoreEvent that) {
        if (!(that instanceof MetastoreTableEvent)) {
            return false;
        }
        TableKey thisKey = getTableKey();
        TableKey thatKey = ((MetastoreTableEvent) that).getTableKey();
        return Objects.equals(thisKey, thatKey);
    }

    /**
     * Returns if the process of this event will create or drop this table.
     */
    protected abstract boolean willCreateOrDropTable();

    /**
     * Returns if the process of this event will rename this table.
     */
    protected abstract boolean willChangeTableName();

    public TableKey getTableKey() {
        return new TableKey(catalogName, dbName, tblName);
    }

    static class TableKey {
        private final String catalogName;
        private final String dbName;
        private final String tblName;

        private TableKey(String catalogName, String dbName, String tblName) {
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tblName = tblName;
        }

        @Override
        public boolean equals(Object that) {
            if (!(that instanceof TableKey)) {
                return false;
            }
            return Objects.equals(catalogName, ((TableKey) that).catalogName)
                        && Objects.equals(dbName, ((TableKey) that).dbName)
                        && Objects.equals(tblName, ((TableKey) that).tblName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, dbName, tblName);
        }
    }
}
