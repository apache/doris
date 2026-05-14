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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.paimon.table.Table;

import java.util.List;
import java.util.Objects;

/**
 * Opaque table handle for Paimon tables.
 * Carries database name, table name, partition key names, and the Paimon Table reference.
 */
public class PaimonTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;

    /** Transient Paimon Table reference; not serialized. Set by PaimonConnectorMetadata. */
    private transient Table paimonTable;

    public PaimonTableHandle(String databaseName, String tableName,
            List<String> partitionKeys, List<String> primaryKeys) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    /** Returns the transient Paimon Table reference, or null if not set. */
    public Table getPaimonTable() {
        return paimonTable;
    }

    /** Sets the transient Paimon Table reference. */
    public void setPaimonTable(Table paimonTable) {
        this.paimonTable = paimonTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PaimonTableHandle)) {
            return false;
        }
        PaimonTableHandle that = (PaimonTableHandle) o;
        return databaseName.equals(that.databaseName) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName);
    }

    @Override
    public String toString() {
        return databaseName + "." + tableName;
    }
}
