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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;

/**
 * Opaque table handle for MaxCompute tables.
 * Carries the ODPS Table and TableIdentifier for downstream operations.
 */
public class MaxComputeTableHandle implements ConnectorTableHandle {
    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final String tableName;

    // Transient: not serialized, rebuilt from catalog on deserialization
    private transient Table odpsTable;
    private transient TableIdentifier tableIdentifier;

    public MaxComputeTableHandle(String dbName, String tableName,
            Table odpsTable, TableIdentifier tableIdentifier) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.odpsTable = odpsTable;
        this.tableIdentifier = tableIdentifier;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public Table getOdpsTable() {
        return odpsTable;
    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }
}
