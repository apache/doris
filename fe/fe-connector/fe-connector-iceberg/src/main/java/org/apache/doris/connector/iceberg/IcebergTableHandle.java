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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

/**
 * Opaque table handle for an Iceberg table, carrying the database (namespace)
 * and table name coordinates.
 */
public class IcebergTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final String tableName;

    public IcebergTableHandle(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "IcebergTableHandle{" + dbName + "." + tableName + "}";
    }
}
