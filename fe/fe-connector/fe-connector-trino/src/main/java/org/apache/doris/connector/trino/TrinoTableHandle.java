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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;

import java.util.Map;

/**
 * Opaque table handle carrying the Trino-internal state needed for metadata operations.
 * fe-core never inspects the internals of this handle.
 */
public class TrinoTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final String tableName;
    private final transient io.trino.spi.connector.ConnectorTableHandle trinoTableHandle;
    private final transient Map<String, ColumnHandle> columnHandleMap;
    private final transient Map<String, ColumnMetadata> columnMetadataMap;

    public TrinoTableHandle(String dbName, String tableName,
            io.trino.spi.connector.ConnectorTableHandle trinoTableHandle,
            Map<String, ColumnHandle> columnHandleMap,
            Map<String, ColumnMetadata> columnMetadataMap) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.trinoTableHandle = trinoTableHandle;
        this.columnHandleMap = columnHandleMap;
        this.columnMetadataMap = columnMetadataMap;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public io.trino.spi.connector.ConnectorTableHandle getTrinoTableHandle() {
        return trinoTableHandle;
    }

    public Map<String, ColumnHandle> getColumnHandleMap() {
        return columnHandleMap;
    }

    public Map<String, ColumnMetadata> getColumnMetadataMap() {
        return columnMetadataMap;
    }
}
