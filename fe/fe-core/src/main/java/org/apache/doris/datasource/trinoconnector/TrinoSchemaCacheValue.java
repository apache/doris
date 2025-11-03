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

package org.apache.doris.datasource.trinoconnector;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.SchemaCacheValue;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TrinoSchemaCacheValue extends SchemaCacheValue {
    private ConnectorMetadata connectorMetadata;
    private Optional<ConnectorTableHandle> connectorTableHandle;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private Map<String, ColumnHandle> columnHandleMap;
    private Map<String, ColumnMetadata> columnMetadataMap;

    public TrinoSchemaCacheValue(List<Column> schema, ConnectorMetadata connectorMetadata,
            Optional<ConnectorTableHandle> connectorTableHandle, ConnectorTransactionHandle connectorTransactionHandle,
            Map<String, ColumnHandle> columnHandleMap, Map<String, ColumnMetadata> columnMetadataMap) {
        super(schema);
        this.connectorMetadata = connectorMetadata;
        this.connectorTableHandle = connectorTableHandle;
        this.connectorTransactionHandle = connectorTransactionHandle;
        this.columnHandleMap = columnHandleMap;
        this.columnMetadataMap = columnMetadataMap;
    }

    public ConnectorMetadata getConnectorMetadata() {
        return connectorMetadata;
    }

    public Optional<ConnectorTableHandle> getConnectorTableHandle() {
        return connectorTableHandle;
    }

    public ConnectorTransactionHandle getConnectorTransactionHandle() {
        return connectorTransactionHandle;
    }

    public Map<String, ColumnHandle> getColumnHandleMap() {
        return columnHandleMap;
    }

    public Map<String, ColumnMetadata> getColumnMetadataMap() {
        return columnMetadataMap;
    }

    public void setConnectorMetadata(ConnectorMetadata connectorMetadata) {
        this.connectorMetadata = connectorMetadata;
    }

    public void setConnectorTableHandle(Optional<ConnectorTableHandle> connectorTableHandle) {
        this.connectorTableHandle = connectorTableHandle;
    }

    public void setConnectorTransactionHandle(ConnectorTransactionHandle connectorTransactionHandle) {
        this.connectorTransactionHandle = connectorTransactionHandle;
    }

    public void setColumnHandleMap(Map<String, ColumnHandle> columnHandleMap) {
        this.columnHandleMap = columnHandleMap;
    }

    public void setColumnMetadataMap(Map<String, ColumnMetadata> columnMetadataMap) {
        this.columnMetadataMap = columnMetadataMap;
    }
}
