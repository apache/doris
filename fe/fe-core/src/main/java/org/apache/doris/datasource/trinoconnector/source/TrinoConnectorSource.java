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

package org.apache.doris.datasource.trinoconnector.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.thrift.TFileAttributes;

import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class TrinoConnectorSource {
    private final TupleDescriptor desc;
    private final TrinoConnectorExternalCatalog trinoConnectorExternalCatalog;
    private final TrinoConnectorExternalTable trinoConnectorExtTable;
    private final CatalogHandle catalogHandle;
    private final Session trinoSession;
    private final Connector connector;
    private final ConnectorName connectorName;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private ConnectorTableHandle trinoConnectorTableHandle;
    private ConnectorMetadata connectorMetadata;

    public TrinoConnectorSource(TupleDescriptor desc) {
        this.desc = desc;
        this.trinoConnectorExtTable = (TrinoConnectorExternalTable) desc.getTable();
        this.trinoConnectorExternalCatalog = (TrinoConnectorExternalCatalog) trinoConnectorExtTable.getCatalog();
        this.catalogHandle = trinoConnectorExternalCatalog.getTrinoCatalogHandle();
        this.trinoConnectorTableHandle = trinoConnectorExtTable.getConnectorTableHandle();
        this.connectorMetadata = trinoConnectorExtTable.getConnectorMetadata();
        this.connectorTransactionHandle = trinoConnectorExtTable.getConnectorTransactionHandle();
        this.trinoSession = trinoConnectorExternalCatalog.getTrinoSession();
        this.connector = trinoConnectorExternalCatalog.getConnector();
        this.connectorName = trinoConnectorExternalCatalog.getConnectorName();
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public ConnectorTableHandle getTrinoConnectorTableHandle() {
        return trinoConnectorTableHandle;
    }

    public TrinoConnectorExternalTable getTargetTable() {
        return trinoConnectorExtTable;
    }

    public TFileAttributes getFileAttributes() throws UserException {
        return new TFileAttributes();
    }

    public TrinoConnectorExternalCatalog getCatalog() {
        return trinoConnectorExternalCatalog;
    }

    public CatalogHandle getCatalogHandle() {
        return catalogHandle;
    }

    public Session getTrinoSession() {
        return trinoSession;
    }

    public Connector getConnector() {
        return connector;
    }

    public ConnectorName getConnectorName() {
        return connectorName;
    }

    public ConnectorMetadata getConnectorMetadata() {
        return connectorMetadata;
    }

    public void setTrinoConnectorTableHandle(ConnectorTableHandle trinoConnectorExtTableHandle) {
        this.trinoConnectorTableHandle = trinoConnectorExtTableHandle;
    }

    public ConnectorTransactionHandle getConnectorTransactionHandle() {
        return connectorTransactionHandle;
    }
}
