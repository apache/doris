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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DorisTable;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TTableDescriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class DorisExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(DorisExternalTable.class);

    private DorisTable dorisTable;

    public DorisExternalTable(long id, String name, String remoteName,
                              DorisExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.DORIS_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            dorisTable = toDorisTable();
            objectCreated = true;
        }
    }

    public DorisTable getDorisTable() {
        makeSureInitialized();
        return dorisTable;
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        return dorisTable.toThrift();
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        DorisRestClient restClient = ((DorisExternalCatalog) catalog).getDorisRestClient();
        return Optional.of(new SchemaCacheValue(restClient.getColumns(dbName, name)));
    }

    private DorisTable toDorisTable() {
        List<Column> schema = getFullSchema();
        DorisExternalCatalog dorisCatalog = (DorisExternalCatalog) catalog;

        String fullDbName = this.dbName + "." + this.name;
        DorisTable dorisTable = new DorisTable(this.id, fullDbName, schema, TableType.JDBC_EXTERNAL_TABLE);

        dorisTable.setFeNodes(dorisCatalog.getFeNodes());
        dorisTable.setFeArrowNodes(dorisCatalog.getFeArrowNodes());
        dorisTable.setUserName(dorisCatalog.getUsername());
        dorisTable.setPassword(dorisCatalog.getPassword());
        dorisTable.setMaxExecBeNum(dorisCatalog.getMaxExecBeNum());
        dorisTable.setHttpSslEnabled(dorisCatalog.enableSsl());

        dorisTable.setExternalTableName(fullDbName);
        dorisTable.setBeNodes(dorisCatalog.getBeNodes());

        return dorisTable;
    }
}
