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
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.TRemoteDorisTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class RemoteDorisExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisExternalTable.class);

    public RemoteDorisExternalTable(long id, String name, String remoteName,
                                    RemoteDorisExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.DORIS_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TRemoteDorisTable tRemoteDorisTable = new TRemoteDorisTable();
        tRemoteDorisTable.setDbName(dbName);
        tRemoteDorisTable.setTableName(name);
        tRemoteDorisTable.setProperties(getCatalog().getProperties());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(),
                TTableType.REMOTE_DORIS_TABLE, schema.size(), 0, getName(), dbName);

        tTableDescriptor.setRemoteDorisTable(tRemoteDorisTable);
        return tTableDescriptor;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        RemoteDorisRestClient restClient = ((RemoteDorisExternalCatalog) catalog).getDorisRestClient();
        return Optional.of(new SchemaCacheValue(restClient.getColumns(dbName, name)));
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        RemoteDorisRestClient restClient = ((RemoteDorisExternalCatalog) catalog).getDorisRestClient();
        return restClient.getRowCount(getDbName(), getName());
    }

    public String getExternalTableName() {
        return getDbName() + "." + getName();
    }
}
