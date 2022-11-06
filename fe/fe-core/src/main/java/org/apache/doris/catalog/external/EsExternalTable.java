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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.EsExternalCatalog;
import org.apache.doris.datasource.InitTableLog;
import org.apache.doris.external.elasticsearch.EsUtil;
import org.apache.doris.qe.MasterCatalogExecutor;
import org.apache.doris.thrift.TEsTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Elasticsearch external table.
 */
public class EsExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(EsExternalTable.class);
    private EsTable esTable;

    /**
     * Create elasticsearch external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param dbName Database name.
     * @param catalog HMSExternalDataSource.
     */
    public EsExternalTable(long id, String name, String dbName, EsExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.ES_EXTERNAL_TABLE);
    }


    public synchronized void makeSureInitialized() {
        if (!initialized) {
            if (!Env.getCurrentEnv().isMaster()) {
                fullSchema = null;
                // Forward to master and wait the journal to replay.
                MasterCatalogExecutor remoteExecutor = new MasterCatalogExecutor();
                try {
                    remoteExecutor.forward(catalog.getId(), catalog.getDbNullable(dbName).getId(), id);
                } catch (Exception e) {
                    Util.logAndThrowRuntimeException(LOG,
                            String.format("failed to forward init external table %s operation to master", name), e);
                }
            } else {
                init();
            }
        }
        if (!objectCreated) {
            esTable = toEsTable();
            objectCreated = true;
        }
    }

    private void init() {
        boolean schemaChanged = false;
        List<Column> tmpSchema = EsUtil.genColumnsFromEs(
                ((EsExternalCatalog) catalog).getEsRestClient(), name, null);
        if (fullSchema == null || fullSchema.size() != tmpSchema.size()) {
            schemaChanged = true;
        } else {
            for (int i = 0; i < fullSchema.size(); i++) {
                if (!fullSchema.get(i).equals(tmpSchema.get(i))) {
                    schemaChanged = true;
                    break;
                }
            }
        }
        if (schemaChanged) {
            timestamp = System.currentTimeMillis();
            fullSchema = tmpSchema;
            esTable = toEsTable();
        }
        initialized = true;
        InitTableLog initTableLog = new InitTableLog();
        initTableLog.setCatalogId(catalog.getId());
        initTableLog.setDbId(catalog.getDbNameToId().get(dbName));
        initTableLog.setTableId(id);
        initTableLog.setSchema(fullSchema);
        Env.getCurrentEnv().getEditLog().logInitExternalTable(initTableLog);
    }

    @Override
    public List<Column> getFullSchema() {
        makeSureInitialized();
        return fullSchema;
    }

    @Override
    public List<Column> getBaseSchema() {
        return getFullSchema();
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return getFullSchema();
    }

    @Override
    public Column getColumn(String name) {
        makeSureInitialized();
        for (Column column : fullSchema) {
            if (name.equals(column.getName())) {
                return column;
            }
        }
        return null;
    }

    public EsTable getEsTable() {
        makeSureInitialized();
        return esTable;
    }

    @Override
    public String getMysqlType() {
        return type.name();
    }

    /**
     * get database name of es table.
     */
    public String getDbName() {
        return dbName;
    }

    @Override
    public TTableDescriptor toThrift() {
        TEsTable tEsTable = new TEsTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ES_TABLE, fullSchema.size(), 0,
                getName(), "");
        tTableDescriptor.setEsTable(tEsTable);
        return tTableDescriptor;
    }

    private EsTable toEsTable() {
        EsExternalCatalog esCatalog = (EsExternalCatalog) catalog;
        EsTable esTable = new EsTable(this.id, this.name, this.fullSchema, TableType.ES_EXTERNAL_TABLE);
        esTable.setIndexName(name);
        esTable.setClient(esCatalog.getEsRestClient());
        esTable.setUserName(esCatalog.getUsername());
        esTable.setPasswd(esCatalog.getPassword());
        esTable.setEnableDocValueScan(esCatalog.isEnableDocValueScan());
        esTable.setEnableKeywordSniff(esCatalog.isEnableKeywordSniff());
        esTable.setNodesDiscovery(esCatalog.isEnableNodesDiscovery());
        esTable.setHttpSslEnabled(esCatalog.isEnableSsl());
        esTable.setSeeds(esCatalog.getNodes());
        esTable.setHosts(String.join(",", esCatalog.getNodes()));
        esTable.syncTableMetaData();
        return esTable;
    }
}
