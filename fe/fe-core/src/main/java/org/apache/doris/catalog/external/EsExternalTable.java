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
import org.apache.doris.catalog.EsTable;
import org.apache.doris.datasource.EsExternalCatalog;
import org.apache.doris.external.elasticsearch.EsUtil;
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

    private final EsExternalCatalog catalog;
    private final String dbName;
    private boolean initialized = false;
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
        super(id, name);
        this.dbName = dbName;
        this.catalog = catalog;
        this.type = TableType.ES_EXTERNAL_TABLE;
    }


    private synchronized void makeSureInitialized() {
        if (!initialized) {
            init();
            initialized = true;
        }
    }

    private void init() {
        fullSchema = EsUtil.genColumnsFromEs(catalog.getEsRestClient(), name, null);
        esTable = toEsTable();
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
     * get database name of hms table.
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
        EsTable esTable = new EsTable(this.id, this.name, this.fullSchema, TableType.ES_EXTERNAL_TABLE);
        esTable.setIndexName(name);
        esTable.setClient(catalog.getEsRestClient());
        esTable.setUserName(catalog.getUsername());
        esTable.setPasswd(catalog.getPassword());
        esTable.setEnableDocValueScan(catalog.isEnableDocValueScan());
        esTable.setEnableKeywordSniff(catalog.isEnableKeywordSniff());
        esTable.setNodesDiscovery(catalog.isEnableNodesDiscovery());
        esTable.setHttpSslEnabled(catalog.isEnableSsl());
        esTable.setSeeds(catalog.getNodes());
        esTable.setHosts(String.join(",", catalog.getNodes()));
        esTable.syncTableMetaData();
        return esTable;
    }
}
