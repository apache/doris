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

package org.apache.doris.datasource.fluss;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TFlussTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.fluss.metadata.TableInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class FlussExternalTable extends ExternalTable {

    public enum FlussTableType {
        LOG_TABLE,
        PRIMARY_KEY_TABLE
    }

    private volatile FlussTableMetadata tableMetadata;

    public FlussExternalTable(long id, String name, String remoteName, FlussExternalCatalog catalog,
            FlussExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.FLUSS_EXTERNAL_TABLE);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        makeSureInitialized();
        return FlussUtils.loadSchemaCacheValue(this);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TFlussTable tFlussTable = new TFlussTable(getDbName(), getName(), new HashMap<>());
        tFlussTable.setBootstrap_servers(getBootstrapServers());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.FLUSS_EXTERNAL_TABLE,
                schema.size(), 0, getName(), getDbName());
        tTableDescriptor.setFlussTable(tFlussTable);
        return tTableDescriptor;
    }

    public String getBootstrapServers() {
        FlussExternalCatalog catalog = (FlussExternalCatalog) getCatalog();
        return catalog.getBootstrapServers();
    }

    public int getNumBuckets() {
        ensureTableMetadataLoaded();
        return tableMetadata != null ? tableMetadata.getNumBuckets() : 1;
    }

    public List<String> getPartitionKeys() {
        ensureTableMetadataLoaded();
        return tableMetadata != null ? tableMetadata.getPartitionKeys() : new ArrayList<>();
    }

    public List<String> getPrimaryKeys() {
        ensureTableMetadataLoaded();
        return tableMetadata != null ? tableMetadata.getPrimaryKeys() : new ArrayList<>();
    }

    public FlussTableType getFlussTableType() {
        ensureTableMetadataLoaded();
        return tableMetadata != null ? tableMetadata.getTableType() : FlussTableType.LOG_TABLE;
    }

    public String getRemoteDbName() {
        return ((FlussExternalDatabase) getDatabase()).getRemoteName();
    }

    public String getRemoteName() {
        return remoteName;
    }

    private void ensureTableMetadataLoaded() {
        if (tableMetadata == null) {
            synchronized (this) {
                if (tableMetadata == null) {
                    loadTableMetadata();
                }
            }
        }
    }

    private void loadTableMetadata() {
        try {
            FlussExternalCatalog catalog = (FlussExternalCatalog) getCatalog();
            FlussMetadataOps metadataOps = (FlussMetadataOps) catalog.getMetadataOps();
            this.tableMetadata = metadataOps.getTableMetadata(getRemoteDbName(), getRemoteName());
        } catch (Exception e) {
            // Use defaults if metadata loading fails
            this.tableMetadata = new FlussTableMetadata();
        }
    }

    public static class FlussTableMetadata {
        private FlussTableType tableType = FlussTableType.LOG_TABLE;
        private List<String> primaryKeys = new ArrayList<>();
        private List<String> partitionKeys = new ArrayList<>();
        private int numBuckets = 1;

        public FlussTableType getTableType() {
            return tableType;
        }

        public void setTableType(FlussTableType tableType) {
            this.tableType = tableType;
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }

        public void setPrimaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys != null ? primaryKeys : new ArrayList<>();
        }

        public List<String> getPartitionKeys() {
            return partitionKeys;
        }

        public void setPartitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys != null ? partitionKeys : new ArrayList<>();
        }

        public int getNumBuckets() {
            return numBuckets;
        }

        public void setNumBuckets(int numBuckets) {
            this.numBuckets = numBuckets > 0 ? numBuckets : 1;
        }
    }
}

