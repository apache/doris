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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.TruncateTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.dlf.DLFCatalog;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.TruncateTableCommand;

import java.util.Map;

public class IcebergDLFExternalCatalog extends IcebergExternalCatalog {

    public IcebergDLFExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props.put(HMSProperties.HIVE_METASTORE_TYPE, "dlf");
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_DLF;
        DLFCatalog dlfCatalog = new DLFCatalog();
        dlfCatalog.setConf(getConfiguration());
        // initialize catalog
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        String catalogName = getName();
        dlfCatalog.initialize(catalogName, catalogProperties);
        catalog = dlfCatalog;
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'create database'");
    }

    @Override
    public void createDb(CreateDatabaseCommand command) throws DdlException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'create database'");
    }

    @Override
    public void dropDb(String dbName, boolean ifExists, boolean force) throws DdlException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'drop database'");
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'create table'");
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'drop table'");
    }

    @Override
    public void dropTable(String dbName, String tableName, boolean isView, boolean isMtmv, boolean ifExists,
                   boolean force) throws DdlException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'drop table'");
    }

    @Override
    public void truncateTable(TruncateTableStmt stmt) throws DdlException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'truncate table'");
    }

    @Override
    public void truncateTable(TruncateTableCommand command) throws DdlException {
        throw new NotSupportedException("iceberg catalog with dlf type not supports 'truncate table'");
    }
}
