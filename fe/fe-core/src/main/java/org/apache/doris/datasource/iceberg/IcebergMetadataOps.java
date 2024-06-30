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
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.DorisTypeVisitor;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.operations.ExternalMetadataOps;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergMetadataOps implements ExternalMetadataOps {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadataOps.class);
    protected Catalog catalog;
    protected IcebergExternalCatalog dorisCatalog;
    protected SupportsNamespaces nsCatalog;

    public IcebergMetadataOps(IcebergExternalCatalog dorisCatalog, Catalog catalog) {
        this.dorisCatalog = dorisCatalog;
        this.catalog = catalog;
        nsCatalog = (SupportsNamespaces) catalog;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public IcebergExternalCatalog getExternalCatalog() {
        return dorisCatalog;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return catalog.tableExists(TableIdentifier.of(dbName, tblName));
    }

    public boolean databaseExist(String dbName) {
        return nsCatalog.namespaceExists(Namespace.of(dbName));
    }

    public List<String> listDatabaseNames() {
        return nsCatalog.listNamespaces().stream()
                .map(e -> e.toString())
                .collect(Collectors.toList());
    }


    @Override
    public List<String> listTableNames(String dbName) {
        List<TableIdentifier> tableIdentifiers = catalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        String dbName = stmt.getFullDbName();
        Map<String, String> properties = stmt.getProperties();
        if (databaseExist(dbName)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create database[{}] which already exists", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
            }
        }
        nsCatalog.createNamespace(Namespace.of(dbName), properties);
        dorisCatalog.onRefresh(true);
    }

    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        if (!databaseExist(dbName)) {
            if (stmt.isSetIfExists()) {
                LOG.info("drop database[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        nsCatalog.dropNamespace(Namespace.of(dbName));
        dorisCatalog.onRefresh(true);
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        String dbName = stmt.getDbName();
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + dorisCatalog.getName());
        }
        String tableName = stmt.getTableName();
        if (tableExist(dbName, tableName)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        List<Column> columns = stmt.getColumns();
        List<StructField> collect = columns.stream()
                .map(col -> new StructField(col.getName(), col.getType(), col.getComment(), col.isAllowNull()))
                .collect(Collectors.toList());
        StructType structType = new StructType(new ArrayList<>(collect));
        org.apache.iceberg.types.Type visit =
                DorisTypeVisitor.visit(structType, new DorisTypeToIcebergType(structType));
        Schema schema = new Schema(visit.asNestedType().asStructType().fields());
        Map<String, String> properties = stmt.getProperties();
        properties.put(ExternalCatalog.DORIS_VERSION, ExternalCatalog.DORIS_VERSION_VALUE);
        PartitionSpec partitionSpec = IcebergUtils.solveIcebergPartitionSpec(stmt.getPartitionDesc(), schema);
        catalog.createTable(TableIdentifier.of(dbName, tableName), schema, partitionSpec, properties);
        db.setUnInitialized(true);
        return false;
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + dorisCatalog.getName());
        }
        String tableName = stmt.getTableName();
        if (!tableExist(dbName, tableName)) {
            if (stmt.isSetIfExists()) {
                LOG.info("drop table[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, tableName, dbName);
            }
        }
        catalog.dropTable(TableIdentifier.of(dbName, tableName));
        db.setUnInitialized(true);
    }

    @Override
    public void truncateTable(String dbName, String tblName, List<String> partitions) {
        throw new UnsupportedOperationException("Truncate Iceberg table is not supported.");
    }
}
