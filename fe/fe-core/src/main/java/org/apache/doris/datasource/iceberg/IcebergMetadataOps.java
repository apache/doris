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
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.DorisTypeVisitor;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.operations.ExternalMetadataOps;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IcebergMetadataOps implements ExternalMetadataOps {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadataOps.class);
    protected Catalog catalog;
    protected ExternalCatalog dorisCatalog;
    protected SupportsNamespaces nsCatalog;
    private PreExecutionAuthenticator preExecutionAuthenticator;
    // Generally, there should be only two levels under the catalog, namely <database>.<table>,
    // but the REST type catalog is obtained from an external server,
    // and the level provided by the external server may be three levels, <catalog>.<database>.<table>.
    // Therefore, if the external server provides a catalog,
    // the catalog needs to be recorded here to ensure semantic consistency.
    private Optional<String> externalCatalogName = Optional.empty();

    public IcebergMetadataOps(ExternalCatalog dorisCatalog, Catalog catalog) {
        this.dorisCatalog = dorisCatalog;
        this.catalog = catalog;
        nsCatalog = (SupportsNamespaces) catalog;
        this.preExecutionAuthenticator = dorisCatalog.getPreExecutionAuthenticator();
        if (dorisCatalog.getProperties().containsKey(IcebergExternalCatalog.EXTERNAL_CATALOG_NAME)) {
            externalCatalogName =
                Optional.of(dorisCatalog.getProperties().get(IcebergExternalCatalog.EXTERNAL_CATALOG_NAME));
        }
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public ExternalCatalog getExternalCatalog() {
        return dorisCatalog;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return catalog.tableExists(getTableIdentifier(dbName, tblName));
    }

    public boolean databaseExist(String dbName) {
        return nsCatalog.namespaceExists(getNamespace(dbName));
    }

    public List<String> listDatabaseNames() {
        try {
            return preExecutionAuthenticator.execute(() -> nsCatalog.listNamespaces(getNamespace())
                   .stream()
                   .map(n -> n.level(n.length() - 1))
                   .collect(Collectors.toList()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to list database names, error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        List<TableIdentifier> tableIdentifiers = catalog.listTables(getNamespace(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        try {
            preExecutionAuthenticator.execute(() -> {
                performCreateDb(stmt);
                return null;

            });
        } catch (Exception e) {
            throw new DdlException("Failed to create database: "
                    + stmt.getFullDbName() + ": " + Util.getRootCauseMessage(e), e);
        }
    }

    private void performCreateDb(CreateDbStmt stmt) throws DdlException {
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
        if (!properties.isEmpty() && dorisCatalog instanceof IcebergExternalCatalog) {
            String icebergCatalogType = ((IcebergExternalCatalog) dorisCatalog).getIcebergCatalogType();
            if (!IcebergExternalCatalog.ICEBERG_HMS.equals(icebergCatalogType)) {
                throw new DdlException(
                    "Not supported: create database with properties for iceberg catalog type: " + icebergCatalogType);
            }
        }
        nsCatalog.createNamespace(getNamespace(dbName), properties);
        dorisCatalog.onRefreshCache(true);
    }

    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        try {
            preExecutionAuthenticator.execute(() -> {
                preformDropDb(stmt);
                return null;
            });
        } catch (Exception e) {
            throw new DdlException(
                "Failed to drop database: " + stmt.getDbName() + ", error message is:" + e.getMessage(), e);
        }
    }

    private void preformDropDb(DropDbStmt stmt) throws DdlException {
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
        nsCatalog.dropNamespace(getNamespace(dbName));
        dorisCatalog.onRefreshCache(true);
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        try {
            preExecutionAuthenticator.execute(() -> performCreateTable(stmt));
        } catch (Exception e) {
            throw new DdlException(
                "Failed to create table: " + stmt.getTableName() + ", error message is:" + e.getMessage(), e);
        }
        return false;
    }

    public boolean performCreateTable(CreateTableStmt stmt) throws UserException {
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
        catalog.createTable(getTableIdentifier(dbName, tableName), schema, partitionSpec, properties);
        db.setUnInitialized(true);
        return false;
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        try {
            preExecutionAuthenticator.execute(() -> {
                performDropTable(stmt);
                return null;
            });
        } catch (Exception e) {
            throw new DdlException(
                "Failed to drop table: " + stmt.getTableName() + ", error message is:" + e.getMessage(), e);
        }
    }

    private void performDropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            if (stmt.isSetIfExists()) {
                LOG.info("database [{}] does not exist when drop table[{}]", dbName, tableName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
        }

        if (!tableExist(dbName, tableName)) {
            if (stmt.isSetIfExists()) {
                LOG.info("drop table[{}] which does not exist", tableName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, tableName, dbName);
            }
        }
        catalog.dropTable(getTableIdentifier(dbName, tableName), true);
        db.setUnInitialized(true);
    }

    @Override
    public void truncateTable(String dbName, String tblName, List<String> partitions) {
        throw new UnsupportedOperationException("Truncate Iceberg table is not supported.");
    }

    public PreExecutionAuthenticator getPreExecutionAuthenticator() {
        return preExecutionAuthenticator;
    }

    @Override
    public Table loadTable(String dbName, String tblName) {
        return catalog.loadTable(getTableIdentifier(dbName, tblName));
    }

    private TableIdentifier getTableIdentifier(String dbName, String tblName) {
        return externalCatalogName
            .map(s -> TableIdentifier.of(s, dbName, tblName))
            .orElseGet(() -> TableIdentifier.of(dbName, tblName));
    }

    private Namespace getNamespace(String dbName) {
        return externalCatalogName
            .map(s -> Namespace.of(s, dbName))
            .orElseGet(() -> Namespace.of(dbName));
    }

    private Namespace getNamespace() {
        return externalCatalogName.map(Namespace::of).orElseGet(() -> Namespace.empty());
    }
}
