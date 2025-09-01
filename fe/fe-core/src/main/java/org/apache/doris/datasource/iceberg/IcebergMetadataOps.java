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

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.DorisTypeVisitor;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.info.BranchOptions;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TagOptions;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

public class IcebergMetadataOps implements ExternalMetadataOps {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadataOps.class);
    protected Catalog catalog;
    protected ExternalCatalog dorisCatalog;
    protected SupportsNamespaces nsCatalog;
    private ExecutionAuthenticator executionAuthenticator;
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
        this.executionAuthenticator = dorisCatalog.getExecutionAuthenticator();

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
        if (catalog != null) {
            catalog = null;
        }
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        try {
            return executionAuthenticator.execute(() -> catalog.tableExists(getTableIdentifier(dbName, tblName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check table exist, error message is:" + e.getMessage(), e);
        }
    }

    public boolean databaseExist(String dbName) {
        try {
            return executionAuthenticator.execute(() -> nsCatalog.namespaceExists(getNamespace(dbName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check database exist, error message is:" + e.getMessage(), e);
        }
    }

    public List<String> listDatabaseNames() {
        try {
            return executionAuthenticator.execute(() -> nsCatalog.listNamespaces(getNamespace())
                   .stream()
                   .map(n -> n.level(n.length() - 1))
                   .collect(Collectors.toList()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to list database names, error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            return executionAuthenticator.execute(() -> {
                List<TableIdentifier> tableIdentifiers = catalog.listTables(getNamespace(dbName));
                List<String> views;
                // Our original intention was simply to clearly define the responsibilities of ViewCatalog and Catalog.
                // IcebergMetadataOps handles listTableNames and listViewNames separately.
                // listTableNames should only focus on the table type,
                // but in reality, Iceberg's return includes views. Therefore, we added a filter to exclude views.
                if (catalog instanceof ViewCatalog) {
                    views = ((ViewCatalog) catalog).listViews(getNamespace(dbName))
                        .stream().map(TableIdentifier::name).collect(Collectors.toList());
                } else {
                    views = Collections.emptyList();
                }
                if (views.isEmpty()) {
                    return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
                } else {
                    return tableIdentifiers.stream()
                        .map(TableIdentifier::name)
                        .filter(name -> !views.contains(name)).collect(Collectors.toList());
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to list table names, error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException {
        try {
            return executionAuthenticator.execute(() -> performCreateDb(dbName, ifNotExists, properties));
        } catch (Exception e) {
            throw new DdlException("Failed to create database: "
                    + dbName + ": " + Util.getRootCauseMessage(e), e);
        }
    }

    @Override
    public void afterCreateDb() {
        dorisCatalog.resetMetaCacheNames();
    }

    private boolean performCreateDb(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException {
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        if (databaseExist(dbName)) {
            if (ifNotExists) {
                LOG.info("create database[{}] which already exists", dbName);
                return true;
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
        return false;
    }

    @Override
    public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
        try {
            executionAuthenticator.execute(() -> {
                preformDropDb(dbName, ifExists, force);
                return null;
            });
        } catch (Exception e) {
            throw new DdlException(
                "Failed to drop database: " + dbName + ", error message is:" + e.getMessage(), e);
        }
    }

    private void preformDropDb(String dbName, boolean ifExists, boolean force) throws DdlException {
        ExternalDatabase dorisDb = dorisCatalog.getDbNullable(dbName);
        if (dorisDb == null) {
            if (ifExists) {
                LOG.info("drop database[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }
        if (force) {
            // try to drop all tables in the database
            List<String> remoteTableNames = listTableNames(dorisDb.getRemoteName());
            for (String remoteTableName : remoteTableNames) {
                performDropTable(dorisDb.getRemoteName(), remoteTableName, true);
            }
            if (!remoteTableNames.isEmpty()) {
                LOG.info("drop database[{}] with force, drop all tables, num: {}", dbName, remoteTableNames.size());
            }
            // try to drop all views in the database
            List<String> remoteViewNames = listViewNames(dorisDb.getRemoteName());
            for (String remoteViewName : remoteViewNames) {
                performDropView(dorisDb.getRemoteName(), remoteViewName);
            }
            if (!remoteViewNames.isEmpty()) {
                LOG.info("drop database[{}] with force, drop all views, num: {}", dbName, remoteViewNames.size());
            }
        }
        nsCatalog.dropNamespace(getNamespace(dorisDb.getRemoteName()));
    }

    @Override
    public void afterDropDb(String dbName) {
        dorisCatalog.unregisterDatabase(dbName);
    }

    @Override
    public boolean createTableImpl(CreateMTMVInfo createMTMVInfo) throws UserException {
        try {
            return executionAuthenticator.execute(() -> performCreateTable(createMTMVInfo));
        } catch (Exception e) {
            throw new DdlException(
                "Failed to create table: " + createMTMVInfo.getTableName() + ", error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        try {
            return executionAuthenticator.execute(() -> performCreateTable(createTableInfo));
        } catch (Exception e) {
            throw new DdlException(
                "Failed to create table: " + createTableInfo.getTableName() + ", error message is:" + e.getMessage(),
                    e);
        }
    }

    public boolean performCreateTable(CreateMTMVInfo createMTMVInfo) throws UserException {
        String dbName = createMTMVInfo.getDbName();
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + dorisCatalog.getName());
        }
        String tableName = createMTMVInfo.getTableName();
        // 1. first, check if table exist in remote
        if (tableExist(db.getRemoteName(), tableName)) {
            if (createMTMVInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        // 2. second, check fi table exist in local.
        // This is because case sensibility issue, eg:
        // 1. lower_case_table_name = 1
        // 2. create table tbl1;
        // 3. create table TBL1;  TBL1 does not exist in remote because the remote system is case-sensitive.
        //    but because lower_case_table_name = 1, the table can not be created in Doris because it is conflict with
        //    tbl1
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable != null) {
            if (createMTMVInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        List<Column> columns = createMTMVInfo.getColumns();
        List<StructField> collect = columns.stream()
                .map(col -> new StructField(col.getName(), col.getType(), col.getComment(), col.isAllowNull()))
                .collect(Collectors.toList());
        StructType structType = new StructType(new ArrayList<>(collect));
        Type visit =
                DorisTypeVisitor.visit(structType, new DorisTypeToIcebergType(structType));
        Schema schema = new Schema(visit.asNestedType().asStructType().fields());
        Map<String, String> properties = createMTMVInfo.getProperties();
        properties.put(ExternalCatalog.DORIS_VERSION, ExternalCatalog.DORIS_VERSION_VALUE);
        PartitionSpec partitionSpec = IcebergUtils.solveIcebergPartitionSpec(createMTMVInfo.getPartitionDesc(), schema);
        catalog.createTable(getTableIdentifier(dbName, tableName), schema, partitionSpec, properties);
        return false;
    }

    public boolean performCreateTable(CreateTableInfo createTableInfo) throws UserException {
        String dbName = createTableInfo.getDbName();
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + dorisCatalog.getName());
        }
        String tableName = createTableInfo.getTableName();
        // 1. first, check if table exist in remote
        if (tableExist(db.getRemoteName(), tableName)) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        // 2. second, check fi table exist in local.
        // This is because case sensibility issue, eg:
        // 1. lower_case_table_name = 1
        // 2. create table tbl1;
        // 3. create table TBL1;  TBL1 does not exist in remote because the remote system is case-sensitive.
        //    but because lower_case_table_name = 1, the table can not be created in Doris because it is conflict with
        //    tbl1
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable != null) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        List<Column> columns = createTableInfo.getColumns();
        List<StructField> collect = columns.stream()
                .map(col -> new StructField(col.getName(), col.getType(), col.getComment(), col.isAllowNull()))
                .collect(Collectors.toList());
        StructType structType = new StructType(new ArrayList<>(collect));
        Type visit =
                DorisTypeVisitor.visit(structType, new DorisTypeToIcebergType(structType));
        Schema schema = new Schema(visit.asNestedType().asStructType().fields());
        Map<String, String> properties = createTableInfo.getProperties();
        properties.put(ExternalCatalog.DORIS_VERSION, ExternalCatalog.DORIS_VERSION_VALUE);
        PartitionSpec partitionSpec = IcebergUtils.solveIcebergPartitionSpec(createTableInfo.getPartitionDesc(),
                schema);
        catalog.createTable(getTableIdentifier(dbName, tableName), schema, partitionSpec, properties);
        return false;
    }

    @Override
    public boolean createTableImpl(CreateTableStmt stmt) throws UserException {
        try {
            return executionAuthenticator.execute(() -> performCreateTable(stmt));
        } catch (Exception e) {
            throw new DdlException(
                "Failed to create table: " + stmt.getTableName() + ", error message is:" + e.getMessage(), e);
        }
    }

    public boolean performCreateTable(CreateTableStmt stmt) throws UserException {
        String dbName = stmt.getDbName();
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + dorisCatalog.getName());
        }
        String tableName = stmt.getTableName();
        // 1. first, check if table exist in remote
        if (tableExist(db.getRemoteName(), tableName)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        // 2. second, check fi table exist in local.
        // This is because case sensibility issue, eg:
        // 1. lower_case_table_name = 1
        // 2. create table tbl1;
        // 3. create table TBL1;  TBL1 does not exist in remote because the remote system is case-sensitive.
        //    but because lower_case_table_name = 1, the table can not be created in Doris because it is conflict with
        //    tbl1
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable != null) {
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
        Type visit =
                DorisTypeVisitor.visit(structType, new DorisTypeToIcebergType(structType));
        Schema schema = new Schema(visit.asNestedType().asStructType().fields());
        Map<String, String> properties = stmt.getProperties();
        properties.put(ExternalCatalog.DORIS_VERSION, ExternalCatalog.DORIS_VERSION_VALUE);
        PartitionSpec partitionSpec = IcebergUtils.solveIcebergPartitionSpec(stmt.getPartitionDesc(), schema);
        catalog.createTable(getTableIdentifier(dbName, tableName), schema, partitionSpec, properties);
        return false;
    }

    @Override
    public void afterCreateTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().resetMetaCacheNames();
        }
        LOG.info("after create table {}.{}.{}, is db exists: {}",
                dorisCatalog.getName(), dbName, tblName, db.isPresent());
    }

    @Override
    public void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        try {
            executionAuthenticator.execute(() -> {
                if (getExternalCatalog().getMetadataOps()
                        .viewExists(dorisTable.getRemoteDbName(), dorisTable.getRemoteName())) {
                    performDropView(dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
                } else {
                    performDropTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(), ifExists);
                }
                return null;
            });
        } catch (Exception e) {
            throw new DdlException(
                    "Failed to drop table: " + dorisTable.getName() + ", error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public void afterDropTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().unregisterTable(tblName);
        }
        LOG.info("after drop table {}.{}.{}. is db exists: {}",
                dorisCatalog.getName(), dbName, tblName, db.isPresent());
    }

    private void performDropTable(String remoteDbName, String remoteTblName, boolean ifExists) throws DdlException {
        if (!tableExist(remoteDbName, remoteTblName)) {
            if (ifExists) {
                LOG.info("drop table[{}] which does not exist", remoteTblName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, remoteTblName, remoteDbName);
            }
        }
        catalog.dropTable(getTableIdentifier(remoteDbName, remoteTblName), true);
    }

    public void renameTableImpl(String dbName, String tblName, String newTblName) throws DdlException {
        try {
            executionAuthenticator.execute(() -> {
                catalog.renameTable(getTableIdentifier(dbName, tblName), getTableIdentifier(dbName, newTblName));
                return null;
            });
        } catch (Exception e) {
            throw new DdlException(
                    "Failed to rename table: " + tblName + " to " + newTblName + ", error message is:" + e.getMessage(),
                    e);
        }
    }

    @Override
    public void afterRenameTable(String dbName, String oldName, String newName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().unregisterTable(oldName);
            db.get().resetMetaCacheNames();
        }
        LOG.info("after rename table {}.{}.{} to {}, is db exists: {}",
                dorisCatalog.getName(), dbName, oldName, newName, db.isPresent());
    }

    @Override
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) {
        throw new UnsupportedOperationException("Truncate Iceberg table is not supported.");
    }

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        BranchOptions branchOptions = branchInfo.getBranchOptions();

        Long snapshotId = branchOptions.getSnapshotId()
                .orElse(
                        // use current snapshot
                        Optional.ofNullable(icebergTable.currentSnapshot()).map(Snapshot::snapshotId).orElse(null));

        ManageSnapshots manageSnapshots;
        try {
            manageSnapshots = executionAuthenticator.execute(icebergTable::manageSnapshots);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create ManageSnapshots for table: " + icebergTable.name()
                            + ", error message is: {} " + ExceptionUtils.getRootCauseMessage(e), e);
        }
        String branchName = branchInfo.getBranchName();
        boolean refExists = null != icebergTable.refs().get(branchName);
        boolean create = branchInfo.getCreate();
        boolean replace = branchInfo.getReplace();
        boolean ifNotExists = branchInfo.getIfNotExists();
        Runnable safeCreateBranch = () -> {
            try {
                executionAuthenticator.execute(() -> {
                    if (snapshotId == null) {
                        manageSnapshots.createBranch(branchName);
                    } else {
                        manageSnapshots.createBranch(branchName, snapshotId);
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to create branch: " + branchName + " in table: " + icebergTable.name()
                                + ", error message is: {} " + ExceptionUtils.getRootCauseMessage(e), e);
            }
        };

        if (create && replace && !refExists) {
            safeCreateBranch.run();
        } else if (replace) {
            if (snapshotId == null) {
                // Cannot perform a replace operation on an empty table
                throw new UserException(
                        "Cannot complete replace branch operation on " + icebergTable.name()
                                + " , main has no snapshot");
            }
            manageSnapshots.replaceBranch(branchName, snapshotId);
        } else {
            if (refExists && ifNotExists) {
                return;
            }
            safeCreateBranch.run();
        }

        branchOptions.getRetain().ifPresent(n -> manageSnapshots.setMaxSnapshotAgeMs(branchName, n));
        branchOptions.getNumSnapshots().ifPresent(n -> manageSnapshots.setMinSnapshotsToKeep(branchName, n));
        branchOptions.getRetention().ifPresent(n -> manageSnapshots.setMaxRefAgeMs(branchName, n));

        try {
            executionAuthenticator.execute(manageSnapshots::commit);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create or replace branch: " + branchName + " in table: " + icebergTable.name()
                            + ", error message is: " + e.getMessage(), e);
        }
    }

    @Override
    public void afterOperateOnBranchOrTag(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            Optional tbl = db.get().getTableForReplay(tblName);
            if (tbl.isPresent()) {
                Env.getCurrentEnv().getRefreshManager()
                        .refreshTableInternal(db.get(), (ExternalTable) tbl.get(),
                                System.currentTimeMillis());
            }
        }
    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        TagOptions tagOptions = tagInfo.getTagOptions();
        Long snapshotId = tagOptions.getSnapshotId()
                .orElse(
                        // use current snapshot
                        Optional.ofNullable(icebergTable.currentSnapshot()).map(Snapshot::snapshotId).orElse(null));

        if (snapshotId == null) {
            // Creating tag for empty tables is not allowed
            throw new UserException(
                    "Cannot complete replace branch operation on " + icebergTable.name() + " , main has no snapshot");
        }

        String tagName = tagInfo.getTagName();
        boolean create = tagInfo.getCreate();
        boolean replace = tagInfo.getReplace();
        boolean ifNotExists = tagInfo.getIfNotExists();
        boolean refExists = null != icebergTable.refs().get(tagName);


        try {
            executionAuthenticator.execute(() -> {
                ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
                if (create && replace && !refExists) {
                    manageSnapshots.createTag(tagName, snapshotId);
                } else if (replace) {
                    manageSnapshots.replaceTag(tagName, snapshotId);
                } else {
                    if (refExists && ifNotExists) {
                        return;
                    }
                    manageSnapshots.createTag(tagName, snapshotId);
                }
                tagOptions.getRetain().ifPresent(n -> manageSnapshots.setMaxRefAgeMs(tagName, n));
                manageSnapshots.commit();
            });
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create or replace tag: " + tagName + " in table: " + icebergTable.name()
                            + ", error message is: " + e.getMessage(), e);
        }
    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException {
        String tagName = tagInfo.getTagName();
        boolean ifExists = tagInfo.getIfExists();
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        SnapshotRef snapshotRef = icebergTable.refs().get(tagName);

        if (snapshotRef != null || !ifExists) {
            try {
                executionAuthenticator.execute(() -> {
                    ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
                    manageSnapshots.removeTag(tagName).commit();
                });
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to drop tag: " + tagName + " in table: " + icebergTable.name()
                                + ", error message is: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException {
        String branchName = branchInfo.getBranchName();
        boolean ifExists = branchInfo.getIfExists();
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        SnapshotRef snapshotRef = icebergTable.refs().get(branchName);

        if (snapshotRef != null || !ifExists) {
            try {
                executionAuthenticator.execute(() -> {
                    ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
                    manageSnapshots.removeBranch(branchName).commit();
                });
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to drop branch: " + branchName + " in table: " + icebergTable.name()
                                + ", error message is: " + e.getMessage(), e);
            }
        }
    }

    private void addOneColumn(UpdateSchema updateSchema, Column column) throws UserException {
        if (!column.isAllowNull()) {
            throw new UserException("can't add a non-nullable column to an Iceberg table");
        }
        org.apache.iceberg.types.Type dorisType = IcebergUtils.dorisTypeToIcebergType(column.getType());
        Literal<?> defaultValue = IcebergUtils.parseIcebergLiteral(column.getDefaultValue(), dorisType);
        updateSchema.addColumn(column.getName(), dorisType, column.getComment(), defaultValue);
    }

    private void applyPosition(UpdateSchema updateSchema, ColumnPosition position, String columnName) {
        if (position.isFirst()) {
            updateSchema.moveFirst(columnName);
        } else {
            updateSchema.moveAfter(columnName, position.getLastCol());
        }
    }

    private void refreshTable(ExternalTable dorisTable) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dorisTable.getRemoteDbName());
        if (db.isPresent()) {
            Optional<?> tbl = db.get().getTableForReplay(dorisTable.getRemoteName());
            if (tbl.isPresent()) {
                Env.getCurrentEnv().getRefreshManager()
                        .refreshTableInternal(db.get(), (ExternalTable) tbl.get(), System.currentTimeMillis());
            }
        }
    }

    @Override
    public void addColumn(ExternalTable dorisTable, Column column, ColumnPosition position)
            throws UserException {
        validateCommonColumnInfo(column);
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        UpdateSchema updateSchema = icebergTable.updateSchema();
        addOneColumn(updateSchema, column);
        if (position != null) {
            applyPosition(updateSchema, position, column.getName());
        }
        try {
            executionAuthenticator.execute(() -> updateSchema.commit());
        } catch (Exception e) {
            throw new UserException("Failed to add column: " + column.getName() + " to table: "
                    + icebergTable.name() + ", error message is: " + e.getMessage(), e);
        }
        refreshTable(dorisTable);
    }

    @Override
    public void addColumns(ExternalTable dorisTable, List<Column> columns) throws UserException {
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        UpdateSchema updateSchema = icebergTable.updateSchema();
        for (Column column : columns) {
            validateCommonColumnInfo(column);
            addOneColumn(updateSchema, column);
        }
        try {
            executionAuthenticator.execute(() -> updateSchema.commit());
        } catch (Exception e) {
            throw new UserException("Failed to add columns to table: " + icebergTable.name()
                    + ", error message is: " + e.getMessage(), e);
        }
        refreshTable(dorisTable);
    }

    @Override
    public void dropColumn(ExternalTable dorisTable, String columnName) throws UserException {
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        UpdateSchema updateSchema = icebergTable.updateSchema();
        updateSchema.deleteColumn(columnName);
        try {
            executionAuthenticator.execute(() -> updateSchema.commit());
        } catch (Exception e) {
            throw new UserException("Failed to drop column: " + columnName + " from table: "
                    + icebergTable.name() + ", error message is: " + e.getMessage(), e);
        }
        refreshTable(dorisTable);
    }

    @Override
    public void renameColumn(ExternalTable dorisTable, String oldName, String newName) throws UserException {
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        UpdateSchema updateSchema = icebergTable.updateSchema();
        updateSchema.renameColumn(oldName, newName);
        try {
            executionAuthenticator.execute(() -> updateSchema.commit());
        } catch (Exception e) {
            throw new UserException("Failed to rename column: " + oldName + " to " + newName
                    + " in table: " + icebergTable.name() + ", error message is: " + e.getMessage(), e);
        }
        refreshTable(dorisTable);
    }

    @Override
    public void modifyColumn(ExternalTable dorisTable, Column column, ColumnPosition position)
            throws UserException {
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        validateForModifyColumn(column, icebergTable);
        Type icebergType = IcebergUtils.dorisTypeToIcebergType(column.getType());
        UpdateSchema updateSchema = icebergTable.updateSchema();
        updateSchema.updateColumn(column.getName(), icebergType.asPrimitiveType(), column.getComment());
        if (column.isAllowNull()) {
            // we can change a required column to optional, but not the other way around
            // because we don't know whether there is existing data with null values.
            updateSchema.makeColumnOptional(column.getName());
        }
        if (position != null) {
            applyPosition(updateSchema, position, column.getName());
        }
        try {
            executionAuthenticator.execute(() -> updateSchema.commit());
        } catch (Exception e) {
            throw new UserException("Failed to modify column: " + column.getName() + " in table: "
                    + icebergTable.name() + ", error message is: " + e.getMessage(), e);
        }
        refreshTable(dorisTable);
    }

    private void validateForModifyColumn(Column column, Table icebergTable) throws UserException {
        validateCommonColumnInfo(column);
        // check complex type
        if (column.getType().isComplexType()) {
            throw new UserException("Modify column type to non-primitive type is not supported: " + column.getType());
        }
        // check exist
        NestedField currentCol = icebergTable.schema().findField(column.getName());
        if (currentCol == null) {
            throw new UserException("Column " + column.getName() + " does not exist");
        }
        // check nullable
        if (currentCol.isOptional() && !column.isAllowNull()) {
            throw new UserException("Can not change nullable column " + column.getName() + " to not null");
        }
    }

    private void validateCommonColumnInfo(Column column) throws UserException {
        // check aggregation method
        if (column.isAggregated()) {
            throw new UserException("Can not specify aggregation method for iceberg table column");
        }
        // check auto inc
        if (column.isAutoInc()) {
            throw new UserException("Can not specify auto incremental iceberg table column");
        }
    }

    @Override
    public void reorderColumns(ExternalTable dorisTable, List<String> newOrder) throws UserException {
        if (newOrder == null || newOrder.isEmpty()) {
            throw new UserException("Reorder column failed, new order is empty.");
        }
        Table icebergTable = IcebergUtils.getIcebergTable(dorisTable);
        UpdateSchema updateSchema = icebergTable.updateSchema();
        updateSchema.moveFirst(newOrder.get(0));
        for (int i = 1; i < newOrder.size(); i++) {
            updateSchema.moveAfter(newOrder.get(i), newOrder.get(i - 1));
        }
        try {
            executionAuthenticator.execute(() -> updateSchema.commit());
        } catch (Exception e) {
            throw new UserException("Failed to reorder columns in table: " + icebergTable.name()
                    + ", error message is: " + e.getMessage(), e);
        }
        refreshTable(dorisTable);
    }

    public ExecutionAuthenticator getExecutionAuthenticator() {
        return executionAuthenticator;
    }

    @Override
    public Table loadTable(String dbName, String tblName) {
        try {
            return executionAuthenticator.execute(() -> catalog.loadTable(getTableIdentifier(dbName, tblName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table, error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean viewExists(String remoteDbName, String remoteViewName) {
        if (!(catalog instanceof ViewCatalog)) {
            return false;
        }
        try {
            return executionAuthenticator.execute(() ->
                    ((ViewCatalog) catalog).viewExists(getTableIdentifier(remoteDbName, remoteViewName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check view exist, error message is:" + e.getMessage(), e);

        }
    }

    @Override
    public View loadView(String dbName, String tblName) {
        if (!(catalog instanceof ViewCatalog)) {
            return null;
        }
        try {
            ViewCatalog viewCatalog = (ViewCatalog) catalog;
            return executionAuthenticator.execute(() -> viewCatalog.loadView(TableIdentifier.of(dbName, tblName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load view, error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listViewNames(String db) {
        if (!(catalog instanceof ViewCatalog)) {
            return Collections.emptyList();
        }
        try {
            return executionAuthenticator.execute(() ->
                ((ViewCatalog) catalog).listViews(Namespace.of(db))
                    .stream().map(TableIdentifier::name).collect(Collectors.toList()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to list view names, error message is:" + e.getMessage(), e);
        }
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

    public ThreadPoolExecutor getThreadPoolWithPreAuth() {
        return dorisCatalog.getThreadPoolWithPreAuth();
    }

    private void performDropView(String remoteDbName, String remoteViewName) throws DdlException {
        if (!(catalog instanceof ViewCatalog)) {
            throw new DdlException("Drop Iceberg view is not supported with not view catalog.");
        }
        ViewCatalog viewCatalog = (ViewCatalog) catalog;
        viewCatalog.dropView(getTableIdentifier(remoteDbName, remoteViewName));
    }
}

