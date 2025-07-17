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

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
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
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.info.BranchOptions;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TagOptions;

import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.types.Type;
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
        if (catalog != null) {
            catalog = null;
        }
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        try {
            return preExecutionAuthenticator.execute(() -> catalog.tableExists(getTableIdentifier(dbName, tblName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check table exist, error message is:" + e.getMessage(), e);
        }
    }

    public boolean databaseExist(String dbName) {
        try {
            return preExecutionAuthenticator.execute(() -> nsCatalog.namespaceExists(getNamespace(dbName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check database exist, error message is:" + e.getMessage(), e);
        }
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
        try {
            return preExecutionAuthenticator.execute(() -> {
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
            return preExecutionAuthenticator.execute(() -> performCreateDb(dbName, ifNotExists, properties));
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
            preExecutionAuthenticator.execute(() -> {
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
    public boolean createTableImpl(CreateTableStmt stmt) throws UserException {
        try {
            return preExecutionAuthenticator.execute(() -> performCreateTable(stmt));
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
            preExecutionAuthenticator.execute(() -> {
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

        ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
        String branchName = branchInfo.getBranchName();
        boolean refExists = null != icebergTable.refs().get(branchName);
        boolean create = branchInfo.getCreate();
        boolean replace = branchInfo.getReplace();
        boolean ifNotExists = branchInfo.getIfNotExists();

        Runnable safeCreateBranch = () -> {
            if (snapshotId == null) {
                manageSnapshots.createBranch(branchName);
            } else {
                manageSnapshots.createBranch(branchName, snapshotId);
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
            preExecutionAuthenticator.execute(() -> manageSnapshots.commit());
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
        try {
            preExecutionAuthenticator.execute(() -> manageSnapshots.commit());
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
            ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
            try {
                preExecutionAuthenticator.execute(() -> manageSnapshots.removeTag(tagName).commit());
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
            ManageSnapshots manageSnapshots = icebergTable.manageSnapshots();
            try {
                preExecutionAuthenticator.execute(() -> manageSnapshots.removeBranch(branchName).commit());
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to drop branch: " + branchName + " in table: " + icebergTable.name()
                        + ", error message is: " + e.getMessage(), e);
            }
        }
    }

    public PreExecutionAuthenticator getPreExecutionAuthenticator() {
        return preExecutionAuthenticator;
    }

    @Override
    public Table loadTable(String dbName, String tblName) {
        try {
            return preExecutionAuthenticator.execute(() -> catalog.loadTable(getTableIdentifier(dbName, tblName)));
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
            return preExecutionAuthenticator.execute(() ->
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
            return preExecutionAuthenticator.execute(() -> viewCatalog.loadView(TableIdentifier.of(dbName, tblName)));
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
            return preExecutionAuthenticator.execute(() ->
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

