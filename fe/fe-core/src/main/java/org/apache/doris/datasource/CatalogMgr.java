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

package org.apache.doris.datasource;

import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.ShowCatalogStmt;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CatalogMgr will load all catalogs at FE startup,
 * and save them in map with name.
 * Note: Catalog in sql syntax will be treated as catalog interface in code level.
 * TODO: Change the package name into catalog.
 */
public class CatalogMgr implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(CatalogMgr.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "idToCatalog")
    private final Map<Long, CatalogIf> idToCatalog = Maps.newConcurrentMap();
    // this map will be regenerated from idToCatalog, so not need to persist.
    private final Map<String, CatalogIf> nameToCatalog = Maps.newConcurrentMap();

    // Use a separate instance to facilitate access.
    // internalDataSource still exists in idToDataSource and nameToDataSource
    private InternalCatalog internalCatalog;

    public CatalogMgr() {
        initInternalCatalog();
    }

    private void initInternalCatalog() {
        internalCatalog = new InternalCatalog();
        addCatalog(internalCatalog);
    }

    private void addCatalog(CatalogIf catalog) {
        nameToCatalog.put(catalog.getName(), catalog);
        idToCatalog.put(catalog.getId(), catalog);
    }

    private CatalogIf removeCatalog(long catalogId) {
        CatalogIf catalog = idToCatalog.remove(catalogId);
        if (catalog != null) {
            nameToCatalog.remove(catalog.getName());
        }
        return catalog;
    }

    private void unprotectedRefreshCatalog(long catalogId) {
        CatalogIf catalog = idToCatalog.get(catalogId);
        if (catalog != null) {
            String catalogName = catalog.getName();
            if (!catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
                ((ExternalCatalog) catalog).setUninitialized();
            }
        }
    }

    public InternalCatalog getInternalCatalog() {
        return internalCatalog;
    }

    public CatalogIf getCatalog(String name) {
        return nameToCatalog.get(name);
    }

    public CatalogIf getCatalog(long id) {
        return idToCatalog.get(id);
    }

    public <E extends Exception> CatalogIf getCatalogOrException(String name, Function<String, E> e) throws E {
        CatalogIf catalog = nameToCatalog.get(name);
        if (catalog == null) {
            throw e.apply(name);
        }
        return catalog;
    }

    public CatalogIf getCatalogOrAnalysisException(String name) throws AnalysisException {
        return getCatalogOrException(name,
                catalog -> new AnalysisException(ErrorCode.ERR_UNKNOWN_CATALOG.formatErrorMsg(catalog),
                        ErrorCode.ERR_UNKNOWN_CATALOG));
    }

    public List<Long> getCatalogIds() {
        return Lists.newArrayList(idToCatalog.keySet());
    }

    /**
     * Get db allow return null
     **/
    public DatabaseIf getDbNullable(long dbId) {
        DatabaseIf db = internalCatalog.getDbNullable(dbId);
        if (db != null) {
            return db;
        }
        for (CatalogIf catalog : nameToCatalog.values()) {
            if (catalog == internalCatalog) {
                continue;
            }
            db = catalog.getDbNullable(dbId);
            if (db != null) {
                return db;
            }
        }
        return null;
    }

    public List<Long> getDbIds() {
        List<Long> dbIds = Lists.newArrayList();
        for (CatalogIf catalog : nameToCatalog.values()) {
            dbIds.addAll(catalog.getDbIds());
        }
        return dbIds;
    }

    public List<String> getDbNames() {
        List<String> dbNames = Lists.newArrayList();
        for (CatalogIf catalog : nameToCatalog.values()) {
            dbNames.addAll(catalog.getDbNames());
        }
        return dbNames;
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    /**
     * Create and hold the catalog instance and write the meta log.
     */
    public void createCatalog(CreateCatalogStmt stmt) throws UserException {
        writeLock();
        try {
            if (stmt.isSetIfNotExists() && nameToCatalog.containsKey(stmt.getCatalogName())) {
                LOG.warn("Catalog {} is already exist.", stmt.getCatalogName());
                return;
            }
            if (nameToCatalog.containsKey(stmt.getCatalogName())) {
                throw new DdlException("Catalog had already exist with name: " + stmt.getCatalogName());
            }
            long id = Env.getCurrentEnv().getNextId();
            CatalogLog log = CatalogFactory.constructorCatalogLog(id, stmt);
            replayCreateCatalog(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_CREATE_CATALOG, log);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove the catalog instance by name and write the meta log.
     */
    public void dropCatalog(DropCatalogStmt stmt) throws UserException {
        writeLock();
        try {
            if (stmt.isSetIfExists() && !nameToCatalog.containsKey(stmt.getCatalogName())) {
                LOG.warn("Non catalog {} is found.", stmt.getCatalogName());
                return;
            }
            CatalogIf catalog = nameToCatalog.get(stmt.getCatalogName());
            if (catalog == null) {
                throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
            }
            CatalogLog log = CatalogFactory.constructorCatalogLog(catalog.getId(), stmt);
            replayDropCatalog(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_DROP_CATALOG, log);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Modify the catalog name into a new one and write the meta log.
     */
    public void alterCatalogName(AlterCatalogNameStmt stmt) throws UserException {
        writeLock();
        try {
            CatalogIf catalog = nameToCatalog.get(stmt.getCatalogName());
            if (catalog == null) {
                throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
            }
            CatalogLog log = CatalogFactory.constructorCatalogLog(catalog.getId(), stmt);
            replayAlterCatalogName(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_ALTER_CATALOG_NAME, log);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Modify the catalog property and write the meta log.
     */
    public void alterCatalogProps(AlterCatalogPropertyStmt stmt) throws UserException {
        writeLock();
        try {
            CatalogIf catalog = nameToCatalog.get(stmt.getCatalogName());
            if (catalog == null) {
                throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
            }
            if (!catalog.getType().equalsIgnoreCase(stmt.getNewProperties().get("type"))) {
                throw new DdlException("Can't modify the type of catalog property with name: " + stmt.getCatalogName());
            }
            CatalogLog log = CatalogFactory.constructorCatalogLog(catalog.getId(), stmt);
            replayAlterCatalogProps(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_ALTER_CATALOG_PROPS, log);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Get catalog, or null if not exists.
     */
    public CatalogIf getCatalogNullable(String catalogName) {
        return nameToCatalog.get(catalogName);
    }

    /**
     * List all catalog or get the special catalog with a name.
     */
    public ShowResultSet showCatalogs(ShowCatalogStmt showStmt) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        readLock();
        try {
            if (showStmt.getCatalogName() == null) {
                for (CatalogIf catalog : nameToCatalog.values()) {
                    if (Env.getCurrentEnv().getAuth()
                            .checkCtlPriv(ConnectContext.get(), catalog.getName(), PrivPredicate.SHOW)) {
                        List<String> row = Lists.newArrayList();
                        row.add(String.valueOf(catalog.getId()));
                        row.add(catalog.getName());
                        row.add(catalog.getType());
                        rows.add(row);
                    }
                }
            } else {
                if (!nameToCatalog.containsKey(showStmt.getCatalogName())) {
                    throw new AnalysisException("No catalog found with name: " + showStmt.getCatalogName());
                }
                CatalogIf<DatabaseIf> catalog = nameToCatalog.get(showStmt.getCatalogName());
                if (!Env.getCurrentEnv().getAuth()
                        .checkCtlPriv(ConnectContext.get(), catalog.getName(), PrivPredicate.SHOW)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                            ConnectContext.get().getQualifiedUser(), catalog.getName());
                }
                for (Map.Entry<String, String> elem : catalog.getProperties().entrySet()) {
                    List<String> row = Lists.newArrayList();
                    row.add(elem.getKey());
                    row.add(elem.getValue());
                    rows.add(row);
                }
            }
        } finally {
            readUnlock();
        }

        return new ShowResultSet(showStmt.getMetaData(), rows);
    }

    /**
     * Refresh the catalog meta and write the meta log.
     */
    public void refreshCatalog(RefreshCatalogStmt stmt) throws UserException {
        writeLock();
        try {
            CatalogIf catalog = nameToCatalog.get(stmt.getCatalogName());
            if (catalog == null) {
                throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
            }
            CatalogLog log = CatalogFactory.constructorCatalogLog(catalog.getId(), stmt);
            replayRefreshCatalog(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_REFRESH_CATALOG, log);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Reply for create catalog event.
     */
    public void replayCreateCatalog(CatalogLog log) throws DdlException {
        writeLock();
        try {
            CatalogIf catalog = CatalogFactory.constructorFromLog(log);
            addCatalog(catalog);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Reply for drop catalog event.
     */
    public void replayDropCatalog(CatalogLog log) {
        writeLock();
        try {
            removeCatalog(log.getCatalogId());
        } finally {
            writeUnlock();
        }
    }

    /**
     * Reply for refresh catalog event.
     */
    public void replayRefreshCatalog(CatalogLog log) throws DdlException {
        writeLock();
        try {
            unprotectedRefreshCatalog(log.getCatalogId());
        } finally {
            writeUnlock();
        }
    }

    /**
     * Reply for alter catalog name event.
     */
    public void replayAlterCatalogName(CatalogLog log) {
        writeLock();
        try {
            CatalogIf catalog = removeCatalog(log.getCatalogId());
            catalog.modifyCatalogName(log.getNewCatalogName());
            addCatalog(catalog);
        } finally {
            writeUnlock();
        }
    }

    public List<CatalogIf> listCatalogs() {
        return nameToCatalog.values().stream().collect(Collectors.toList());
    }

    /**
     * Reply for alter catalog props event.
     */
    public void replayAlterCatalogProps(CatalogLog log) {
        writeLock();
        try {
            CatalogIf catalog = idToCatalog.get(log.getCatalogId());
            catalog.modifyCatalogProps(log.getNewProps());
        } finally {
            writeUnlock();
        }
    }

    public void replayInitCatalog(InitCatalogLog log) {
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        Preconditions.checkArgument(catalog != null);
        catalog.replayInitCatalog(log);
    }

    public void replayInitExternalDb(InitDatabaseLog log) {
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        Preconditions.checkArgument(catalog != null);
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        Preconditions.checkArgument(db != null);
        db.replayInitDb(log, catalog);
    }

    public void replayInitExternalTable(InitTableLog log) {
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        Preconditions.checkArgument(catalog != null);
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        Preconditions.checkArgument(db != null);
        ExternalTable table = db.getTableForReplay(log.getTableId());
        Preconditions.checkArgument(table != null);
        table.replayInitTable(log.getSchema());
    }

    public void replayRefreshExternalDb(ExternalObjectLog log) {
        writeLock();
        try {
            ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
            ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
            db.setUnInitialized();
        } finally {
            writeUnlock();
        }
    }

    public void replayRefreshExternalTable(ExternalObjectLog log) {
        writeLock();
        try {
            ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
            ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
            ExternalTable table = db.getTableForReplay(log.getTableId());
            table.setUnInitialized();
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static CatalogMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CatalogMgr.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (CatalogIf catalog : idToCatalog.values()) {
            nameToCatalog.put(catalog.getName(), catalog);
        }
        internalCatalog = (InternalCatalog) idToCatalog.get(InternalCatalog.INTERNAL_DS_ID);
    }
}
