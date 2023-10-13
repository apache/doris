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

import org.apache.doris.analysis.AlterCatalogCommentStmt;
import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.ShowCatalogStmt;
import org.apache.doris.analysis.ShowCreateCatalogStmt;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Resource.ReferenceType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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

    public static final String ACCESS_CONTROLLER_CLASS_PROP = "access_controller.class";
    public static final String ACCESS_CONTROLLER_PROPERTY_PREFIX_PROP = "access_controller.properties.";
    public static final String METADATA_REFRESH_INTERVAL_SEC = "metadata_refresh_interval_sec";
    public static final String CATALOG_TYPE_PROP = "type";

    private static final String YES = "yes";

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "idToCatalog")
    private final Map<Long, CatalogIf> idToCatalog = Maps.newConcurrentMap();
    // this map will be regenerated from idToCatalog, so not need to persist.
    private final Map<String, CatalogIf> nameToCatalog = Maps.newConcurrentMap();
    // record last used database of every catalog
    private final Map<String, String> lastDBOfCatalog = Maps.newConcurrentMap();

    // Use a separate instance to facilitate access.
    // internalDataSource still exists in idToCatalog and nameToCatalog
    private InternalCatalog internalCatalog;

    public CatalogMgr() {
        initInternalCatalog();
    }

    public static CatalogMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CatalogMgr.class);
    }

    private void initInternalCatalog() {
        internalCatalog = new InternalCatalog();
        addCatalog(internalCatalog);
    }

    private void addCatalog(CatalogIf catalog) {
        nameToCatalog.put(catalog.getName(), catalog);
        idToCatalog.put(catalog.getId(), catalog);
        if (!Strings.isNullOrEmpty(catalog.getResource())) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(catalog.getResource());
            if (resource != null) {
                resource.addReference(catalog.getName(), ReferenceType.CATALOG);
            }
        }
    }

    private CatalogIf removeCatalog(long catalogId) {
        CatalogIf catalog = idToCatalog.remove(catalogId);
        if (catalog != null) {
            catalog.onClose();
            nameToCatalog.remove(catalog.getName());
            lastDBOfCatalog.remove(catalog.getName());
            Env.getCurrentEnv().getExtMetaCacheMgr().removeCache(catalog.getId());
            if (!Strings.isNullOrEmpty(catalog.getResource())) {
                Resource catalogResource = Env.getCurrentEnv().getResourceMgr().getResource(catalog.getResource());
                if (catalogResource != null) {
                    catalogResource.removeReference(catalog.getName(), ReferenceType.CATALOG);
                }
            }
            Env.getCurrentEnv().getQueryStats().clear(catalog.getId());
        }
        return catalog;
    }

    private void unprotectedRefreshCatalog(long catalogId, boolean invalidCache) {
        CatalogIf catalog = idToCatalog.get(catalogId);
        if (catalog != null) {
            String catalogName = catalog.getName();
            if (!catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
                ((ExternalCatalog) catalog).setUninitialized(invalidCache);
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

    public CatalogIf getCatalogOrAnalysisException(long id) throws AnalysisException {
        return getCatalogOrException(id,
                catalog -> new AnalysisException(ErrorCode.ERR_UNKNOWN_CATALOG.formatErrorMsg(catalog),
                        ErrorCode.ERR_UNKNOWN_CATALOG));
    }

    public <E extends Exception> CatalogIf getCatalogOrException(long id, Function<Long, E> e) throws E {
        CatalogIf catalog = idToCatalog.get(id);
        if (catalog == null) {
            throw e.apply(id);
        }
        return catalog;
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

    public void addLastDBOfCatalog(String catalog, String db) {
        lastDBOfCatalog.put(catalog, db);
    }

    public String getLastDB(String catalog) {
        return lastDBOfCatalog.get(catalog);
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
        long id = Env.getCurrentEnv().getNextId();
        CatalogIf catalog = CatalogFactory.createFromStmt(id, stmt);
        writeLock();
        try {
            if (nameToCatalog.containsKey(catalog.getName())) {
                if (stmt.isSetIfNotExists()) {
                    LOG.warn("Catalog {} is already exist.", stmt.getCatalogName());
                    return;
                }
                throw new DdlException("Catalog had already exist with name: " + stmt.getCatalogName());
            }
            createCatalogInternal(catalog, false);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_CREATE_CATALOG, catalog.constructEditLog());
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
            CatalogIf<DatabaseIf<TableIf>> catalog = nameToCatalog.get(stmt.getCatalogName());
            if (catalog == null) {
                throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
            }
            CatalogLog log = CatalogFactory.createCatalogLog(catalog.getId(), stmt);
            replayDropCatalog(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_DROP_CATALOG, log);

            lastDBOfCatalog.remove(stmt.getCatalogName());
            Env.getCurrentEnv().getQueryStats().clear(catalog.getId());
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
            if (nameToCatalog.get(stmt.getNewCatalogName()) != null) {
                throw new DdlException("Catalog with name " + stmt.getNewCatalogName() + " already exist");
            }
            CatalogLog log = CatalogFactory.createCatalogLog(catalog.getId(), stmt);
            replayAlterCatalogName(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_ALTER_CATALOG_NAME, log);

            String db = lastDBOfCatalog.get(stmt.getCatalogName());
            if (db != null) {
                lastDBOfCatalog.remove(stmt.getCatalogName());
                lastDBOfCatalog.put(log.getNewCatalogName(), db);
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Modify the catalog comment to a new one and write the meta log.
     */
    public void alterCatalogComment(AlterCatalogCommentStmt stmt) throws UserException {
        writeLock();
        try {
            CatalogIf catalog = nameToCatalog.get(stmt.getCatalogName());
            if (catalog == null) {
                throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
            }
            CatalogLog log = CatalogFactory.createCatalogLog(catalog.getId(), stmt);
            replayAlterCatalogComment(log);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_ALTER_CATALOG_COMMENT, log);
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
            Map<String, String> oldProperties = catalog.getProperties();
            if (catalog == null) {
                throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
            }
            if (stmt.getNewProperties().containsKey("type") && !catalog.getType()
                    .equalsIgnoreCase(stmt.getNewProperties().get("type"))) {
                throw new DdlException("Can't modify the type of catalog property with name: " + stmt.getCatalogName());
            }
            CatalogLog log = CatalogFactory.createCatalogLog(catalog.getId(), stmt);
            replayAlterCatalogProps(log, oldProperties, false);
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
        return showCatalogs(showStmt, InternalCatalog.INTERNAL_CATALOG_NAME);
    }

    public ShowResultSet showCatalogs(ShowCatalogStmt showStmt, String currentCtlg) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        readLock();
        try {
            if (showStmt.getCatalogName() == null) {
                PatternMatcher matcher = null;
                if (showStmt.getPattern() != null) {
                    matcher = PatternMatcherWrapper.createMysqlPattern(showStmt.getPattern(),
                            CaseSensibility.CATALOG.getCaseSensibility());
                }
                for (CatalogIf catalog : nameToCatalog.values()) {
                    if (Env.getCurrentEnv().getAccessManager()
                            .checkCtlPriv(ConnectContext.get(), catalog.getName(), PrivPredicate.SHOW)) {
                        String name = catalog.getName();
                        // Filter catalog name
                        if (matcher != null && !matcher.match(name)) {
                            continue;
                        }
                        List<String> row = Lists.newArrayList();
                        row.add(String.valueOf(catalog.getId()));
                        row.add(name);
                        row.add(catalog.getType());
                        if (name.equals(currentCtlg)) {
                            row.add(YES);
                        } else {
                            row.add("");
                        }
                        Map<String, String> props = catalog.getProperties();
                        String createTime = props.getOrDefault(CreateCatalogStmt.CREATE_TIME_PROP, "UNRECORDED");
                        row.add(createTime);
                        row.add(TimeUtils.longToTimeString(catalog.getLastUpdateTime()));
                        row.add(catalog.getComment());
                        rows.add(row);
                    }

                    // sort by catalog name
                    rows.sort((x, y) -> {
                        return x.get(1).compareTo(y.get(1));
                    });
                }
            } else {
                if (!nameToCatalog.containsKey(showStmt.getCatalogName())) {
                    throw new AnalysisException("No catalog found with name: " + showStmt.getCatalogName());
                }
                CatalogIf<DatabaseIf> catalog = nameToCatalog.get(showStmt.getCatalogName());
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkCtlPriv(ConnectContext.get(), catalog.getName(), PrivPredicate.SHOW)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                            ConnectContext.get().getQualifiedUser(), catalog.getName());
                }
                if (!Strings.isNullOrEmpty(catalog.getResource())) {
                    rows.add(Arrays.asList("resource", catalog.getResource()));
                }
                // use tree map to maintain display order, making it easier to view properties
                Map<String, String> sortedMap = new TreeMap<>(catalog.getProperties()).descendingMap();
                for (Map.Entry<String, String> elem : sortedMap.entrySet()) {
                    if (PrintableMap.HIDDEN_KEY.contains(elem.getKey())) {
                        continue;
                    }
                    if (PrintableMap.SENSITIVE_KEY.contains(elem.getKey())) {
                        rows.add(Arrays.asList(elem.getKey(), PrintableMap.PASSWORD_MASK));
                    } else {
                        rows.add(Arrays.asList(elem.getKey(), elem.getValue()));
                    }
                }
            }
        } finally {
            readUnlock();
        }

        return new ShowResultSet(showStmt.getMetaData(), rows);
    }

    public ShowResultSet showCreateCatalog(ShowCreateCatalogStmt showStmt) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        readLock();
        try {
            CatalogIf catalog = nameToCatalog.get(showStmt.getCatalog());
            if (catalog == null) {
                throw new AnalysisException("No catalog found with name " + showStmt.getCatalog());
            }
            StringBuilder sb = new StringBuilder();
            sb.append("\nCREATE CATALOG `").append(ClusterNamespace.getNameFromFullName(showStmt.getCatalog()))
                    .append("`");
            if (!Strings.isNullOrEmpty(catalog.getComment())) {
                sb.append("\nCOMMENT \"").append(catalog.getComment()).append("\"\n");
            }
            if (catalog.getProperties().size() > 0) {
                sb.append(" PROPERTIES (\n");
                sb.append(new PrintableMap<>(catalog.getProperties(), "=", true, true, true, true));
                sb.append("\n);");
            }

            rows.add(Lists.newArrayList(ClusterNamespace.getNameFromFullName(showStmt.getCatalog()), sb.toString()));
        } finally {
            readUnlock();
        }

        return new ShowResultSet(showStmt.getMetaData(), rows);
    }

    /**
     * Refresh the catalog meta and write the meta log.
     */
    public void refreshCatalog(RefreshCatalogStmt stmt) throws UserException {
        CatalogIf catalog = nameToCatalog.get(stmt.getCatalogName());
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + stmt.getCatalogName());
        }
        CatalogLog log = CatalogFactory.createCatalogLog(catalog.getId(), stmt);
        refreshCatalog(log);
    }

    public void refreshCatalog(CatalogLog log) {
        writeLock();
        try {
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
        CatalogIf catalog = CatalogFactory.createFromLog(log);
        createCatalogInternal(catalog, true);
    }

    private void createCatalogInternal(CatalogIf catalog, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (!isReplay && catalog instanceof ExternalCatalog) {
                ((ExternalCatalog) catalog).checkProperties();
            }
            Map<String, String> props = catalog.getProperties();
            if (props.containsKey(METADATA_REFRESH_INTERVAL_SEC)) {
                // need refresh
                long catalogId = catalog.getId();
                Integer metadataRefreshIntervalSec = Integer.valueOf(props.get(METADATA_REFRESH_INTERVAL_SEC));
                Integer[] sec = {metadataRefreshIntervalSec, metadataRefreshIntervalSec};
                Env.getCurrentEnv().getRefreshManager().addToRefreshMap(catalogId, sec);
            }
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
    public void replayRefreshCatalog(CatalogLog log) {
        writeLock();
        try {
            unprotectedRefreshCatalog(log.getCatalogId(), log.isInvalidCache());
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

    /**
     * Reply for alter catalog comment event.
     */
    public void replayAlterCatalogComment(CatalogLog log) {
        writeLock();
        try {
            CatalogIf catalog = idToCatalog.get(log.getCatalogId());
            if (catalog != null) {
                catalog.setComment(log.getComment());
            }
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
    public void replayAlterCatalogProps(CatalogLog log, Map<String, String> oldProperties, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            CatalogIf catalog = idToCatalog.get(log.getCatalogId());
            if (catalog instanceof ExternalCatalog) {
                Map<String, String> newProps = log.getNewProps();
                ((ExternalCatalog) catalog).tryModifyCatalogProps(newProps);
                if (!isReplay) {
                    try {
                        ((ExternalCatalog) catalog).checkProperties();
                    } catch (DdlException ddlException) {
                        if (oldProperties != null) {
                            ((ExternalCatalog) catalog).rollBackCatalogProps(oldProperties);
                        }
                        throw ddlException;
                    }
                }
                if (newProps.containsKey(METADATA_REFRESH_INTERVAL_SEC)) {
                    long catalogId = catalog.getId();
                    Integer metadataRefreshIntervalSec = Integer.valueOf(newProps.get(METADATA_REFRESH_INTERVAL_SEC));
                    Integer[] sec = {metadataRefreshIntervalSec, metadataRefreshIntervalSec};
                    Env.getCurrentEnv().getRefreshManager().addToRefreshMap(catalogId, sec);
                }
            }
            catalog.modifyCatalogProps(log.getNewProps());
        } finally {
            writeUnlock();
        }
    }

    // init catalog and init db can happen at any time,
    // even after catalog or db is dropped.
    // Because it may already hold the catalog or db object before they are being dropped.
    // So just skip the edit log if object does not exist.
    public void replayInitCatalog(InitCatalogLog log) {
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            return;
        }
        catalog.replayInitCatalog(log);
    }

    public void replayInitExternalDb(InitDatabaseLog log) {
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            return;
        }
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        if (db == null) {
            return;
        }
        db.replayInitDb(log, catalog);
    }

    public void replayRefreshExternalDb(ExternalObjectLog log) {
        writeLock();
        try {
            ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
            ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
            db.setUnInitialized(log.isInvalidCache());
        } finally {
            writeUnlock();
        }
    }

    public void refreshExternalTable(String dbName, String tableName, String catalogName, boolean ignoreIfNotExists)
            throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Tables");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }
        if (table instanceof ExternalTable) {
            ((ExternalTable) table).unsetObjectCreated();
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(catalog.getId(), dbName, tableName);
        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableId(table.getId());
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }

    public void replayRefreshExternalTable(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
            return;
        }
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        if (db == null) {
            LOG.warn("No db found with id:[{}], it may have been dropped.", log.getDbId());
            return;
        }
        ExternalTable table = db.getTableForReplay(log.getTableId());
        if (table == null) {
            LOG.warn("No table found with id:[{}], it may have been dropped.", log.getTableId());
            return;
        }
        table.unsetObjectCreated();
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidateTableCache(catalog.getId(), db.getFullName(), table.getName());
    }

    public void dropExternalTable(String dbName, String tableName, String catalogName, boolean ignoreIfExists)
            throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support drop ExternalCatalog Tables");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }
        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableId(table.getId());
        log.setLastUpdateTime(System.currentTimeMillis());
        replayDropExternalTable(log);
        Env.getCurrentEnv().getEditLog().logDropExternalTable(log);
    }

    public void replayDropExternalTable(ExternalObjectLog log) {
        LOG.debug("ReplayDropExternalTable,catalogId:[{}],dbId:[{}],tableId:[{}]", log.getCatalogId(), log.getDbId(),
                log.getTableId());
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
            return;
        }
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        if (db == null) {
            LOG.warn("No db found with id:[{}], it may have been dropped.", log.getDbId());
            return;
        }
        ExternalTable table = db.getTableForReplay(log.getTableId());
        if (table == null) {
            LOG.warn("No table found with id:[{}], it may have been dropped.", log.getTableId());
            return;
        }
        db.writeLock();
        try {
            db.dropTableForReplay(table.getName());
            db.setLastUpdateTime(log.getLastUpdateTime());
        } finally {
            db.writeUnlock();
        }

        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidateTableCache(catalog.getId(), db.getFullName(), table.getName());
    }

    public boolean externalTableExistInLocal(String dbName, String tableName, String catalogName) throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog Tables");
        }
        return ((ExternalCatalog) catalog).tableExistInLocal(dbName, tableName);
    }

    public void createExternalTableFromEvent(String dbName, String tableName, String catalogName,
            boolean ignoreIfExists)
            throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support create ExternalCatalog Tables");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table != null) {
            if (!ignoreIfExists) {
                throw new DdlException("Table " + tableName + " has exist in db " + dbName);
            }
            return;
        }
        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableName(tableName);
        log.setTableId(Env.getCurrentEnv().getNextId());
        log.setLastUpdateTime(System.currentTimeMillis());
        replayCreateExternalTableFromEvent(log);
        Env.getCurrentEnv().getEditLog().logCreateExternalTable(log);
    }

    public void replayCreateExternalTableFromEvent(ExternalObjectLog log) {
        LOG.debug("ReplayCreateExternalTable,catalogId:[{}],dbId:[{}],tableId:[{}],tableName:[{}]", log.getCatalogId(),
                log.getDbId(), log.getTableId(), log.getTableName());
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
            return;
        }
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        if (db == null) {
            LOG.warn("No db found with id:[{}], it may have been dropped.", log.getDbId());
            return;
        }
        db.writeLock();
        try {
            db.createTableForReplay(log.getTableName(), log.getTableId());
            db.setLastUpdateTime(log.getLastUpdateTime());
        } finally {
            db.writeUnlock();
        }
    }

    public void dropExternalDatabase(String dbName, String catalogName, boolean ignoreIfNotExists) throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support drop ExternalCatalog databases");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setInvalidCache(true);
        replayDropExternalDatabase(log);
        Env.getCurrentEnv().getEditLog().logDropExternalDatabase(log);
    }

    public void replayDropExternalDatabase(ExternalObjectLog log) {
        writeLock();
        try {
            LOG.debug("ReplayDropExternalTable,catalogId:[{}],dbId:[{}],tableId:[{}]", log.getCatalogId(),
                    log.getDbId(), log.getTableId());
            ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
            if (catalog == null) {
                LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
                return;
            }
            ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
            if (db == null) {
                LOG.warn("No db found with id:[{}], it may have been dropped.", log.getDbId());
                return;
            }
            catalog.dropDatabaseForReplay(db.getFullName());
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDbCache(catalog.getId(), db.getFullName());
        } finally {
            writeUnlock();
        }
    }

    public void createExternalDatabase(String dbName, String catalogName, boolean ignoreIfExists) throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support create ExternalCatalog databases");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db != null) {
            if (!ignoreIfExists) {
                throw new DdlException("Database " + dbName + " has exist in catalog " + catalog.getName());
            }
            return;
        }

        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(Env.getCurrentEnv().getNextId());
        log.setDbName(dbName);
        replayCreateExternalDatabase(log);
        Env.getCurrentEnv().getEditLog().logCreateExternalDatabase(log);
    }

    public void replayCreateExternalDatabase(ExternalObjectLog log) {
        writeLock();
        try {
            LOG.debug("ReplayCreateExternalDatabase,catalogId:[{}],dbId:[{}],dbName:[{}]", log.getCatalogId(),
                    log.getDbId(), log.getDbName());
            ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
            if (catalog == null) {
                LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
                return;
            }
            catalog.createDatabaseForReplay(log.getDbId(), log.getDbName());
        } finally {
            writeUnlock();
        }
    }

    public void addExternalPartitions(String catalogName, String dbName, String tableName, List<String> partitionNames,
                                      boolean ignoreIfNotExists)
            throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }
        if (!(table instanceof HMSExternalTable)) {
            LOG.warn("only support HMSTable");
            return;
        }

        HMSExternalTable hmsTable = (HMSExternalTable) table;
        Env.getCurrentEnv().getExtMetaCacheMgr().addPartitionsCache(catalog.getId(), hmsTable, partitionNames);
        long lastPartitionUpdateTime = System.currentTimeMillis();
        hmsTable.setPartitionUpdateTime(lastPartitionUpdateTime);
        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableId(table.getId());
        log.setPartitionNames(partitionNames);
        log.setLastUpdateTime(lastPartitionUpdateTime);
        Env.getCurrentEnv().getEditLog().logAddExternalPartitions(log);
    }

    public void replayAddExternalPartitions(ExternalObjectLog log) {
        LOG.debug("ReplayAddExternalPartitions,catalogId:[{}],dbId:[{}],tableId:[{}]", log.getCatalogId(),
                log.getDbId(), log.getTableId());
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
            return;
        }
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        if (db == null) {
            LOG.warn("No db found with id:[{}], it may have been dropped.", log.getDbId());
            return;
        }
        ExternalTable table = db.getTableForReplay(log.getTableId());
        if (table == null) {
            LOG.warn("No table found with id:[{}], it may have been dropped.", log.getTableId());
            return;
        }
        if (!(table instanceof HMSExternalTable)) {
            LOG.warn("only support HMSTable");
            return;
        }

        HMSExternalTable hmsTable = (HMSExternalTable) table;
        try {
            Env.getCurrentEnv().getExtMetaCacheMgr()
                        .addPartitionsCache(catalog.getId(), hmsTable, log.getPartitionNames());
            hmsTable.setPartitionUpdateTime(log.getLastUpdateTime());
        } catch (HMSClientException e) {
            LOG.warn("Network problem occurs or hms table has been deleted, fallback to invalidate table cache", e);
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(catalog.getId(),
                    db.getFullName(), table.getName());
        }
    }

    public void dropExternalPartitions(String catalogName, String dbName, String tableName, List<String> partitionNames,
                                       boolean ignoreIfNotExists)
            throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }

        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableId(table.getId());
        log.setPartitionNames(partitionNames);
        log.setLastUpdateTime(System.currentTimeMillis());
        replayDropExternalPartitions(log);
        Env.getCurrentEnv().getEditLog().logDropExternalPartitions(log);
    }

    public void replayDropExternalPartitions(ExternalObjectLog log) {
        LOG.debug("ReplayDropExternalPartitions,catalogId:[{}],dbId:[{}],tableId:[{}]", log.getCatalogId(),
                log.getDbId(), log.getTableId());
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
            return;
        }
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        if (db == null) {
            LOG.warn("No db found with id:[{}], it may have been dropped.", log.getDbId());
            return;
        }
        ExternalTable table = db.getTableForReplay(log.getTableId());
        if (table == null) {
            LOG.warn("No table found with id:[{}], it may have been dropped.", log.getTableId());
            return;
        }
        if (!(table instanceof HMSExternalTable)) {
            LOG.warn("only support HMSTable");
            return;
        }
        HMSExternalTable hmsTable = (HMSExternalTable) table;
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .dropPartitionsCache(catalog.getId(), hmsTable, log.getPartitionNames());
        hmsTable.setPartitionUpdateTime(log.getLastUpdateTime());
    }

    public void refreshExternalPartitions(String catalogName, String dbName, String tableName,
            List<String> partitionNames, boolean ignoreIfNotExists)
            throws DdlException {
        CatalogIf catalog = nameToCatalog.get(catalogName);
        if (catalog == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("No catalog found with name: " + catalogName);
            }
            return;
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }

        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableId(table.getId());
        log.setPartitionNames(partitionNames);
        log.setLastUpdateTime(System.currentTimeMillis());
        replayRefreshExternalPartitions(log);
        Env.getCurrentEnv().getEditLog().logInvalidateExternalPartitions(log);
    }

    public void replayRefreshExternalPartitions(ExternalObjectLog log) {
        LOG.debug("replayRefreshExternalPartitions,catalogId:[{}],dbId:[{}],tableId:[{}]", log.getCatalogId(),
                log.getDbId(), log.getTableId());
        ExternalCatalog catalog = (ExternalCatalog) idToCatalog.get(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("No catalog found with id:[{}], it may have been dropped.", log.getCatalogId());
            return;
        }
        ExternalDatabase db = catalog.getDbForReplay(log.getDbId());
        if (db == null) {
            LOG.warn("No db found with id:[{}], it may have been dropped.", log.getDbId());
            return;
        }
        ExternalTable table = db.getTableForReplay(log.getTableId());
        if (table == null) {
            LOG.warn("No table found with id:[{}], it may have been dropped.", log.getTableId());
            return;
        }
        if (!(table instanceof HMSExternalTable)) {
            LOG.warn("only support HMSTable");
            return;
        }
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidatePartitionsCache(catalog.getId(), db.getFullName(), table.getName(),
                        log.getPartitionNames());
        ((HMSExternalTable) table).setPartitionUpdateTime(log.getLastUpdateTime());
    }

    public void registerCatalogRefreshListener(Env env) {
        for (CatalogIf catalog : idToCatalog.values()) {
            Map<String, String> properties = catalog.getProperties();
            if (properties.containsKey(METADATA_REFRESH_INTERVAL_SEC)) {
                Integer metadataRefreshIntervalSec = Integer.valueOf(properties.get(METADATA_REFRESH_INTERVAL_SEC));
                Integer[] sec = {metadataRefreshIntervalSec, metadataRefreshIntervalSec};
                env.getRefreshManager().addToRefreshMap(catalog.getId(), sec);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (CatalogIf catalog : idToCatalog.values()) {
            nameToCatalog.put(catalog.getName(), catalog);
            // ATTN: can not call catalog.getProperties() here, because ResourceMgr is not replayed yet.
        }
        internalCatalog = (InternalCatalog) idToCatalog.get(InternalCatalog.INTERNAL_CATALOG_ID);
    }

    public Map<Long, CatalogIf> getIdToCatalog() {
        return idToCatalog;
    }

    public Set<CatalogIf> getCopyOfCatalog() {
        return new HashSet<>(idToCatalog.values());
    }
}

