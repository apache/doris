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

import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * External data source for hive metastore compatible data sources.
 */
public class HMSExternalDataSource extends ExternalDataSource {
    private static final Logger LOG = LogManager.getLogger(HMSExternalDataSource.class);

    //Cache of db name to db id.
    private ConcurrentHashMap<String, Long> dbNameToId = new ConcurrentHashMap();
    private static final AtomicLong nextId = new AtomicLong(0);

    private boolean initialized = false;
    protected String hiveMetastoreUris;
    protected HiveMetaStoreClient client;

    /**
     * Default constructor for HMSExternalDataSource.
     */
    public HMSExternalDataSource(String name, Map<String, String> props) {
        this.id = nextId.incrementAndGet();
        this.name = name;
        this.type = "hms";
        this.dsProperty = new DataSourceProperty();
        this.dsProperty.setProperties(props);
        this.hiveMetastoreUris = props.getOrDefault("hive.metastore.uris", "thrift://127.0.0.1:9083");
    }

    private void init() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreUris);
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            LOG.warn("Failed to create HiveMetaStoreClient: {}", e.getMessage());
        }
        List<String> allDatabases;
        try {
            allDatabases = client.getAllDatabases();
        } catch (MetaException e) {
            LOG.warn("Fail to init db name to id map. {}", e.getMessage());
            return;
        }
        // Update the db name to id map.
        if (allDatabases == null) {
            return;
        }
        for (String db : allDatabases) {
            dbNameToId.put(db, nextId.incrementAndGet());
        }
    }

    /**
     * Datasource can't be init when creating because the external datasource may depend on third system.
     * So you have to make sure the client of third system is initialized before any method was called.
     */
    private synchronized void makeSureInitialized() {
        if (!initialized) {
            init();
            initialized = true;
        }
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        try {
            List<String> allDatabases = client.getAllDatabases();
            // Update the db name to id map.
            for (String db : allDatabases) {
                dbNameToId.putIfAbsent(db, nextId.incrementAndGet());
            }
            return allDatabases;
        } catch (MetaException e) {
            LOG.warn("List Database Names failed. {}", e.getMessage());
        }
        return Lists.newArrayList();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        try {
            return client.getAllTables(dbName);
        } catch (MetaException e) {
            LOG.warn("List Table Names failed. {}", e.getMessage());
        }
        return Lists.newArrayList();
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        try {
            return client.tableExists(dbName, tblName);
        } catch (TException e) {
            LOG.warn("Check table exist failed. {}", e.getMessage());
        }
        return false;
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(String dbName) {
        makeSureInitialized();
        try {
            client.getDatabase(dbName);
        } catch (TException e) {
            LOG.warn("External database {} not exist.", dbName);
            return null;
        }
        // The id may change after FE restart since we don't persist it.
        // Different FEs may have different ids for the same dbName.
        // May duplicate with internal db id as well.
        dbNameToId.putIfAbsent(dbName, nextId.incrementAndGet());
        return new HMSExternalDatabase(this, dbNameToId.get(dbName), dbName, hiveMetastoreUris);
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(long dbId) {
        makeSureInitialized();
        for (Map.Entry<String, Long> entry : dbNameToId.entrySet()) {
            if (entry.getValue() == dbId) {
                return new HMSExternalDatabase(this, dbId, entry.getKey(), hiveMetastoreUris);
            }
        }
        return null;
    }

    @Override
    public Optional<ExternalDatabase> getDb(String dbName) {
        return Optional.ofNullable(getDbNullable(dbName));
    }

    @Override
    public Optional<ExternalDatabase> getDb(long dbId) {
        return Optional.ofNullable(getDbNullable(dbId));
    }

    @Override
    public <E extends Exception> ExternalDatabase getDbOrException(String dbName, Function<String, E> e) throws E {
        ExternalDatabase db = getDbNullable(dbName);
        if (db == null) {
            throw e.apply(dbName);
        }
        return db;
    }

    @Override
    public <E extends Exception> ExternalDatabase getDbOrException(long dbId, Function<Long, E> e) throws E {
        ExternalDatabase db = getDbNullable(dbId);
        if (db == null) {
            throw e.apply(dbId);
        }
        return db;
    }

    @Override
    public ExternalDatabase getDbOrMetaException(String dbName) throws MetaNotFoundException {
        return getDbOrException(dbName,
                s -> new MetaNotFoundException("unknown databases, dbName=" + s, ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public ExternalDatabase getDbOrMetaException(long dbId) throws MetaNotFoundException {
        return getDbOrException(dbId,
                s -> new MetaNotFoundException("unknown databases, dbId=" + s, ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public ExternalDatabase getDbOrDdlException(String dbName) throws DdlException {
        return getDbOrException(dbName,
                s -> new DdlException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public ExternalDatabase getDbOrDdlException(long dbId) throws DdlException {
        return getDbOrException(dbId,
                s -> new DdlException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public ExternalDatabase getDbOrAnalysisException(String dbName) throws AnalysisException {
        return getDbOrException(dbName,
                s -> new AnalysisException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public ExternalDatabase getDbOrAnalysisException(long dbId) throws AnalysisException {
        return getDbOrException(dbId,
                s -> new AnalysisException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public List<Long> getDbIds() {
        makeSureInitialized();
        return Lists.newArrayList(dbNameToId.values());
    }
}
