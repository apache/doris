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

import com.google.common.collect.Lists;
import org.apache.doris.catalog.*;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * External data source for hive metastore compatible data sources.
 */
public class HMSExternalDataSource extends ExternalDataSource {
    private static final Logger LOG = LogManager.getLogger(HMSExternalDataSource.class);
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    protected final String hiveMetastoreUris;
    protected final HiveMetaStoreClient client;
    private final HMSType hmsType;

    private ConcurrentHashMap<String, DatabaseIf> nameToDb = new ConcurrentHashMap();

    public HMSExternalDataSource(String hiveMetastoreUris, HMSType hmsType) throws DdlException {
        this.hiveMetastoreUris = hiveMetastoreUris;
        this.hmsType = hmsType;
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreUris);
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            LOG.warn("Create HiveMetaStoreClient failed: {}", e.getMessage());
            throw new DdlException("Create HiveMetaStoreClient failed: " + e.getMessage());
        }
    }

    public HMSType getHmsType() {
        return hmsType;
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        try {
            return client.getAllDatabases();
        } catch (MetaException e) {
            LOG.warn("List Database Names failed. " + e.getMessage());
        }
        return null;
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        try {
            return client.getAllTables(dbName);
        } catch (MetaException e) {
            LOG.warn("List Table Names failed. " + e.getMessage());
        }
        return null;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        try {
            return client.tableExists(dbName, tblName);
        } catch (TException e) {
            LOG.warn("Check table exist failed. " + e.getMessage());
        }
        return false;
    }

    @Override
    public List<Column> getSchema(SessionContext ctx, String dbName, String tblName) {
        List<Column> hiveMetastoreSchema = Lists.newArrayList();
        try {
            List<FieldSchema> hmsSchema = client.getSchema(dbName, tblName);
            for (FieldSchema field : hmsSchema) {
                hiveMetastoreSchema.add(new Column(field.getName(), HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType()),
                    true, null, true, null, field.getComment()));
            }
        } catch (TException e) {
            LOG.warn("Get table schema failed. " + e.getMessage());
        }
        return hiveMetastoreSchema;
    }

//    @Override
//    public List<ExternalScanRange> getExternalScanRanges(SessionContext ctx) {
//        return null;
//    }

    @Nullable
    @Override
    public DatabaseIf getDbNullable(String dbName) {
        try {
            client.getDatabase(dbName);
        } catch (TException e) {
            LOG.warn("External database {} not exist.", dbName);
            return null;
        }
        // TODO: get a valid id.
        return new HMSExternalDatabase(this, 0, dbName, hiveMetastoreUris);
    }

    @Nullable
    @Override
    public DatabaseIf getDbNullable(long dbId) {
        return null;
    }

    @Override
    public Optional<DatabaseIf> getDb(String dbName) {
        return Optional.ofNullable(getDbNullable(dbName));
    }

    @Override
    public Optional<DatabaseIf> getDb(long dbId) {
        return Optional.ofNullable(getDbNullable(dbId));
    }

    @Override
    public <E extends Exception> DatabaseIf getDbOrException(String dbName, Function<String, E> e) throws E {
        DatabaseIf db = getDbNullable(dbName);
        if (db == null) {
            throw e.apply(dbName);
        }
        return db;
    }

    @Override
    public <E extends Exception> DatabaseIf getDbOrException(long dbId, Function<Long, E> e) throws E {
        DatabaseIf db = getDbNullable(dbId);
        if (db == null) {
            throw e.apply(dbId);
        }
        return db;
    }

    @Override
    public DatabaseIf getDbOrMetaException(String dbName) throws MetaNotFoundException {
        return getDbOrException(dbName, s -> new MetaNotFoundException("unknown databases, dbName=" + s,
            ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public DatabaseIf getDbOrMetaException(long dbId) throws MetaNotFoundException {
        return getDbOrException(dbId, s -> new MetaNotFoundException("unknown databases, dbId=" + s,
            ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public DatabaseIf getDbOrDdlException(String dbName) throws DdlException {
        return getDbOrException(dbName, s -> new DdlException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s),
            ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public DatabaseIf getDbOrDdlException(long dbId) throws DdlException {
        return getDbOrException(dbId, s -> new DdlException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s),
            ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public DatabaseIf getDbOrAnalysisException(String dbName) throws AnalysisException {
        return getDbOrException(dbName, s -> new AnalysisException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s),
            ErrorCode.ERR_BAD_DB_ERROR));
    }

    @Override
    public DatabaseIf getDbOrAnalysisException(long dbId) throws AnalysisException {
        return getDbOrException(dbId, s -> new AnalysisException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s),
            ErrorCode.ERR_BAD_DB_ERROR));
    }

    public enum HMSType {
        HIVE,
        ICEBERG,
        HUDI
    }
}
