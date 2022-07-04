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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.ExternalDataSource;
import org.apache.doris.datasource.HMSExternalDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Hive metastore external database.
 */
public class HMSExternalDatabase extends ExternalDatabase<HMSExternalTable> {

    private static final Logger LOG = LogManager.getLogger(HMSExternalDatabase.class);

    // Cache of table name to table id.
    private ConcurrentHashMap<String, Long> tableNameToId = new ConcurrentHashMap<>();
    private AtomicLong nextId = new AtomicLong(0);
    private final String hiveMetastoreUri;

    /**
     * Create HMS external database.
     *
     * @param extDataSource External data source this database belongs to.
     * @param id database id.
     * @param name database name.
     * @param uri Hive metastore uri.
     */
    public HMSExternalDatabase(ExternalDataSource extDataSource, long id, String name, String uri) {
        super(extDataSource, id, name);
        this.hiveMetastoreUri = uri;
        init();
    }

    private void init() {
        List<String> tableNames = extDataSource.listTableNames(null, name);
        if (tableNames != null) {
            for (String tableName : tableNames) {
                tableNameToId.put(tableName, nextId.incrementAndGet());
            }
        }
    }

    @Override
    public List<HMSExternalTable> getTables() {
        List<HMSExternalTable> tables = new ArrayList<>();
        List<String> tableNames = extDataSource.listTableNames(null, name);
        for (String tableName : tableNames) {
            tableNameToId.putIfAbsent(tableName, nextId.incrementAndGet());
            try {
                tables.add(new HMSExternalTable(tableNameToId.get(tableName), tableName, name,
                        (HMSExternalDataSource) extDataSource));
            } catch (MetaNotFoundException e) {
                LOG.warn("Table {} in db {} not found in Hive metastore.", tableName, name, e);
            }
        }
        return tables;
    }

    @Override
    public List<HMSExternalTable> getTablesOnIdOrder() {
        // Sort the name instead, because the id may change.
        return getTables().stream().sorted(Comparator.comparing(TableIf::getName)).collect(Collectors.toList());
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        // Doesn't need to lock because everytime we call the hive metastore api to get table names.
        return new HashSet<>(extDataSource.listTableNames(null, name));
    }

    @Override
    public HMSExternalTable getTableNullable(String tableName) {
        if (extDataSource.tableExist(null, name, tableName)) {
            tableNameToId.putIfAbsent(tableName, nextId.incrementAndGet());
            try {
                return new HMSExternalTable(tableNameToId.get(tableName), tableName, name,
                        (HMSExternalDataSource) extDataSource);
            } catch (MetaNotFoundException e) {
                LOG.warn("Table {} in db {} not found in Hive metastore.", tableName, name, e);
            }
        }
        return null;
    }

    @Override
    public HMSExternalTable getTableNullable(long tableId) {
        for (Map.Entry<String, Long> entry : tableNameToId.entrySet()) {
            if (entry.getValue() == tableId) {
                if (extDataSource.tableExist(null, name, entry.getKey())) {
                    try {
                        return new HMSExternalTable(tableId, entry.getKey(), name,
                                (HMSExternalDataSource) extDataSource);
                    } catch (MetaNotFoundException e) {
                        LOG.warn("Table {} in db {} not found in Hive metastore.", entry.getKey(), name, e);
                    }
                }
                return null;
            }
        }
        return null;
    }

    @Override
    public Optional<HMSExternalTable> getTable(String tableName) {
        return Optional.ofNullable(getTableNullable(tableName));
    }

    @Override
    public Optional<HMSExternalTable> getTable(long tableId) {
        return Optional.ofNullable(getTableNullable(tableId));
    }

    @Override
    public <E extends Exception> HMSExternalTable getTableOrException(String tableName, Function<String, E> e)
            throws E {
        HMSExternalTable table = getTableNullable(tableName);
        if (table == null) {
            throw e.apply(tableName);
        }
        return table;
    }

    @Override
    public <E extends Exception> HMSExternalTable getTableOrException(long tableId, Function<Long, E> e) throws E {
        HMSExternalTable table = getTableNullable(tableId);
        if (table == null) {
            throw e.apply(tableId);
        }
        return table;
    }

    @Override
    public HMSExternalTable getTableOrMetaException(String tableName) throws MetaNotFoundException {
        return getTableOrException(tableName, t -> new MetaNotFoundException("unknown table, tableName=" + t));
    }

    @Override
    public HMSExternalTable getTableOrMetaException(long tableId) throws MetaNotFoundException {
        return getTableOrException(tableId, t -> new MetaNotFoundException("unknown table, tableName=" + t));
    }

    @Override
    public HMSExternalTable getTableOrMetaException(String tableName, TableIf.TableType tableType)
            throws MetaNotFoundException {
        HMSExternalTable table = getTableOrMetaException(tableName);
        if (table.getType() != tableType) {
            throw new MetaNotFoundException("table type is not "
                + tableType + ", tableName=" + tableName + ", type=" + table.getType());
        }
        return table;
    }

    @Override
    public HMSExternalTable getTableOrMetaException(long tableId, TableIf.TableType tableType)
            throws MetaNotFoundException {
        HMSExternalTable table = getTableOrMetaException(tableId);
        if (table.getType() != tableType) {
            throw new MetaNotFoundException("table type is not "
                + tableType + ", tableId=" + tableId + ", type=" + table.getType());
        }
        return table;
    }

    @Override
    public HMSExternalTable getTableOrDdlException(String tableName) throws DdlException {
        return getTableOrException(tableName, t -> new DdlException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t)));
    }

    @Override
    public HMSExternalTable getTableOrDdlException(long tableId) throws DdlException {
        return getTableOrException(tableId, t -> new DdlException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t)));
    }

    @Override
    public HMSExternalTable getTableOrAnalysisException(String tableName) throws AnalysisException {
        return getTableOrException(tableName,
            t -> new AnalysisException(ErrorCode.ERR_UNKNOWN_TABLE.formatErrorMsg(t, name)));
    }

    @Override
    public HMSExternalTable getTableOrAnalysisException(long tableId) throws AnalysisException {
        return getTableOrException(tableId,
            t -> new AnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t)));
    }
}
