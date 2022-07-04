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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalDataSource;
import org.apache.doris.datasource.HMSExternalDataSource;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hive metastore external database.
 */
public class HMSExternalDatabase extends ExternalDatabase<HMSExternalTable> {
    private static final Logger LOG = LogManager.getLogger(HMSExternalDatabase.class);

    // Cache of table name to table id.
    private Map<String, Long> tableNameToId = Maps.newConcurrentMap();
    private Map<Long, HMSExternalTable> idToTbl = Maps.newHashMap();
    private boolean initialized = false;

    /**
     * Create HMS external database.
     *
     * @param extDataSource External data source this database belongs to.
     * @param id database id.
     * @param name database name.
     */
    public HMSExternalDatabase(ExternalDataSource extDataSource, long id, String name) {
        super(extDataSource, id, name);
    }

    private synchronized void makeSureInitialized() {
        if (!initialized) {
            init();
            initialized = true;
        }
    }

    private void init() {
        List<String> tableNames = extDataSource.listTableNames(null, name);
        if (tableNames != null) {
            for (String tableName : tableNames) {
                long tblId = Catalog.getCurrentCatalog().getNextId();
                tableNameToId.put(tableName, tblId);
                idToTbl.put(tblId, new HMSExternalTable(tblId, tableName, name, (HMSExternalDataSource) extDataSource));
            }
        }
    }

    @Override
    public List<HMSExternalTable> getTables() {
        makeSureInitialized();
        return Lists.newArrayList(idToTbl.values());
    }

    @Override
    public List<HMSExternalTable> getTablesOnIdOrder() {
        // Sort the name instead, because the id may change.
        return getTables().stream().sorted(Comparator.comparing(TableIf::getName)).collect(Collectors.toList());
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        makeSureInitialized();
        return Sets.newHashSet(tableNameToId.keySet());
    }

    @Override
    public HMSExternalTable getTableNullable(String tableName) {
        makeSureInitialized();
        if (!tableNameToId.containsKey(tableName)) {
            return null;
        }
        return idToTbl.get(tableNameToId.get(tableName));
    }

    @Override
    public HMSExternalTable getTableNullable(long tableId) {
        makeSureInitialized();
        return idToTbl.get(tableId);
    }
}
