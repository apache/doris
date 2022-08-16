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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.EsExternalCatalog;
import org.apache.doris.datasource.ExternalCatalog;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Elasticsearch metastore external database.
 */
public class EsExternalDatabase extends ExternalDatabase<EsExternalTable> {

    private static final Logger LOG = LogManager.getLogger(EsExternalDatabase.class);

    // Cache of table name to table id.
    private Map<String, Long> tableNameToId = Maps.newConcurrentMap();
    private Map<Long, EsExternalTable> idToTbl = Maps.newHashMap();

    /**
     * Create Elasticsearch external database.
     *
     * @param extCatalog External data source this database belongs to.
     * @param id database id.
     * @param name database name.
     */
    public EsExternalDatabase(ExternalCatalog extCatalog, long id, String name) {
        super(extCatalog, id, name);
        init();
    }

    private void init() {
        List<String> tableNames = extCatalog.listTableNames(null, name);
        if (tableNames != null) {
            for (String tableName : tableNames) {
                long tblId = Env.getCurrentEnv().getNextId();
                tableNameToId.put(tableName, tblId);
                idToTbl.put(tblId, new EsExternalTable(tblId, tableName, name, (EsExternalCatalog) extCatalog));
            }
        }
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        // Doesn't need to lock because everytime we call the hive metastore api to get table names.
        return new HashSet<>(extCatalog.listTableNames(null, name));
    }

    @Override
    public List<EsExternalTable> getTables() {
        return new ArrayList<>(idToTbl.values());
    }

    @Override
    public EsExternalTable getTableNullable(String tableName) {
        if (!tableNameToId.containsKey(tableName)) {
            return null;
        }
        return idToTbl.get(tableNameToId.get(tableName));
    }

    @Override
    public EsExternalTable getTableNullable(long tableId) {
        return idToTbl.get(tableId);
    }
}
