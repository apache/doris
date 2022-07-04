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

import org.apache.doris.datasource.ExternalDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Elasticsearch metastore external database.
 */
public class EsExternalDatabase extends ExternalDatabase<HMSExternalTable> {

    private static final Logger LOG = LogManager.getLogger(EsExternalDatabase.class);

    // Cache of table name to table id.
    private ConcurrentHashMap<String, Long> tableNameToId = new ConcurrentHashMap<>();
    private AtomicLong nextId = new AtomicLong(0);

    /**
     * Create Elasticsearch external database.
     *
     * @param extDataSource External data source this database belongs to.
     * @param id database id.
     * @param name database name.
     * @param uri Hive metastore uri.
     */
    public EsExternalDatabase(ExternalDataSource extDataSource, long id, String name) {
        super(extDataSource, id, name);
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
    public Set<String> getTableNamesWithLock() {
        // Doesn't need to lock because everytime we call the hive metastore api to get table names.
        return new HashSet<>(extDataSource.listTableNames(null, name));
    }
}
