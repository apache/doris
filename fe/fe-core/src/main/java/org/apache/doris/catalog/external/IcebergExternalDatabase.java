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
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class IcebergExternalDatabase extends ExternalDatabase<IcebergExternalTable> {

    private static final Logger LOG = LogManager.getLogger(IcebergExternalDatabase.class);

    public IcebergExternalDatabase(ExternalCatalog extCatalog, Long id, String name) {
        super(extCatalog, id, name, InitDatabaseLog.Type.ICEBERG);
    }

    @Override
    protected IcebergExternalTable getExternalTable(String tableName, long tblId, ExternalCatalog catalog) {
        return new IcebergExternalTable(tblId, tableName, name, (IcebergExternalCatalog) extCatalog);
    }

    @Override
    public List<IcebergExternalTable> getTablesOnIdOrder() {
        // Sort the name instead, because the id may change.
        return getTables().stream().sorted(Comparator.comparing(TableIf::getName)).collect(Collectors.toList());
    }

    @Override
    public void dropTable(String tableName) {
        LOG.debug("drop table [{}]", tableName);
        Long tableId = tableNameToId.remove(tableName);
        if (tableId == null) {
            LOG.warn("drop table [{}] failed", tableName);
        }
        idToTbl.remove(tableId);
    }

    @Override
    public void createTable(String tableName, long tableId) {
        LOG.debug("create table [{}]", tableName);
        tableNameToId.put(tableName, tableId);
        IcebergExternalTable table = new IcebergExternalTable(tableId, tableName, name,
                (IcebergExternalCatalog) extCatalog);
        idToTbl.put(tableId, table);
    }
}
