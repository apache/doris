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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hive metastore external database.
 */
public class HMSExternalDatabase extends ExternalDatabase<HMSExternalTable> {
    private static final Logger LOG = LogManager.getLogger(HMSExternalDatabase.class);

    /**
     * Create HMS external database.
     *
     * @param extCatalog External catalog this database belongs to.
     * @param id database id.
     * @param name database name.
     * @param remoteName remote database name.
     */
    public HMSExternalDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName) {
        super(extCatalog, id, name, remoteName, InitDatabaseLog.Type.HMS);
    }

    @Override
    public HMSExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog,
            ExternalDatabase db) {
        return new HMSExternalTable(tblId, localTableName, remoteTableName, (HMSExternalCatalog) extCatalog,
                (HMSExternalDatabase) db);
    }

    @Override
    public boolean registerTable(TableIf tableIf) {
        super.registerTable(tableIf);
        HMSExternalTable table = getTableNullable(tableIf.getName());
        if (table != null) {
            table.setUpdateTime(tableIf.getUpdateTime());
        }
        return true;
    }

    @Override
    public HMSExternalTable getTableNullable(String tableName) {
        makeSureInitialized();
        // must use full qualified name to generate id.
        // otherwise, if 2 databases have the same table name, the id will be the same.
        return metaCache.getMetaObj(tableName,
            Util.genIdByName(extCatalog.getName(), name, tableName)).orElse(null);
    }

    @Override
    public HMSExternalTable buildTableForInit(String remoteTableName, String localTableName, long tblId,
                               ExternalCatalog catalog, ExternalDatabase db, boolean checkExists) {

        if (localTableName == null && remoteTableName != null) {
            localTableName = extCatalog.fromRemoteTableName(remoteName, remoteTableName);
        }

        if (remoteTableName == null) {
            remoteTableName = localTableName;
        }

        if (checkExists && !FeConstants.runningUnitTest) {
            if (!extCatalog.tableExist(ConnectContext.get().getSessionContext(), remoteName, remoteTableName)) {
                // If connection fails, treat the table as non-existent
                LOG.warn("Failed to check existence of table {} in the remote system. Ignoring this table.",
                        localTableName);
                return null;
            }
        }

        return buildTableInternal(remoteTableName, localTableName, tblId, catalog, db);
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        makeSureInitialized();
        return Objects.requireNonNull(listTableNames()).stream().map(Pair::value).collect(Collectors.toSet());
    }
}
