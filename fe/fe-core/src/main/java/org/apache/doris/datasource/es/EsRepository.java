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

package org.apache.doris.datasource.es;


import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;


/**
 * It is responsible for loading all ES external table's meta-data such as `fields`, `partitions` periodically,
 * playing the `repo` role at Doris On ES
 */
public class EsRepository extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(EsRepository.class);

    private Map<Long, EsTable> esTables;

    private Map<Long, EsRestClient> esClients;

    public EsRepository() {
        super("es repository", Config.es_state_sync_interval_second * 1000);
        esTables = Maps.newConcurrentMap();
        esClients = Maps.newConcurrentMap();
    }

    public void registerTable(EsTable esTable) {
        if (Env.isCheckpointThread()) {
            return;
        }
        esTables.put(esTable.getId(), esTable);
        esClients.put(esTable.getId(),
                new EsRestClient(esTable.getSeeds(), esTable.getUserName(), esTable.getPasswd(),
                        esTable.isHttpSslEnabled()));
        LOG.info("register a new table [{}] to sync list", esTable);
    }

    public void deRegisterTable(long tableId) {
        esTables.remove(tableId);
        esClients.remove(tableId);
        LOG.info("deregister table [{}] from sync list", tableId);
    }

    @Override
    protected void runAfterCatalogReady() {
        for (EsTable esTable : esTables.values()) {
            try {
                esTable.syncTableMetaData();
            } catch (Throwable e) {
                LOG.warn("Exception happens when fetch index [{}] meta data from remote es cluster",
                        esTable.getName(), e);
                esTable.setEsTablePartitions(null);
                esTable.setLastMetaDataSyncException(e);
            }
        }
    }

    // should call this method to init the state store after loading image
    // the rest of tables will be added or removed by replaying edit log
    // when fe is start to load image, should call this method to init the state store
    public void loadTableFromCatalog() {
        if (Env.isCheckpointThread()) {
            return;
        }
        List<Long> dbIds = Env.getCurrentEnv().getInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database database = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (database == null) {
                continue;
            }

            List<Table> tables = database.getTables();
            for (Table table : tables) {
                if (table.getType() == TableType.ELASTICSEARCH) {
                    registerTable((EsTable) table);
                }
            }
        }
    }
}
