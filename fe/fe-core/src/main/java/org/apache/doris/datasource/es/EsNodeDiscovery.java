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


import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;


/**
 * It is responsible for automatic loading all ES external catalogs' nodes info periodically
 */
public class EsNodeDiscovery extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(EsNodeDiscovery.class);

    private Map<Long, EsExternalCatalog> esCatalogs;

    public EsNodeDiscovery() {
        super("es node discovery", Config.es_state_sync_interval_second * 1000);
        esCatalogs = Maps.newConcurrentMap();
    }

    public void registerCatalog(EsExternalCatalog esCatalog) {
        if (Env.isCheckpointThread()) {
            return;
        }
        esCatalogs.put(esCatalog.getId(), esCatalog);
        LOG.info("register a new catalog [{}] to sync list", esCatalog);
    }

    public void deregisterCatalog(long catalogId) {
        esCatalogs.remove(catalogId);
    }

    @Override
    protected void runAfterCatalogReady() {
        for (EsExternalCatalog esCatalog : esCatalogs.values()) {
            try {
                esCatalog.detectAvailableNodesInfo();
            } catch (Throwable e) {
                LOG.warn("Exception happens when fetch catalog [{}] nodes info data from remote es cluster",
                        esCatalog.getName(), e);
                esCatalog.clearAvailableNodesInfo();
            }
        }
    }
}
