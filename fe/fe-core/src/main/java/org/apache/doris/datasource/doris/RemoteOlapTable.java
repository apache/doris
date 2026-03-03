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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TempPartitions;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RemoteOlapTable extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(RemoteOlapTable.class);

    private RemoteDorisExternalCatalog catalog;
    private RemoteDorisExternalDatabase database;

    public RemoteDorisExternalCatalog getCatalog() {
        return catalog;
    }

    public void setCatalog(RemoteDorisExternalCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public RemoteDorisExternalDatabase getDatabase() {
        return database;
    }

    public void setDatabase(RemoteDorisExternalDatabase database) {
        this.database = database;
    }

    public static RemoteOlapTable fromOlapTable(OlapTable olapTable) {
        try {
            RemoteOlapTable externalOlapTable = new RemoteOlapTable();
            Class<?> currentClass = olapTable.getClass();
            while (currentClass != null) {
                for (Field field : currentClass.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    field.setAccessible(true);
                    field.set(externalOlapTable, field.get(olapTable));
                }
                currentClass = currentClass.getSuperclass();
            }
            return externalOlapTable;
        } catch (Exception e) {
            throw new RuntimeException("failed to initial external olap table", e);
        }
    }

    public void rebuildPartitions(List<Partition> oldPartitions, List<Partition> updatedPartitions,
            List<Long> removedPartitions)
            throws AnalysisException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("rebuildPartitions oldPartitions: " + oldPartitions.size() + ", updatedPartitions: "
                    + updatedPartitions.size() + ", removedPartitions: " + removedPartitions.size());
        }
        ConcurrentHashMap<Long, Partition> newIdToPartition = new ConcurrentHashMap<>();
        for (Partition oldPartition : oldPartitions) {
            newIdToPartition.put(oldPartition.getId(), oldPartition);
        }
        for (Long removedPartition : removedPartitions) {
            newIdToPartition.remove(removedPartition);
        }
        for (Partition updatedPartition : updatedPartitions) {
            newIdToPartition.put(updatedPartition.getId(), updatedPartition);
        }
        Map<String, Partition> newNameToPartition = Maps.newTreeMap();
        for (Partition partition : newIdToPartition.values()) {
            newNameToPartition.put(partition.getName(), partition);
        }
        this.idToPartition = newIdToPartition;
        this.nameToPartition = newNameToPartition;
    }

    public void rebuildTempPartitions(List<Partition> oldPartitions, List<Partition> updatedPartitions,
                                  List<Long> removedPartitions) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("rebuildTempPartitions oldPartitions: " + oldPartitions.size() + ", updatedPartitions: "
                    + updatedPartitions.size() + ", removedPartitions: " + removedPartitions.size());
        }
        ConcurrentHashMap<Long, Partition> newIdToPartition = new ConcurrentHashMap<>();
        for (Partition oldPartition : oldPartitions) {
            newIdToPartition.put(oldPartition.getId(), oldPartition);
        }
        for (Long removedPartition : removedPartitions) {
            newIdToPartition.remove(removedPartition);
        }
        for (Partition updatedPartition : updatedPartitions) {
            newIdToPartition.put(updatedPartition.getId(), updatedPartition);
        }
        Map<String, Partition> newNameToPartition = Maps.newTreeMap();
        for (Partition partition : newIdToPartition.values()) {
            newNameToPartition.put(partition.getName(), partition);
        }
        this.setTempPartitions(new TempPartitions(newIdToPartition, newNameToPartition));
    }

    public void invalidateBackendsIfNeed() {
        ImmutableMap<Long, Backend> backends =
                Env.getCurrentEnv().getExtMetaCacheMgr().getDorisExternalMetaCacheMgr().getBackends(catalog.getId());
        for (Partition partition : getPartitions()) {
            for (Tablet tablet : partition.getBaseIndex().getTablets()) {
                for (long backendId : tablet.getBackendIds()) {
                    if (!backends.containsKey(backendId)) {
                        Env.getCurrentEnv().getExtMetaCacheMgr().getDorisExternalMetaCacheMgr()
                                .invalidateBackendCache(catalog.getId());
                        return;
                    }
                }
            }
        }
    }

    @Override
    public long getCatalogId() {
        return catalog.getId();
    }

    public ImmutableMap<Long, Backend> getAllBackendsByAllCluster() {
        return Env.getCurrentEnv().getExtMetaCacheMgr().getDorisExternalMetaCacheMgr().getBackends(catalog.getId());
    }
}
