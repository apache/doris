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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class HudiMetadataCacheMgr {
    private final Map<Long, HudiPartitionProcessor> partitionProcessors = Maps.newConcurrentMap();
    private final Map<Long, HudiCachedFsViewProcessor> fsViewProcessors = Maps.newConcurrentMap();
    private final Map<Long, HudiCachedMetaClientProcessor> metaClientProcessors = Maps.newConcurrentMap();

    private final ExecutorService executor;

    public HudiMetadataCacheMgr(ExecutorService executor) {
        this.executor = executor;
    }

    public HudiPartitionProcessor getPartitionProcessor(CatalogIf catalog) {
        return partitionProcessors.computeIfAbsent(catalog.getId(), catalogId -> {
            if (catalog instanceof HMSExternalCatalog) {
                return new HudiCachedPartitionProcessor(catalogId, executor);
            } else {
                throw new RuntimeException("Hudi only supports hive(or compatible) catalog now");
            }
        });
    }

    public HudiCachedFsViewProcessor getFsViewProcessor(CatalogIf catalog) {
        return fsViewProcessors.computeIfAbsent(catalog.getId(), catalogId -> {
            if (catalog instanceof HMSExternalCatalog) {
                return new HudiCachedFsViewProcessor(executor);
            } else {
                throw new RuntimeException("Hudi only supports hive(or compatible) catalog now");
            }
        });
    }

    public HudiCachedMetaClientProcessor getHudiMetaClientProcessor(CatalogIf catalog) {
        return metaClientProcessors.computeIfAbsent(catalog.getId(), catalogId -> {
            if (catalog instanceof HMSExternalCatalog) {
                return new HudiCachedMetaClientProcessor(executor);
            } else {
                throw new RuntimeException("Hudi only supports hive(or compatible) catalog now");
            }
        });
    }

    public void removeCache(long catalogId) {
        HudiPartitionProcessor partitionProcessor = partitionProcessors.remove(catalogId);
        if (partitionProcessor != null) {
            partitionProcessor.cleanUp();
        }
        HudiCachedFsViewProcessor fsViewProcessor = fsViewProcessors.remove(catalogId);
        if (fsViewProcessor != null) {
            fsViewProcessor.cleanUp();
        }
        HudiCachedMetaClientProcessor metaClientProcessor = metaClientProcessors.remove(catalogId);
        if (metaClientProcessor != null) {
            metaClientProcessor.cleanUp();
        }
    }

    public void invalidateCatalogCache(long catalogId) {
        HudiPartitionProcessor processor = partitionProcessors.get(catalogId);
        if (processor != null) {
            processor.cleanUp();
        }
        HudiCachedFsViewProcessor fsViewProcessor = fsViewProcessors.get(catalogId);
        if (fsViewProcessor != null) {
            fsViewProcessor.invalidateAll();
        }
        HudiCachedMetaClientProcessor metaClientProcessor = metaClientProcessors.get(catalogId);
        if (metaClientProcessor != null) {
            metaClientProcessor.invalidateAll();
        }
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        HudiPartitionProcessor processor = partitionProcessors.get(catalogId);
        if (processor != null) {
            processor.cleanDatabasePartitions(dbName);
        }
        HudiCachedFsViewProcessor fsViewProcessor = fsViewProcessors.get(catalogId);
        if (fsViewProcessor != null) {
            fsViewProcessor.invalidateDbCache(dbName);
        }
        HudiCachedMetaClientProcessor metaClientProcessor = metaClientProcessors.get(catalogId);
        if (metaClientProcessor != null) {
            metaClientProcessor.invalidateDbCache(dbName);
        }
    }

    public void invalidateTableCache(ExternalTable dorisTable) {
        long catalogId = dorisTable.getCatalog().getId();
        HudiPartitionProcessor processor = partitionProcessors.get(catalogId);
        if (processor != null) {
            processor.cleanTablePartitions(dorisTable);
        }
        HudiCachedFsViewProcessor fsViewProcessor = fsViewProcessors.get(catalogId);
        if (fsViewProcessor != null) {
            fsViewProcessor.invalidateTableCache(dorisTable);
        }
        HudiCachedMetaClientProcessor metaClientProcessor = metaClientProcessors.get(catalogId);
        if (metaClientProcessor != null) {
            metaClientProcessor.invalidateTableCache(dorisTable);
        }
    }

    public Map<String, Map<String, String>> getCacheStats(CatalogIf catalog) {
        Map<String, Map<String, String>> res = Maps.newHashMap();

        HudiCachedPartitionProcessor partitionProcessor = (HudiCachedPartitionProcessor) getPartitionProcessor(catalog);
        res.putAll(partitionProcessor.getCacheStats());

        HudiCachedFsViewProcessor fsViewProcessor = getFsViewProcessor(catalog);
        res.putAll(fsViewProcessor.getCacheStats());

        HudiCachedMetaClientProcessor metaClientProcessor = getHudiMetaClientProcessor(catalog);
        res.putAll(metaClientProcessor.getCacheStats());

        return res;
    }
}
