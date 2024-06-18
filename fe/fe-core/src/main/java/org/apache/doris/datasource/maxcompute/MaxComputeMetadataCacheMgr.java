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

package org.apache.doris.datasource.maxcompute;

import com.google.common.collect.Maps;

import java.util.Map;

public class MaxComputeMetadataCacheMgr {

    private final Map<Long, MaxComputeMetadataCache> maxComputeMetadataCaches = Maps.newConcurrentMap();

    public MaxComputeMetadataCacheMgr() {
    }

    public MaxComputeMetadataCache getMaxComputeMetadataCache(long catalogId) {
        MaxComputeMetadataCache cache = maxComputeMetadataCaches.get(catalogId);
        if (cache == null) {
            cache = new MaxComputeMetadataCache();
            maxComputeMetadataCaches.put(catalogId, cache);
        }
        return cache;
    }

    public void removeCache(long catalogId) {
        MaxComputeMetadataCache cache = maxComputeMetadataCaches.remove(catalogId);
        if (cache != null) {
            cache.cleanUp();
        }
    }

    public void invalidateCatalogCache(long catalogId) {
        MaxComputeMetadataCache cache = maxComputeMetadataCaches.get(catalogId);
        if (cache != null) {
            cache.cleanUp();
        }
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        MaxComputeMetadataCache cache = maxComputeMetadataCaches.get(catalogId);
        if (cache != null) {
            cache.cleanDatabaseCache(dbName);
        }
    }

    public void invalidateTableCache(long catalogId, String dbName, String tblName) {
        MaxComputeMetadataCache cache = maxComputeMetadataCaches.get(catalogId);
        if (cache != null) {
            cache.cleanTableCache(dbName, tblName);
        }
    }
}
