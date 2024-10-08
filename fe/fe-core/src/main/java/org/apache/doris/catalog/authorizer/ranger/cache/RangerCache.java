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

package org.apache.doris.catalog.authorizer.ranger.cache;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.RowFilterPolicy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class RangerCache {
    private static final Logger LOG = LoggerFactory.getLogger(RangerCache.class);

    private CatalogAccessController controller;
    private LoadingCache<DatamaskCacheKey, Optional<DataMaskPolicy>> datamaskCache = CacheBuilder.newBuilder()
            .maximumSize(Config.ranger_cache_size)
            .build(new CacheLoader<DatamaskCacheKey, Optional<DataMaskPolicy>>() {
                @Override
                public Optional<DataMaskPolicy> load(DatamaskCacheKey key) {
                    return loadDataMask(key);
                }
            });

    private LoadingCache<RowFilterCacheKey, List<? extends RowFilterPolicy>> rowFilterCache = CacheBuilder.newBuilder()
            .maximumSize(Config.ranger_cache_size)
            .build(new CacheLoader<RowFilterCacheKey, List<? extends RowFilterPolicy>>() {
                @Override
                public List<? extends RowFilterPolicy> load(RowFilterCacheKey key) {
                    return loadRowFilter(key);
                }
            });

    public RangerCache() {
    }

    public void init(CatalogAccessController controller) {
        this.controller = controller;
    }

    private Optional<DataMaskPolicy> loadDataMask(DatamaskCacheKey key) {
        Objects.requireNonNull(controller, "controller can not be null");
        if (LOG.isDebugEnabled()) {
            LOG.debug("load datamask: {}", key);
        }
        return controller.evalDataMaskPolicy(key.getUserIdentity(), key.getCtl(), key.getDb(), key.getTbl(),
                key.getCol());
    }

    private List<? extends RowFilterPolicy> loadRowFilter(RowFilterCacheKey key) {
        Objects.requireNonNull(controller, "controller can not be null");
        if (LOG.isDebugEnabled()) {
            LOG.debug("load row filter: {}", key);
        }
        return controller.evalRowFilterPolicies(key.getUserIdentity(), key.getCtl(), key.getDb(), key.getTbl());
    }

    public void invalidateDataMaskCache() {
        datamaskCache.invalidateAll();
    }

    public void invalidateRowFilterCache() {
        rowFilterCache.invalidateAll();
    }

    public Optional<DataMaskPolicy> getDataMask(DatamaskCacheKey key) {
        try {
            return datamaskCache.get(key);
        } catch (ExecutionException e) {
            throw new CacheException("failed to get datamask for:" + key, e);
        }
    }

    public List<? extends RowFilterPolicy> getRowFilters(RowFilterCacheKey key) {
        try {
            return rowFilterCache.get(key);
        } catch (ExecutionException e) {
            throw new CacheException("failed to get row filter for:" + key, e);
        }
    }

}
