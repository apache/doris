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

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class HudiCachedFsViewProcessor {
    private static final Logger LOG = LogManager.getLogger(HudiCachedFsViewProcessor.class);
    private final LoadingCache<FsViewKey, HoodieTableFileSystemView> fsViewCache;

    public HudiCachedFsViewProcessor(ExecutorService executor) {
        CacheFactory partitionCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.fsViewCache = partitionCacheFactory.buildCache(this::createFsView, null, executor);
    }

    private HoodieTableFileSystemView createFsView(FsViewKey key) {
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
        HudiLocalEngineContext ctx = new HudiLocalEngineContext(key.getClient().getStorageConf());
        return FileSystemViewManager.createInMemoryFileSystemView(ctx, key.getClient(), metadataConfig);
    }

    public HoodieTableFileSystemView getFsView(String dbName, String tbName, HoodieTableMetaClient hudiClient) {
        return fsViewCache.get(new FsViewKey(dbName, tbName, hudiClient));
    }

    public void cleanUp() {
        fsViewCache.cleanUp();
    }

    public void invalidateAll() {
        fsViewCache.invalidateAll();
    }

    public void invalidateDbCache(String dbName) {
        fsViewCache.asMap().forEach((k, v) -> {
            if (k.getDbName().equals(dbName)) {
                fsViewCache.invalidate(k);
            }
        });
    }

    public void invalidateTableCache(String dbName, String tbName) {
        fsViewCache.asMap().forEach((k, v) -> {
            if (k.getDbName().equals(dbName) && k.getTbName().equals(tbName)) {
                fsViewCache.invalidate(k);
            }
        });
    }

    private static class FsViewKey {
        String dbName;
        String tbName;
        HoodieTableMetaClient client;

        public FsViewKey(String dbName, String tbName, HoodieTableMetaClient client) {
            this.dbName = dbName;
            this.tbName = tbName;
            this.client = client;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTbName() {
            return tbName;
        }

        public HoodieTableMetaClient getClient() {
            return client;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FsViewKey fsViewKey = (FsViewKey) o;
            return Objects.equals(dbName, fsViewKey.dbName) && Objects.equals(tbName, fsViewKey.tbName)
                && Objects.equals(client.getBasePathV2(), fsViewKey.client.getBasePathV2());
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tbName, client.getBasePathV2());
        }
    }

    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("hudi_fs_view_cache",
                ExternalMetaCacheMgr.getCacheStats(fsViewCache.stats(), fsViewCache.estimatedSize()));
        return res;
    }
}
