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
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class HudiCachedMetaClientProcessor {
    private static final Logger LOG = LogManager.getLogger(HudiCachedMetaClientProcessor.class);
    private final LoadingCache<HudiCachedClientKey, HoodieTableMetaClient> hudiTableMetaClientCache;

    public HudiCachedMetaClientProcessor(ExecutorService executor) {
        CacheFactory partitionCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_cache_num,
                true,
                null);

        this.hudiTableMetaClientCache =
                partitionCacheFactory.buildCache(
                        this::createHoodieTableMetaClient,
                        null,
                        executor);
    }

    private HoodieTableMetaClient createHoodieTableMetaClient(HudiCachedClientKey key) {
        LOG.debug("create hudi table meta client for {}.{}", key.getDbName(), key.getTbName());
        HadoopStorageConfiguration hadoopStorageConfiguration = new HadoopStorageConfiguration(key.getConf());
        return HiveMetaStoreClientHelper.ugiDoAs(
            key.getConf(),
            () -> HoodieTableMetaClient
                .builder()
                .setConf(hadoopStorageConfiguration)
                .setBasePath(key.getHudiBasePath())
                .build());
    }

    public HoodieTableMetaClient getHoodieTableMetaClient(
            String dbName, String tbName, String hudiBasePath, Configuration conf) {
        return hudiTableMetaClientCache.get(new HudiCachedClientKey(dbName, tbName, hudiBasePath, conf));
    }

    public void cleanUp() {
        hudiTableMetaClientCache.cleanUp();
    }

    public void invalidateAll() {
        hudiTableMetaClientCache.invalidateAll();
    }

    public void invalidateDbCache(String dbName) {
        hudiTableMetaClientCache.asMap().forEach((k, v) -> {
            if (k.getDbName().equals(dbName)) {
                hudiTableMetaClientCache.invalidate(k);
            }
        });
    }

    public void invalidateTableCache(String dbName, String tbName) {
        hudiTableMetaClientCache.asMap().forEach((k, v) -> {
            if (k.getDbName().equals(dbName) && k.getTbName().equals(tbName)) {
                hudiTableMetaClientCache.invalidate(k);
            }
        });
    }

    private static class HudiCachedClientKey {
        String dbName;
        String tbName;
        String hudiBasePath;
        Configuration conf;

        public HudiCachedClientKey(String dbName, String tbName, String hudiBasePath, Configuration conf) {
            this.dbName = dbName;
            this.tbName = tbName;
            this.hudiBasePath = hudiBasePath;
            this.conf = conf;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTbName() {
            return tbName;
        }

        public String getHudiBasePath() {
            return hudiBasePath;
        }

        public Configuration getConf() {
            return conf;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HudiCachedClientKey that = (HudiCachedClientKey) o;
            return Objects.equals(dbName, that.dbName) && Objects.equals(tbName, that.tbName)
                    && Objects.equals(hudiBasePath, that.hudiBasePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tbName, hudiBasePath);
        }
    }

    public Map<String, Map<String, String>> getCacheStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("hudi_meta_client_cache", ExternalMetaCacheMgr.getCacheStats(hudiTableMetaClientCache.stats(),
                hudiTableMetaClientCache.estimatedSize()));
        return res;
    }
}
