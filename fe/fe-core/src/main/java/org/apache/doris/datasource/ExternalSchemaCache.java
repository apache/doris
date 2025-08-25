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

package org.apache.doris.datasource;

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;

import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.Data;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;

// The schema cache for external table
public class ExternalSchemaCache {
    private static final Logger LOG = LogManager.getLogger(ExternalSchemaCache.class);
    private final ExternalCatalog catalog;

    private LoadingCache<SchemaCacheKey, Optional<SchemaCacheValue>> schemaCache;

    public ExternalSchemaCache(ExternalCatalog catalog, ExecutorService executor) {
        this.catalog = catalog;
        init(executor);
        initMetrics();
    }

    private void init(ExecutorService executor) {
        long schemaCacheTtlSecond = NumberUtils.toLong(
                (catalog.getProperties().get(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND)), ExternalCatalog.CACHE_NO_TTL);
        CacheFactory schemaCacheFactory = new CacheFactory(
                OptionalLong.of(schemaCacheTtlSecond >= ExternalCatalog.CACHE_TTL_DISABLE_CACHE
                        ? schemaCacheTtlSecond : Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_schema_cache_num,
                false,
                null);
        schemaCache = schemaCacheFactory.buildCache(this::loadSchema, null, executor);
    }

    private void initMetrics() {
        // schema cache
        GaugeMetric<Long> schemaCacheGauge = new GaugeMetric<Long>("external_schema_cache",
                Metric.MetricUnit.NOUNIT, "external schema cache number") {
            @Override
            public Long getValue() {
                return schemaCache.estimatedSize();
            }
        };
        schemaCacheGauge.addLabel(new MetricLabel("catalog", catalog.getName()));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(schemaCacheGauge);
    }

    private Optional<SchemaCacheValue> loadSchema(SchemaCacheKey key) {
        Optional<SchemaCacheValue> schema = catalog.getSchema(key);
        if (schema.isPresent()) {
            schema.get().validateSchema();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("load schema for {} in catalog {}", key, catalog.getName());
        }
        return schema;
    }

    public Optional<SchemaCacheValue> getSchemaValue(SchemaCacheKey key) {
        return schemaCache.get(key);
    }

    public void invalidateTableCache(ExternalTable dorisTable) {
        schemaCache.asMap().keySet().stream()
                .filter(key -> key.getNameMapping().getLocalDbName().equals(dorisTable.getDbName())
                        && key.getNameMapping().getLocalTblName().equals(dorisTable.getName()))
                .forEach(schemaCache::invalidate);
    }

    public void invalidateDbCache(String dbName) {
        long start = System.currentTimeMillis();
        Set<SchemaCacheKey> keys = schemaCache.asMap().keySet();
        for (SchemaCacheKey key : keys) {
            if (key.getNameMapping().getLocalDbName().equals(dbName)) {
                schemaCache.invalidate(key);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid schema cache for db {} in catalog {} cost: {} ms", dbName, catalog.getName(),
                    (System.currentTimeMillis() - start));
        }
    }

    public void invalidateAll() {
        schemaCache.invalidateAll();
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid all schema cache in catalog {}", catalog.getName());
        }
    }

    @Data
    public static class SchemaCacheKey {
        private NameMapping nameMapping;

        public SchemaCacheKey(NameMapping nameMapping) {
            this.nameMapping = nameMapping;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SchemaCacheKey)) {
                return false;
            }
            return nameMapping.equals(((SchemaCacheKey) obj).nameMapping);
        }

        @Override
        public int hashCode() {
            return nameMapping.hashCode();
        }

        @Override
        public String toString() {
            return "SchemaCacheKey{" + "dbName='"
                    + nameMapping.getLocalDbName() + '\'' + ", tblName='" + nameMapping.getLocalTblName() + '\'' + '}';
        }
    }
}
