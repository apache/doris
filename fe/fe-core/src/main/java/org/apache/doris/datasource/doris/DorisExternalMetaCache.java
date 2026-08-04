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
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Remote Doris engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code backends}: remote backend topology keyed by catalog id</li>
 *   <li>{@code schema}: schema cache keyed by {@link SchemaCacheKey}</li>
 * </ul>
 *
 * <p>The backend cache is intentionally independent from table/db invalidation and can be
 * refreshed explicitly via {@link #invalidateBackendCache(long)}.
 *
 * <p>db/table/partition invalidation only targets schema entries.
 */
public class DorisExternalMetaCache extends AbstractExternalMetaCache {
    private static final Logger LOG = LogManager.getLogger(DorisExternalMetaCache.class);

    public static final String ENGINE = "doris";
    public static final String ENTRY_BACKENDS = "backends";
    public static final String ENTRY_SCHEMA = "schema";
    @SuppressWarnings("unchecked")
    private static final Class<ImmutableMap<Long, Backend>> BACKEND_MAP_CLASS =
            (Class<ImmutableMap<Long, Backend>>) (Class<?>) ImmutableMap.class;
    private static final String BACKEND_ENTRY_KEY = "backends";

    private final EntryHandle<String, ImmutableMap<Long, Backend>> backendsEntry;
    private final EntryHandle<SchemaCacheKey, SchemaCacheValue> schemaEntry;

    public DorisExternalMetaCache(ExecutorService refreshExecutor) {
        super(ENGINE, refreshExecutor);
        backendsEntry = registerEntry(MetaCacheEntryDef.contextualOnly(
                ENTRY_BACKENDS,
                String.class,
                BACKEND_MAP_CLASS,
                CacheSpec.of(true, Config.external_cache_expire_time_seconds_after_access, 20)));
        schemaEntry = registerEntry(MetaCacheEntryDef.of(
                ENTRY_SCHEMA,
                SchemaCacheKey.class,
                SchemaCacheValue.class,
                this::loadSchemaCacheValue,
                defaultSchemaCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(SchemaCacheKey::getNameMapping)));
    }

    @Override
    public Collection<String> aliases() {
        return Collections.singleton("external_doris");
    }

    public ImmutableMap<Long, Backend> getBackends(long catalogId) {
        ImmutableMap<Long, Backend> backends = backendsEntry.get(catalogId)
                .get(BACKEND_ENTRY_KEY, ignored -> loadBackends(catalogId));
        return backends == null ? ImmutableMap.of() : backends;
    }

    public void invalidateBackendCache(long catalogId) {
        MetaCacheEntry<String, ImmutableMap<Long, Backend>> backends = backendsEntry.getIfInitialized(catalogId);
        if (backends != null) {
            backends.invalidateKey(BACKEND_ENTRY_KEY);
        }
    }

    private ImmutableMap<Long, Backend> loadBackends(long catalogId) {
        RemoteDorisExternalCatalog catalog = (RemoteDorisExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(catalogId);
        List<Backend> backends = catalog.getFeServiceClient().listBackends();
        if (LOG.isDebugEnabled()) {
            List<String> names = backends.stream().map(Backend::getAddress).collect(Collectors.toList());
            LOG.debug("load backends:{} from:{}", String.join(",", names), catalog.getName());
        }
        Map<Long, Backend> backendMap = Maps.newHashMap();
        backends.forEach(backend -> backendMap.put(backend.getId(), backend));
        return ImmutableMap.copyOf(backendMap);
    }

    private SchemaCacheValue loadSchemaCacheValue(SchemaCacheKey key) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        return dorisTable.initSchemaAndUpdateTime(key).orElseThrow(() ->
                new CacheException("failed to load doris schema cache value for: %s.%s.%s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName()));
    }

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        return singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA);
    }
}
