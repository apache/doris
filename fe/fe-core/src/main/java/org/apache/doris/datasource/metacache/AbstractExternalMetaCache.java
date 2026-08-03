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

package org.apache.doris.datasource.metacache;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.connector.metacache.AbstractMetaCache;
import org.apache.doris.connector.metacache.spi.CacheSpec;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * FE adapter for the data-source-neutral {@link AbstractMetaCache} runtime.
 *
 * <p>This class keeps only dependencies that require FE state: Config-backed defaults,
 * Env catalog lookup, ExternalTable resolution and schema validation.
 */
public abstract class AbstractExternalMetaCache extends AbstractMetaCache implements ExternalMetaCache {
    protected AbstractExternalMetaCache(String engine, ExecutorService refreshExecutor) {
        super(
                engine,
                refreshExecutor,
                Config.external_cache_refresh_time_minutes * 60,
                Config.external_meta_cache_object_entry_lock_stripes);
    }

    protected static CacheSpec defaultEntryCacheSpec() {
        return CacheSpec.of(
                true,
                Config.external_cache_expire_time_seconds_after_access,
                Config.max_external_table_cache_num);
    }

    protected static CacheSpec defaultSchemaCacheSpec() {
        return CacheSpec.of(
                true,
                Config.external_cache_expire_time_seconds_after_access,
                Config.max_external_schema_cache_num);
    }

    protected final boolean matchDb(NameMapping nameMapping, String dbName) {
        return nameMapping.getLocalDbName().equals(dbName);
    }

    protected final boolean matchTable(NameMapping nameMapping, String dbName, String tableName) {
        return matchDb(nameMapping, dbName) && nameMapping.getLocalTblName().equals(tableName);
    }

    protected final ExternalTable findExternalTable(NameMapping nameMapping, String engineNameForError) {
        CatalogIf<?> catalog = getCatalog(nameMapping.getCtlId());
        if (!(catalog instanceof ExternalCatalog)) {
            throw new CacheException("catalog %s is not external when loading %s schema cache",
                    null, nameMapping.getCtlId(), engineNameForError);
        }
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        return externalCatalog.getDb(nameMapping.getLocalDbName())
                .flatMap(db -> db.getTable(nameMapping.getLocalTblName()))
                .orElseThrow(() -> new CacheException(
                        "table %s.%s.%s not found when loading %s schema cache",
                        null, nameMapping.getCtlId(), nameMapping.getLocalDbName(),
                        nameMapping.getLocalTblName(), engineNameForError));
    }

    protected CatalogIf<?> getCatalog(long catalogId) {
        if (Env.getCurrentEnv() == null || Env.getCurrentEnv().getCatalogMgr() == null) {
            return null;
        }
        return Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
    }

    @Override
    protected <K, V> Function<K, V> decorateLoader(Function<K, V> loader, Class<V> valueType) {
        if (loader == null || !SchemaCacheValue.class.isAssignableFrom(valueType)) {
            return loader;
        }
        return key -> {
            V value = loader.apply(key);
            ((SchemaCacheValue) value).validateSchema();
            return value;
        };
    }
}
