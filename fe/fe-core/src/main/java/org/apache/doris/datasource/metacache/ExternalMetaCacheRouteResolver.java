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

import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Resolves which engine caches should participate for a catalog lifecycle event.
 */
public class ExternalMetaCacheRouteResolver {
    private static final String ENGINE_DEFAULT = "default";
    private static final String ENGINE_HIVE = "hive";
    private static final String ENGINE_HUDI = "hudi";
    private static final String ENGINE_ICEBERG = "iceberg";
    private static final String ENGINE_PAIMON = "paimon";
    private static final String ENGINE_MAXCOMPUTE = "maxcompute";
    private static final String ENGINE_DORIS = "doris";

    private final ExternalMetaCacheRegistry registry;

    public ExternalMetaCacheRouteResolver(ExternalMetaCacheRegistry registry) {
        this.registry = registry;
    }

    public List<ExternalMetaCache> resolveCatalogCaches(long catalogId, @Nullable CatalogIf<?> catalog) {
        Set<ExternalMetaCache> resolved = new LinkedHashSet<>();
        if (catalog != null) {
            addBuiltinRoutes(resolved, catalog);
            return new ArrayList<>(resolved);
        }
        registry.allCaches().forEach(cache -> {
            if (cache.isCatalogInitialized(catalogId)) {
                resolved.add(cache);
            }
        });
        return new ArrayList<>(resolved);
    }

    private void addBuiltinRoutes(Set<ExternalMetaCache> resolved, CatalogIf<?> catalog) {
        if (catalog instanceof IcebergExternalCatalog) {
            resolved.add(registry.resolve(ENGINE_ICEBERG));
            return;
        }
        if (catalog instanceof PaimonExternalCatalog) {
            resolved.add(registry.resolve(ENGINE_PAIMON));
            return;
        }
        if (catalog instanceof MaxComputeExternalCatalog) {
            resolved.add(registry.resolve(ENGINE_MAXCOMPUTE));
            return;
        }
        if (catalog instanceof RemoteDorisExternalCatalog) {
            resolved.add(registry.resolve(ENGINE_DORIS));
            return;
        }
        if (catalog instanceof HMSExternalCatalog) {
            resolved.add(registry.resolve(ENGINE_HIVE));
            resolved.add(registry.resolve(ENGINE_HUDI));
            resolved.add(registry.resolve(ENGINE_ICEBERG));
            return;
        }
        if (catalog instanceof ExternalCatalog) {
            resolved.add(registry.resolve(ENGINE_DEFAULT));
        }
    }
}
