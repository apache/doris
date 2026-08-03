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

package org.apache.doris.connector.metacache.spi;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Data-source-neutral lifecycle contract for one engine's metadata cache.
 *
 * <p>The contract deliberately excludes cache implementation types and FE state.
 * Entry lookup belongs to the runtime or an engine-specific adapter.
 */
public interface MetaCacheLifecycle extends AutoCloseable {
    String engine();

    default Collection<String> aliases() {
        return Collections.singleton(engine());
    }

    void initCatalog(long catalogId, Map<String, String> catalogProperties);

    void checkCatalogInitialized(long catalogId);

    boolean isCatalogInitialized(long catalogId);

    void invalidateCatalog(long catalogId);

    default void invalidateCatalogEntries(long catalogId) {
        invalidateCatalog(catalogId);
    }

    void invalidateDb(long catalogId, String dbName);

    void invalidateTable(long catalogId, String dbName, String tableName);

    void invalidatePartitions(long catalogId, String dbName, String tableName, List<String> partitions);

    Map<String, MetaCacheEntryStats> stats(long catalogId);

    @Override
    void close();
}
