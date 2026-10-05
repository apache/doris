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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.ReflectiveObjectSizeEstimator;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.iceberg.IcebergTable;
import org.apache.paimon.table.lance.LanceTable;
import org.apache.paimon.table.object.ObjectTable;

import java.net.URI;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Retained-size formulas for Paimon table-cache entries.
 *
 * <p>The table's shallow size includes references to FileIO, catalog loaders, and lock factories,
 * but their graphs are catalog-scoped executable services rather than entry-owned metadata. Walking
 * those graphs both double-counts shared state and reaches strongly encapsulated JDK objects. The
 * estimator therefore expands only immutable metadata owned by the entry.
 */
final class PaimonCacheSizeEstimator {
    private PaimonCacheSizeEstimator() {
    }

    static MetaCacheSizeEstimate estimateTable(Identifier key, Table table, long entryOverheadBytes) {
        if (table instanceof PrivilegedFileStoreTable) {
            return MetaCacheSizeEstimate.incomplete(
                    "authorization decorators must be applied outside the metadata cache");
        }
        long bytes = add(entryOverheadBytes, ReflectiveObjectSizeEstimator.estimateComplete(key));
        if (table instanceof FileStoreTable) {
            Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
            bytes = add(bytes, estimateFileStoreTable((FileStoreTable) table, visited));
        } else {
            if (!isSupportedNonFileStoreTable(table)) {
                return MetaCacheSizeEstimate.incomplete(
                        "unsupported retained graph for " + table.getClass().getName());
            }
            bytes = add(bytes, JvmSizeUtils.instanceSize(table.getClass()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.rowType()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.partitionKeys()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.primaryKeys()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.options()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.comment()));
            bytes = add(bytes, JvmSizeUtils.stringSize(location(table)));
        }
        return MetaCacheSizeEstimate.complete(bytes);
    }

    private static boolean isSupportedNonFileStoreTable(Table table) {
        return table instanceof FormatTable
                || table instanceof ObjectTable
                || table instanceof LanceTable
                || table instanceof IcebergTable;
    }

    private static long estimateFileStoreTable(FileStoreTable table, Set<Object> visited) {
        if (!visited.add(table)) {
            return 0L;
        }
        long bytes = JvmSizeUtils.instanceSize(table.getClass());
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallback = (FallbackReadFileStoreTable) table;
            bytes = add(bytes, estimateFileStoreTable(fallback.wrapped(), visited));
            return add(bytes, estimateFileStoreTable(fallback.fallback(), visited));
        }
        if (table instanceof DelegatedFileStoreTable) {
            return add(bytes, estimateFileStoreTable(
                    ((DelegatedFileStoreTable) table).wrapped(), visited));
        }
        bytes = add(bytes, estimateCompleteOnce(table.schema(), visited));
        bytes = add(bytes, estimatePath(table.location(), visited));
        return add(bytes, estimateCatalogEnvironment(table.catalogEnvironment(), visited));
    }

    private static long estimateCompleteOnce(Object value, Set<Object> visited) {
        return value == null || !visited.add(value)
                ? 0L : ReflectiveObjectSizeEstimator.estimateComplete(value);
    }

    private static long estimateCatalogEnvironment(CatalogEnvironment environment, Set<Object> visited) {
        if (environment == null || !visited.add(environment)) {
            return 0L;
        }
        long bytes = JvmSizeUtils.instanceSize(environment.getClass());
        bytes = add(bytes, estimateCompleteOnce(environment.identifier(), visited));
        return add(bytes, estimateStringOnce(environment.uuid(), visited));
    }

    private static long estimatePath(Path path, Set<Object> visited) {
        if (path == null || !visited.add(path)) {
            return 0L;
        }
        URI uri = path.toUri();
        long bytes = JvmSizeUtils.instanceSize(path.getClass());
        if (visited.add(uri)) {
            bytes = add(bytes, JvmSizeUtils.instanceSize(uri.getClass()));
            bytes = add(bytes, estimateStringOnce(uri.toString(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getScheme(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getUserInfo(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getHost(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getPath(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getQuery(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getFragment(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getAuthority(), visited));
            bytes = add(bytes, estimateStringOnce(uri.getSchemeSpecificPart(), visited));
        }
        return bytes;
    }

    private static long estimateStringOnce(String value, Set<Object> visited) {
        return value == null || !visited.add(value) ? 0L : JvmSizeUtils.stringSize(value);
    }

    private static String location(Table table) {
        if (table instanceof FormatTable) {
            return ((FormatTable) table).location();
        }
        if (table instanceof ObjectTable) {
            return ((ObjectTable) table).location();
        }
        if (table instanceof LanceTable) {
            return ((LanceTable) table).location();
        }
        if (table instanceof IcebergTable) {
            return ((IcebergTable) table).location();
        }
        return null;
    }

    private static long add(long left, long right) {
        return JvmSizeUtils.saturatedAdd(left, right);
    }
}
