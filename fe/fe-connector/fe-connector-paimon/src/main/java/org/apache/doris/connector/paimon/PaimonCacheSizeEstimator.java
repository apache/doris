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
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.iceberg.IcebergTable;
import org.apache.paimon.table.lance.LanceTable;
import org.apache.paimon.table.object.ObjectTable;

import java.net.URI;

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
        long bytes = add(entryOverheadBytes, ReflectiveObjectSizeEstimator.estimateComplete(key));
        bytes = add(bytes, JvmSizeUtils.instanceSize(table.getClass()));
        if (table instanceof FileStoreTable) {
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(fileStoreTable.schema()));
            bytes = add(bytes, estimatePath(fileStoreTable.location()));
            bytes = add(bytes, estimateCatalogEnvironment(fileStoreTable.catalogEnvironment()));
        } else {
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.rowType()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.partitionKeys()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.primaryKeys()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.options()));
            bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(table.comment()));
            bytes = add(bytes, JvmSizeUtils.stringSize(location(table)));
        }
        return MetaCacheSizeEstimate.complete(bytes);
    }

    private static long estimateCatalogEnvironment(CatalogEnvironment environment) {
        if (environment == null) {
            return 0L;
        }
        long bytes = JvmSizeUtils.instanceSize(environment.getClass());
        bytes = add(bytes, ReflectiveObjectSizeEstimator.estimateComplete(environment.identifier()));
        return add(bytes, JvmSizeUtils.stringSize(environment.uuid()));
    }

    private static long estimatePath(Path path) {
        if (path == null) {
            return 0L;
        }
        URI uri = path.toUri();
        long bytes = add(JvmSizeUtils.instanceSize(path.getClass()), JvmSizeUtils.instanceSize(uri.getClass()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.toString()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.getScheme()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.getUserInfo()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.getHost()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.getPath()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.getQuery()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.getFragment()));
        bytes = add(bytes, JvmSizeUtils.stringSize(uri.getAuthority()));
        return add(bytes, JvmSizeUtils.stringSize(uri.getSchemeSpecificPart()));
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
