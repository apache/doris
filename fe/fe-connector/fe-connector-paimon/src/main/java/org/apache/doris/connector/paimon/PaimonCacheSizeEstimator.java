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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.iceberg.IcebergTable;
import org.apache.paimon.table.lance.LanceTable;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import java.net.URI;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Retained-size formulas for Paimon table-cache entries.
 *
 * <p>The table's shallow size includes references to FileIO, catalog loaders, and lock factories,
 * but their graphs are catalog-scoped executable services rather than entry-owned metadata. Walking
 * those graphs both double-counts shared state and reaches strongly encapsulated JDK objects. The
 * estimator therefore expands only immutable metadata owned by the entry.
 *
 * <p>A cached value is weighed once, at admission, but a {@link FileStoreTable} keeps growing afterwards: the
 * first {@code latestSnapshot()} or scan builds its transient store (a {@code FileStore} holding its own
 * {@code RowType} copies of the schema and a copy of the options) and field lookups build the four lazy index
 * maps of every {@code RowType}. That growth is reserved from the schema's shape. On Paimon 1.4.2 it measures
 * about 1-2 KB per table, 200-270 bytes per field, another 200 bytes per field nested in a row and 40 bytes per
 * option; the constants below keep a margin above that.
 */
final class PaimonCacheSizeEstimator {
    private static final long STORE_GROWTH_BASE_BYTES = 2048L;
    private static final long STORE_GROWTH_FIELD_BYTES = 320L;
    private static final long STORE_GROWTH_NESTED_FIELD_BYTES = 256L;
    // A child type of an array, map, multiset or vector: the store's copy of it.
    private static final long STORE_GROWTH_TYPE_NODE_BYTES = 128L;
    private static final long STORE_GROWTH_OPTION_BYTES = 64L;
    // A partition, primary or bucket key: its slot in the derived key types; a primary key is also copied as a
    // renamed key field.
    private static final long STORE_GROWTH_KEY_BYTES = 192L;
    // RESTCatalog with data tokens gives each table its own RESTTokenFileIO. The FileIO it delegates to lives in
    // Paimon's process-wide cache, but the vended token it fetches on first data access (an STS key id, secret and
    // security token, about 2 KB) belongs to the table and arrives after admission. A refresh replaces that token
    // instead of adding to it, and Paimon gives no hook to reweigh the table when it does, so the reserve is a
    // fixed 16 KB, several times any STS token seen so far.
    private static final long REST_TOKEN_FILE_IO_BYTES =
            JvmSizeUtils.saturatedAdd(JvmSizeUtils.instanceSize(RESTTokenFileIO.class), 16L * 1024L);

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
            return add(bytes, estimateFileStoreTable(fallback.other(), visited));
        }
        if (table instanceof DelegatedFileStoreTable) {
            return add(bytes, estimateFileStoreTable(
                    ((DelegatedFileStoreTable) table).wrapped(), visited));
        }
        bytes = add(bytes, estimateCompleteOnce(table.schema(), visited));
        bytes = add(bytes, estimateStoreGrowth(table.schema()));
        bytes = add(bytes, estimateTableOwnedFileIO(table.fileIO(), visited));
        bytes = add(bytes, estimatePath(table.location(), visited));
        return add(bytes, estimateCatalogEnvironment(table.catalogEnvironment(), visited));
    }

    /** Reserves the store and index graph this table builds after admission; no IO, no store access. */
    private static long estimateStoreGrowth(TableSchema schema) {
        long bytes = add(STORE_GROWTH_BASE_BYTES,
                multiply(schema.options().size(), STORE_GROWTH_OPTION_BYTES));
        long keys = (long) schema.partitionKeys().size() + schema.primaryKeys().size() + schema.bucketKeys().size();
        bytes = add(bytes, multiply(keys, STORE_GROWTH_KEY_BYTES));
        for (String primaryKey : schema.primaryKeys()) {
            bytes = add(bytes, JvmSizeUtils.stringSize(primaryKey));
        }
        return add(bytes, estimateFieldGrowth(schema.fields(), STORE_GROWTH_FIELD_BYTES));
    }

    private static long estimateFieldGrowth(List<DataField> fields, long perFieldBytes) {
        long bytes = multiply(fields.size(), perFieldBytes);
        for (DataField field : fields) {
            bytes = add(bytes, estimateTypeGrowth(field.type()));
        }
        return bytes;
    }

    private static long estimateTypeGrowth(DataType type) {
        if (type instanceof RowType) {
            return estimateFieldGrowth(((RowType) type).getFields(),
                    STORE_GROWTH_FIELD_BYTES + STORE_GROWTH_NESTED_FIELD_BYTES);
        }
        if (type instanceof ArrayType) {
            return estimateChildTypeGrowth(((ArrayType) type).getElementType());
        }
        if (type instanceof MultisetType) {
            return estimateChildTypeGrowth(((MultisetType) type).getElementType());
        }
        if (type instanceof VectorType) {
            return estimateChildTypeGrowth(((VectorType) type).getElementType());
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return add(estimateChildTypeGrowth(mapType.getKeyType()),
                    estimateChildTypeGrowth(mapType.getValueType()));
        }
        return 0L;
    }

    private static long estimateChildTypeGrowth(DataType child) {
        return add(STORE_GROWTH_TYPE_NODE_BYTES, estimateTypeGrowth(child));
    }

    private static long estimateTableOwnedFileIO(FileIO fileIO, Set<Object> visited) {
        return fileIO instanceof RESTTokenFileIO && visited.add(fileIO) ? REST_TOKEN_FILE_IO_BYTES : 0L;
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

    private static long multiply(long left, long right) {
        return JvmSizeUtils.saturatedMultiply(left, right);
    }
}
