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

package org.apache.doris.datasource.mvcc;

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.ReflectiveObjectSizeEstimator;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccPartition;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One published partition generation: maps and pruning index share the same read-only partition items. */
public final class PluginDrivenPartitionView {
    private final ImmutableMap<String, PartitionItem> items;
    private final ImmutableMap<String, Long> freshness;
    private final SortedPartitionRanges<String> sortedRanges;

    PluginDrivenPartitionView(Map<String, PartitionItem> items, Map<String, Long> freshness) {
        this.items = ImmutableMap.copyOf(items);
        this.freshness = ImmutableMap.copyOf(freshness);
        // Build before cache admission, like HivePartitionValues: the cache accounts for the index
        // together with its maps, and readers never observe a partially initialized generation.
        this.sortedRanges = hasComparableKeys(this.items) ? SortedPartitionRanges.build(this.items) : null;
    }

    // Iceberg display partitions can come from different specs. They remain useful for counts and
    // display, but different tuple shapes/types do not form one ordered pruning index.
    private static boolean hasComparableKeys(Map<String, PartitionItem> items) {
        List<PrimitiveType> types = null;
        for (PartitionItem item : items.values()) {
            if (item instanceof ListPartitionItem) {
                for (PartitionKey key : ((ListPartitionItem) item).getItems()) {
                    if (types == null) {
                        types = key.getTypes();
                    } else if (!types.equals(key.getTypes())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /** Admission-only traversal, bounded per partition rather than per complete table. */
    public static MetaCacheSizeEstimate estimateSize(Key key, PluginDrivenPartitionView view) {
        long bytes = JvmSizeUtils.instanceSize(Key.class);
        bytes = JvmSizeUtils.saturatedAdd(bytes, ReflectiveObjectSizeEstimator.estimateComplete(
                java.util.Arrays.asList(key.database, key.table, key.types, key.timeZone, key.version)));
        bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.objectArraySize(key.partitions.size()));
        for (Object partition : key.partitions) {
            bytes = JvmSizeUtils.saturatedAdd(bytes, ReflectiveObjectSizeEstimator.estimateComplete(partition));
        }
        bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.instanceSize(PluginDrivenPartitionView.class));
        // ImmutableMap tables, entries, and freshness values; shared names/items are counted once below.
        bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.saturatedMultiply(
                view.items.size() + (long) view.freshness.size(), 64L));
        for (Map.Entry<String, PartitionItem> entry : view.items.entrySet()) {
            bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.stringSize(entry.getKey()));
            bytes = JvmSizeUtils.saturatedAdd(bytes, estimatePartitionItem(entry.getValue()));
        }
        for (String name : view.freshness.keySet()) {
            bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.stringSize(name));
        }
        if (view.sortedRanges != null) {
            bytes = JvmSizeUtils.saturatedAdd(bytes,
                    JvmSizeUtils.instanceSize(SortedPartitionRanges.class));
            bytes = JvmSizeUtils.saturatedAdd(bytes,
                    JvmSizeUtils.objectArraySize(view.sortedRanges.sortedPartitions.size()));
            bytes = JvmSizeUtils.saturatedAdd(bytes,
                    JvmSizeUtils.objectArraySize(view.sortedRanges.defaultPartitions.size()));
            // Index ranges keep converted bounds/literals in addition to the shared PartitionItem.
            for (SortedPartitionRanges.PartitionItemAndRange<String> range : view.sortedRanges.sortedPartitions) {
                bytes = JvmSizeUtils.saturatedAdd(bytes, estimatePartitionItem(range.partitionItem));
                bytes = JvmSizeUtils.saturatedAdd(bytes, 128L);
            }
            bytes = JvmSizeUtils.saturatedAdd(bytes,
                    JvmSizeUtils.saturatedMultiply(view.sortedRanges.defaultPartitions.size(), 32L));
        }
        return MetaCacheSizeEstimate.complete(bytes);
    }

    private static long estimatePartitionItem(PartitionItem item) {
        long bytes = JvmSizeUtils.instanceSize(item.getClass());
        if (item instanceof ListPartitionItem) {
            List<PartitionKey> keys = ((ListPartitionItem) item).getItems();
            bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.arrayListSize(keys.size()));
            for (PartitionKey key : keys) {
                bytes = JvmSizeUtils.saturatedAdd(bytes, estimatePartitionKey(key));
            }
        } else {
            com.google.common.collect.Range<PartitionKey> range = ((RangePartitionItem) item).getItems();
            // Range and its two cut objects.
            bytes = JvmSizeUtils.saturatedAdd(bytes, 96L);
            if (range.hasLowerBound()) {
                bytes = JvmSizeUtils.saturatedAdd(bytes, estimatePartitionKey(range.lowerEndpoint()));
            }
            if (range.hasUpperBound()) {
                bytes = JvmSizeUtils.saturatedAdd(bytes, estimatePartitionKey(range.upperEndpoint()));
            }
        }
        return bytes;
    }

    private static long estimatePartitionKey(PartitionKey key) {
        long bytes = JvmSizeUtils.instanceSize(PartitionKey.class);
        // Value, primitive-type and original-value lists. Overcount shared scalar types/strings instead
        // of traversing Expr's memoized supplier (which points back at the expression).
        bytes = JvmSizeUtils.saturatedAdd(bytes,
                JvmSizeUtils.saturatedMultiply(JvmSizeUtils.arrayListSize(key.getKeys().size()), 3L));
        for (org.apache.doris.analysis.LiteralExpr value : key.getKeys()) {
            bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.instanceSize(value.getClass()));
            bytes = JvmSizeUtils.saturatedAdd(bytes,
                    ReflectiveObjectSizeEstimator.estimateComplete(value.getType()));
            // Empty child list, memoizing supplier and its bound supplier, and numeric/date payload.
            bytes = JvmSizeUtils.saturatedAdd(bytes, 128L);
            bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.stringSize(value.getStringValue()));
        }
        return bytes;
    }

    ImmutableMap<String, PartitionItem> getItems() {
        return items;
    }

    ImmutableMap<String, Long> getFreshness() {
        return freshness;
    }

    Optional<SortedPartitionRanges<String>> getSortedRanges() {
        return Optional.ofNullable(sortedRanges);
    }

    /**
     * Uses a connector-certified immutable generation when available, without retaining or visiting
     * partition records. Otherwise matches the actual connector listing rather than an independently
     * refreshed snapshot token.
     * LIST records' conversion fields are immutable; compare only those fields, not unrelated
     * property maps or statistics. This also reuses equal records from uncached connectors.
     * RANGE bounds are defensively copied because their SPI wrappers can
     * retain mutable caller lists. Schema types and session timezone also affect literal conversion.
     */
    public static final class Key {
        private final long tableId;
        private final String database;
        private final String table;
        private final List<?> partitions;
        private final boolean range;
        private final List<TTypeDesc> types;
        private final String timeZone;
        private final int hash;
        private final String version;

        private Key(long tableId, String database, String table, List<?> partitions,
                List<Type> types, boolean range) {
            this(tableId, database, table, partitions, types, range, null);
        }

        private Key(long tableId, String database, String table, List<?> partitions,
                List<Type> types, boolean range, String version) {
            this.version = version;
            this.tableId = tableId;
            this.database = database;
            this.table = table;
            this.partitions = ImmutableList.copyOf(partitions);
            this.range = range;
            this.types = types.stream().map(Type::toThrift).collect(ImmutableList.toImmutableList());
            ConnectContext context = ConnectContext.get();
            // Same fallback as TimestampTzLiteral when parsing an unqualified value.
            this.timeZone = context == null ? "UTC" : context.getSessionVariable().getTimeZone();
            int recordsHash = 1;
            for (Object partition : this.partitions) {
                recordsHash = 31 * recordsHash + (range ? partition.hashCode()
                        : listRecordHash((ConnectorPartitionInfo) partition));
            }
            this.hash = Objects.hash(tableId, database, table, range, this.types, timeZone, version, recordsHash);
        }

        static Key versioned(long tableId, String database, String table,
                String version, List<Type> types, boolean range) {
            return new Key(tableId, database, table, ImmutableList.of(), types, range,
                    Objects.requireNonNull(version, "version"));
        }

        static Key list(long tableId, String database, String table,
                List<ConnectorPartitionInfo> partitions, List<Type> types) {
            return new Key(tableId, database, table, partitions, types, false);
        }

        static Key range(long tableId, String database, String table,
                List<ConnectorMvccPartition> partitions, List<Type> types) {
            List<ConnectorMvccPartition> frozen = partitions.stream().map(partition -> new ConnectorMvccPartition(
                    partition.getName(), ImmutableList.copyOf(partition.getLowerBound()),
                    ImmutableList.copyOf(partition.getUpperBound()), partition.getFreshnessValue()))
                    .collect(ImmutableList.toImmutableList());
            return new Key(tableId, database, table, frozen, types, true);
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        @SuppressWarnings("unchecked")
        List<ConnectorPartitionInfo> listPartitions() {
            return (List<ConnectorPartitionInfo>) partitions;
        }

        @SuppressWarnings("unchecked")
        List<ConnectorMvccPartition> rangePartitions() {
            return (List<ConnectorMvccPartition>) partitions;
        }

        List<Type> frozenTypes() {
            return types.stream().map(type -> {
                // Type.fromThrift does not retain TIMESTAMPTZ scale or VARBINARY length.
                // Preserve all scalar parameters when freezing the conversion's schema.
                if (type.getTypes().get(0).isSetScalarType()) {
                    TScalarType scalar = type.getTypes().get(0).getScalarType();
                    return ScalarType.createType(PrimitiveType.fromThrift(scalar.getType()),
                            scalar.getLen(), scalar.getPrecision(), scalar.getScale());
                }
                return Type.fromThrift(type);
            }).collect(ImmutableList.toImmutableList());
        }

        private static int listRecordHash(ConnectorPartitionInfo partition) {
            int result = partition.getPartitionName().hashCode();
            result = 31 * result + partition.getOrderedPartitionValues().hashCode();
            result = 31 * result + partition.getPartitionValueNullFlags().hashCode();
            result = 31 * result + partition.getPartitionValueTypes().hashCode();
            return 31 * result + Long.hashCode(partition.getLastModifiedMillis());
        }

        private static boolean sameListRecord(ConnectorPartitionInfo left, ConnectorPartitionInfo right) {
            return left == right || (left.getLastModifiedMillis() == right.getLastModifiedMillis()
                    && left.getPartitionName().equals(right.getPartitionName())
                    && left.getOrderedPartitionValues().equals(right.getOrderedPartitionValues())
                    && left.getPartitionValueNullFlags().equals(right.getPartitionValueNullFlags())
                    && left.getPartitionValueTypes().equals(right.getPartitionValueTypes()));
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Key)) {
                return false;
            }
            Key that = (Key) other;
            if (hash != that.hash || tableId != that.tableId || range != that.range
                    || !Objects.equals(version, that.version)
                    || !Objects.equals(database, that.database) || !Objects.equals(table, that.table)
                    || !types.equals(that.types) || !timeZone.equals(that.timeZone)
                    || partitions.size() != that.partitions.size()) {
                return false;
            }
            for (int i = 0; i < partitions.size(); i++) {
                if (range ? !partitions.get(i).equals(that.partitions.get(i))
                        : !sameListRecord((ConnectorPartitionInfo) partitions.get(i),
                                (ConnectorPartitionInfo) that.partitions.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }
}
