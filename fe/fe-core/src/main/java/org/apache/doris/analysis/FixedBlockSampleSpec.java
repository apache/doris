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

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFixedRowIdBlockSampleParams;
import org.apache.doris.thrift.TKeyRangeBlockSampleParams;
import org.apache.doris.thrift.TTabletSampleOptions;
import org.apache.doris.thrift.TTabletSamplingAlgorithm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** A repeatable sample of physical row blocks in explicitly chosen tablets. */
public final class FixedBlockSampleSpec {
    public static final int MAX_GROUPS = 1024;
    public static final int MAX_TABLETS = 65536;

    public enum Mode {
        FIXED_BLOCK, KEY_RANGE_BLOCK
    }

    public final Mode mode;
    public final long unitRows;
    public final long seed;
    public final List<Group> groups;
    private final Map<Long, Group> byTablet;

    public FixedBlockSampleSpec(long unitRows, long seed, List<Group> groups) {
        this(Mode.FIXED_BLOCK, unitRows, seed, groups);
    }

    public FixedBlockSampleSpec(Mode mode, long unitRows, long seed, List<Group> groups) {
        this.mode = Objects.requireNonNull(mode);
        if (unitRows <= 0 || seed < 0 || groups.isEmpty() || groups.size() > MAX_GROUPS) {
            throw new AnalysisException("Invalid FIXED_BLOCK unit_rows, REPEATABLE seed or group count");
        }
        this.unitRows = unitRows;
        this.seed = seed;
        this.groups = ImmutableList.copyOf(groups);
        Map<Long, Group> index = new LinkedHashMap<>();
        for (Group group : groups) {
            for (long id : group.tablets) {
                if (id <= 0 || index.putIfAbsent(id, group) != null) {
                    throw new AnalysisException("Invalid or duplicate FIXED_BLOCK tablet ID " + id);
                }
                if (index.size() > MAX_TABLETS) {
                    throw new AnalysisException("Too many FIXED_BLOCK tablets (maximum " + MAX_TABLETS + ")");
                }
            }
        }
        this.byTablet = ImmutableMap.copyOf(index);
    }

    /** Validate the bound index before pruning and recheck tablet ownership when building scan ranges. */
    public void validateTablets(OlapTable table, long selectedIndexId, Optional<Boolean> expectTempPartition)
            throws UserException {
        if (mode == Mode.KEY_RANGE_BLOCK && !supportsKeyRange(table, selectedIndexId)) {
            throw new UserException("KEY_RANGE_BLOCK requires a hash-routed MOR or AGG index with its storage keys");
        }
        List<Long> ids = ImmutableList.copyOf(tabletIds());
        List<TabletMeta> metas = Env.getCurrentInvertedIndex().getTabletMetaList(ids);
        for (int pos = 0; pos < ids.size(); pos++) {
            long tabletId = ids.get(pos);
            TabletMeta meta = metas.get(pos);
            if (meta == null || meta == TabletInvertedIndex.NOT_EXIST_TABLET_META
                    || meta.getTableId() != table.getId() || meta.getIndexId() != selectedIndexId) {
                throw staleTablet(table, selectedIndexId, tabletId);
            }
            if (expectTempPartition.isPresent()
                    && table.isTemporaryPartition(meta.getPartitionId()) != expectTempPartition.get()) {
                throw new UserException("Stale FIXED_BLOCK sample plan: tablet " + tabletId
                        + " does not belong to a " + (expectTempPartition.get() ? "temporary" : "normal")
                        + " partition of table " + table.getName());
            }
            Partition part = table.getPartition(meta.getPartitionId());
            MaterializedIndex index = part == null ? null : table.getPartitionIndex(part, selectedIndexId);
            if (index == null || index.getTablet(tabletId) == null) {
                throw staleTablet(table, selectedIndexId, tabletId);
            }
            if (mode == Mode.KEY_RANGE_BLOCK && table.isTemporaryPartition(meta.getPartitionId())
                    && !routesByKeys(table.getIndexMetaByIndexId(selectedIndexId), part.getDistributionInfo())) {
                throw new UserException("KEY_RANGE_BLOCK requires a hash-routed temporary partition");
            }
            if (index.isChangelog()) {
                throw new UserException("FIXED_BLOCK cannot read a changelog index");
            }
        }
    }

    public static boolean supportsKeyRange(OlapTable table, long indexId) {
        MaterializedIndexMeta meta = table.getIndexMetaByIndexId(indexId);
        if (meta == null || !(meta.getKeysType() == KeysType.AGG_KEYS
                || meta.getKeysType() == KeysType.UNIQUE_KEYS && !table.getEnableUniqueKeyMergeOnWrite())
                || table.isZOrderSort() || meta.getSchema().stream().anyMatch(Column::isClusterKey)) {
            return false;
        }
        if (indexId != table.getBaseIndexId()) {
            if (table.getBaseIndexId() < 0) {
                return false;
            }
            // Storage reads hidden columns and column-level sequence mappings by their physical names.
            Set<String> sequenceColumns = table.getColumnSeqMapping().keySet();
            for (Column source : table.getBaseSchema(true)) {
                if ((!source.isVisible() || sequenceColumns.stream().anyMatch(source.getName()::equalsIgnoreCase))
                        && meta.getSchema().stream().noneMatch(target -> target.getName().equalsIgnoreCase(
                                source.getName()) && mapsTo(meta, target, source))) {
                    return false;
                }
            }
        }
        return routesByKeys(meta, table.getDefaultDistributionInfo()) && table.getPartitions().stream()
                .allMatch(part -> routesByKeys(meta, part.getDistributionInfo()));
    }

    public static boolean supportsRandomAgg(OlapTable table, long indexId) {
        MaterializedIndexMeta meta = table.getIndexMetaByIndexId(indexId);
        if (meta == null || meta.getKeysType() != KeysType.AGG_KEYS || table.getKeysType() != KeysType.AGG_KEYS
                || !(table.getDefaultDistributionInfo() instanceof RandomDistributionInfo)
                || table.isZOrderSort() || meta.getSchema().stream().anyMatch(Column::isClusterKey)
                || meta.getSchema().stream().noneMatch(Column::isKey)) {
            return false;
        }
        if (indexId != table.getBaseIndexId() && table.getBaseIndexId() < 0) {
            return false;
        }
        List<Column> baseSchema = indexId == table.getBaseIndexId() ? List.of() : table.getBaseSchema(true);
        for (Column col : meta.getSchema()) {
            if (!col.isKey() && !supportsRandomPreAgg(col)) {
                return false;
            }
            if (indexId != table.getBaseIndexId()
                    && baseSchema.stream().noneMatch(base -> mapsTo(meta, col, base))) {
                return false;
            }
        }
        return table.getPartitions().stream().allMatch(part ->
                part.getDistributionInfo() instanceof RandomDistributionInfo
                        || routesByKeys(meta, part.getDistributionInfo()));
    }

    public static boolean supportsMergedSampling(OlapTable table, long indexId) {
        return supportsKeyRange(table, indexId) || supportsRandomAgg(table, indexId);
    }

    private static boolean supportsRandomPreAgg(Column column) {
        AggregateType agg = column.getAggregationType();
        if (agg == null) {
            return false;
        }
        return switch (agg) {
            case SUM, MAX, MIN, HLL_UNION, BITMAP_UNION, QUANTILE_UNION -> true;
            case GENERIC -> column.getType().isAggStateType();
            default -> false;
        };
    }

    public static boolean retainsPartitionKeys(OlapTable table, MaterializedIndexMeta meta) {
        return table.getPartitionInfo().getPartitionColumns().stream().allMatch(col -> hasKey(meta, col));
    }

    private static boolean routesByKeys(MaterializedIndexMeta meta, DistributionInfo distribution) {
        return distribution instanceof HashDistributionInfo hash && !hash.getDistributionColumns().isEmpty()
                && hash.getDistributionColumns().stream().allMatch(col -> hasKey(meta, col));
    }

    private static boolean hasKey(MaterializedIndexMeta meta, Column base) {
        return meta.getSchema().stream().anyMatch(target -> target.isKey() && mapsTo(meta, target, base));
    }

    private static boolean mapsTo(MaterializedIndexMeta meta, Column target, Column base) {
        if (!target.getType().equals(base.getType()) || target.isAllowNull() != base.isAllowNull()) {
            return false;
        }
        if (target.getDefineExpr() instanceof SlotRef slot) {
            return base.getName().equalsIgnoreCase(slot.getColumnName());
        }
        // ADD ROLLUP copies base columns and may assign them new UIDs. An expression-defined MV
        // requires an explicit direct slot mapping before it can prove identical routing.
        return target == base || (meta.getDefineStmt() == null && target.getDefineExpr() == null
                && target.getName().equalsIgnoreCase(base.getName()));
    }

    private static UserException staleTablet(OlapTable table, long indexId, long tabletId) {
        return new UserException("Stale FIXED_BLOCK sample plan: tablet " + tabletId
                + " no longer belongs to table " + table.getName() + " index " + indexId);
    }

    public Set<Long> tabletIds() {
        return byTablet.keySet();
    }

    public Group groupFor(long id) {
        return byTablet.get(id);
    }

    public TTabletSampleOptions toThrift(long tabletId) {
        Group group = Objects.requireNonNull(groupFor(tabletId));
        TTabletSampleOptions options = new TTabletSampleOptions();
        options.setProtocolVersion(1);
        if (mode == Mode.KEY_RANGE_BLOCK) {
            TKeyRangeBlockSampleParams params = new TKeyRangeBlockSampleParams();
            params.setSampleUnitRows(unitRows);
            params.setMaxSampleUnitsPerTablet(group.maxUnits);
            options.setKeyRangeBlockParams(params);
            options.setAlgorithm(TTabletSamplingAlgorithm.KEY_RANGE_BLOCK);
        } else {
            TFixedRowIdBlockSampleParams params = new TFixedRowIdBlockSampleParams();
            params.setSampleUnitRows(unitRows);
            params.setMaxSampleUnitsPerTablet(group.maxUnits);
            options.setFixedRowidBlockParams(params);
            options.setAlgorithm(TTabletSamplingAlgorithm.FIXED_ROWID_BLOCK);
        }
        options.setIntraTabletUnitProbability(group.unitRate);
        options.setSeed(seed);
        options.setTabletInclusionProbability(group.tabletRate);
        return options;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof FixedBlockSampleSpec)) {
            return false;
        }
        FixedBlockSampleSpec that = (FixedBlockSampleSpec) other;
        return mode == that.mode && unitRows == that.unitRows && seed == that.seed && groups.equals(that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, unitRows, seed, groups);
    }

    @Override
    public String toString() {
        return mode + "(unit_rows=" + unitRows + ", groups=" + groups + ") REPEATABLE " + seed;
    }

    /** Tablet and within-tablet inclusion probabilities apply at different sampling stages. */
    public static final class Group {
        public final double tabletRate;
        public final double unitRate;
        public final long maxUnits;
        public final List<Long> tablets;

        public Group(double tabletRate, double unitRate, long maxUnits, List<Long> tablets) {
            if (!validProbability(tabletRate) || !validProbability(unitRate) || maxUnits <= 0 || tablets.isEmpty()) {
                throw new AnalysisException("Invalid FIXED_BLOCK group probability, max_units or tablet list");
            }
            this.tabletRate = tabletRate;
            this.unitRate = unitRate;
            this.maxUnits = maxUnits;
            this.tablets = ImmutableList.copyOf(tablets);
        }

        private static boolean validProbability(double p) {
            return Double.isFinite(p) && p > 0 && p <= 1;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Group)) {
                return false;
            }
            Group that = (Group) other;
            return tabletRate == that.tabletRate && unitRate == that.unitRate && maxUnits == that.maxUnits
                    && tablets.equals(that.tablets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tabletRate, unitRate, maxUnits, tablets);
        }

        @Override
        public String toString() {
            return "GROUP(tablet_rate=" + tabletRate + ", unit_rate=" + unitRate + ", max_units=" + maxUnits
                    + ", tablets=" + tablets + ")";
        }
    }
}
