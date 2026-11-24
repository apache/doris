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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.thrift.TFixedRowIdBlockSampleParams;
import org.apache.doris.thrift.TKeyRangeBlockSampleParams;
import org.apache.doris.thrift.TKeyRangeBound;
import org.apache.doris.thrift.TKeyRangeInterval;
import org.apache.doris.thrift.TTabletSampleOptions;
import org.apache.doris.thrift.TTabletSamplingAlgorithm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;

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
        boolean hashKeyRange = mode == Mode.KEY_RANGE_BLOCK && supportsKeyRange(table, selectedIndexId);
        boolean randomAggKeyRange = mode == Mode.KEY_RANGE_BLOCK && !hashKeyRange
                && supportsRandomAgg(table, selectedIndexId);
        if (mode == Mode.KEY_RANGE_BLOCK && !hashKeyRange && !randomAggKeyRange) {
            throw new UserException(
                    "KEY_RANGE_BLOCK requires a hash-routed MOR/AGG index or pre-aggregatable RANDOM AGG index");
        }
        List<Long> ids = ImmutableList.copyOf(tabletIds());
        List<TabletMeta> metas = Env.getCurrentInvertedIndex().getTabletMetaList(ids);
        Set<Long> checkedRandomPartitions = new HashSet<>();
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
            if (mode == Mode.KEY_RANGE_BLOCK && table.isTemporaryPartition(meta.getPartitionId())) {
                MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(selectedIndexId);
                boolean validTemp = routesByKeys(indexMeta, part.getDistributionInfo())
                        || (supportsPerTabletAggMv(table, selectedIndexId, indexMeta)
                                && (part.getDistributionInfo() instanceof HashDistributionInfo
                                        || table.getKeysType() == KeysType.DUP_KEYS))
                        || (randomAggKeyRange && part.getDistributionInfo() instanceof RandomDistributionInfo);
                if (!validTemp) {
                    throw new UserException("KEY_RANGE_BLOCK requires a compatible temporary partition distribution");
                }
            }
            if (mode == Mode.KEY_RANGE_BLOCK && part.getDistributionInfo() instanceof RandomDistributionInfo
                    && checkedRandomPartitions.add(part.getId())) {
                Group expectedGroup = groupFor(tabletId);
                for (Tablet partTablet : index.getTablets()) {
                    if (groupFor(partTablet.getId()) != expectedGroup) {
                        throw new UserException("KEY_RANGE_BLOCK on RANDOM partition " + part.getName()
                                + " requires all partition tablets in the same sample group");
                    }
                }
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
                || table.isZOrderSort() || meta.getSchema().stream().anyMatch(Column::isClusterKey)
                || !hasOrderedShortKeyPrefix(meta)) {
            return false;
        }
        if (indexId != table.getBaseIndexId()) {
            if (table.getBaseIndexId() < 0) {
                return false;
            }
            if (meta.getKeysType() == KeysType.UNIQUE_KEYS) {
                // Storage reads MOR merge hidden columns and column-level sequence mappings by physical name;
                // __DORIS_VERSION_COL__ and __DORIS_ROW_STORE_COL__ are base-only and unused by MOR scans.
                Set<String> sequenceColumns = table.getColumnSeqMapping().keySet();
                for (Column source : table.getBaseSchema(true)) {
                    boolean requiredMergeColumn = (!source.isVisible()
                            && !source.isVersionColumn()
                            && !source.isRowStoreColumn())
                            || sequenceColumns.stream().anyMatch(source.getName()::equalsIgnoreCase);
                    if (requiredMergeColumn && meta.getSchema().stream().noneMatch(target ->
                            target.getName().equalsIgnoreCase(source.getName()) && mapsTo(meta, target, source))) {
                        return false;
                    }
                }
            }
        }
        return routesIndexByKeys(table, meta) || supportsPerTabletAggMv(table, indexId, meta);
    }

    public static boolean supportsRandomAgg(OlapTable table, long indexId) {
        MaterializedIndexMeta meta = table.getIndexMetaByIndexId(indexId);
        if (meta == null || meta.getKeysType() != KeysType.AGG_KEYS || table.getKeysType() != KeysType.AGG_KEYS
                || !(table.getDefaultDistributionInfo() instanceof RandomDistributionInfo)
                || table.hasRowTtl() || table.isZOrderSort()
                || meta.getSchema().stream().anyMatch(Column::isClusterKey)
                || meta.getSchema().stream().noneMatch(Column::isKey)
                || !hasOrderedShortKeyPrefix(meta)) {
            return false;
        }
        if (indexId != table.getBaseIndexId() && table.getBaseIndexId() < 0) {
            return false;
        }
        for (Column col : meta.getSchema()) {
            if (!col.isKey() && !supportsRandomPreAgg(col)) {
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

    public static boolean hasOrderedShortKeyPrefix(MaterializedIndexMeta meta) {
        if (meta == null) {
            return false;
        }
        List<Column> schema = meta.getSchema();
        int shortKeys = Math.min(meta.getShortKeyColumnCount(), schema.size());
        for (int i = 0; i + 1 < shortKeys; i++) {
            PrimitiveType type = schema.get(i).getDataType();
            if (type == PrimitiveType.VARCHAR || type == PrimitiveType.STRING) {
                return false;
            }
        }
        return true;
    }

    public static boolean routesIndexByKeys(OlapTable table, MaterializedIndexMeta meta) {
        return meta != null && routesByKeys(meta, table.getDefaultDistributionInfo())
                && table.getPartitions().stream().allMatch(part -> routesByKeys(meta, part.getDistributionInfo()));
    }

    public static boolean guaranteesGlobalKeyUniqueness(OlapTable table, MaterializedIndexMeta meta, long indexId) {
        if (meta == null || !retainsPartitionKeys(table, meta)) {
            return false;
        }
        return meta.getKeysType() != KeysType.AGG_KEYS
                || routesIndexByKeys(table, meta)
                || supportsRandomAgg(table, indexId);
    }

    private static boolean supportsPerTabletAggMv(OlapTable table, long indexId, MaterializedIndexMeta meta) {
        if (meta == null || indexId == table.getBaseIndexId() || meta.getKeysType() != KeysType.AGG_KEYS
                || table.getKeysType() == null) {
            return false;
        }
        if (table.getKeysType() == KeysType.AGG_KEYS
                && table.getDefaultDistributionInfo() instanceof RandomDistributionInfo) {
            return false;
        }
        return table.getPartitions().stream().allMatch(part ->
                part.getDistributionInfo() instanceof HashDistributionInfo
                        || (table.getKeysType() == KeysType.DUP_KEYS
                                && part.getDistributionInfo() instanceof RandomDistributionInfo));
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
        if (!compatibleSlotType(target.getType(), base.getType()) || target.isAllowNull() != base.isAllowNull()) {
            return false;
        }
        if (target.getDefineExpr() instanceof SlotRef slot) {
            return base.getName().equalsIgnoreCase(slot.getColumnName());
        }
        if (!base.isVisible() && !target.isVisible() && target.getDefineExpr() == null) {
            return target.getName().equalsIgnoreCase(base.getName());
        }
        // ADD ROLLUP copies base columns and may assign them new UIDs. An expression-defined MV
        // requires an explicit direct slot mapping before it can prove identical routing.
        return target == base || (meta.getDefineStmt() == null && target.getDefineExpr() == null
                && target.getName().equalsIgnoreCase(base.getName()));
    }

    private static boolean compatibleSlotType(Type target, Type base) {
        if (target.equals(base)) {
            return true;
        }
        // Sync MVs normalize string SlotRef projections to setMaxLength() (len = -1) while keeping
        // the same PrimitiveType, which is lossless whenever target length >= base length.
        if (target instanceof ScalarType targetScalar && base instanceof ScalarType baseScalar
                && targetScalar.getPrimitiveType() == baseScalar.getPrimitiveType()
                && targetScalar.isStringType()) {
            return targetScalar.getLength() >= baseScalar.getLength();
        }
        return false;
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

    public static void applyEmptySharedIntervals(TTabletSampleOptions options) {
        TKeyRangeBlockSampleParams params = Objects.requireNonNull(options.getKeyRangeBlockParams());
        params.setSharedIntervals(List.of());
        params.setSharedPopulationUnits(0L);
        params.setSharedSelectedUnits(0L);
    }

    public static void applySharedIntervals(TTabletSampleOptions options,
            InternalService.PSampleTabletKeyRangesResponse response) {
        TKeyRangeBlockSampleParams params = Objects.requireNonNull(options.getKeyRangeBlockParams());
        List<TKeyRangeInterval> intervals = new ArrayList<>(response.getIntervalsCount());
        for (InternalService.PKeyRangeInterval src : response.getIntervalsList()) {
            TKeyRangeInterval dst = new TKeyRangeInterval();
            if (src.hasLower()) {
                dst.setLower(toThriftBound(src.getLower()));
            }
            if (src.hasUpper()) {
                dst.setUpper(toThriftBound(src.getUpper()));
            }
            intervals.add(dst);
        }
        params.setSharedIntervals(intervals);
        params.setSharedPopulationUnits(response.getPopulationUnits());
        params.setSharedSelectedUnits(response.getSelectedUnits());
    }

    /**
     * Merges weighted start-bound chunks across backends of a RANDOM partition into equal-mass
     * partition-wide key intervals and draws a repeatable SRSWOR subset at {@code probability}.
     */
    public static InternalService.PSampleTabletKeyRangesResponse mergeAndSampleSharedKeyRanges(
            List<InternalService.PSampleTabletKeyRangesResponse> slices, long domainId, long unitRows,
            double probability, long maxUnits, long seed) throws UserException {
        record WeightedCut(InternalService.PKeyRangeBound bound, long weight) {
        }

        long totalRows = 0L;
        List<WeightedCut> raw = new ArrayList<>();
        for (InternalService.PSampleTabletKeyRangesResponse slice : slices) {
            totalRows += slice.getTotalRows();
            for (InternalService.PKeyRangeWeightedCut wc : slice.getBoundaryCutsList()) {
                if (wc.getWeight() > 0 && wc.hasBound()) {
                    raw.add(new WeightedCut(wc.getBound(), wc.getWeight()));
                }
            }
        }
        List<InternalService.PKeyRangeBound> cuts = new ArrayList<>();
        if (totalRows > unitRows && !raw.isEmpty()) {
            raw.sort((a, b) -> compareBounds(a.bound(), b.bound()));
            List<WeightedCut> coalesced = new ArrayList<>(raw.size());
            for (WeightedCut cur : raw) {
                if (!coalesced.isEmpty()
                        && compareBounds(coalesced.get(coalesced.size() - 1).bound(), cur.bound()) == 0) {
                    WeightedCut prev = coalesced.get(coalesced.size() - 1);
                    coalesced.set(coalesced.size() - 1, new WeightedCut(prev.bound(), prev.weight() + cur.weight()));
                } else {
                    coalesced.add(cur);
                }
            }
            long effectiveUnit = Math.max(unitRows, (totalRows + 4095L) / 4096L);
            long accumulated = 0L;
            for (WeightedCut wc : coalesced) {
                if (accumulated >= effectiveUnit) {
                    cuts.add(wc.bound());
                    accumulated = accumulated < 2L * effectiveUnit ? accumulated - effectiveUnit : 0L;
                }
                accumulated += wc.weight();
            }
        }
        int population = 1 + cuts.size();
        int selected = roundCumulative(probability, seed, domainId, population);
        if (selected > maxUnits) {
            throw new UserException("tablet_id=" + domainId + " population_units=" + population
                    + " selected_units=" + selected + " exceed cap=" + maxUnits);
        }
        SplittableRandom rng = new SplittableRandom(mix64(mix64(seed ^ 0x4b52425f4d455247L) + domainId));
        List<int[]> runs = new ArrayList<>();
        int seats = selected;
        for (int i = 0; seats > 0 && i < population; i++) {
            if (seats == population - i || rng.nextInt(population - i) < seats) {
                if (!runs.isEmpty() && runs.get(runs.size() - 1)[1] == i) {
                    runs.get(runs.size() - 1)[1]++;
                } else {
                    runs.add(new int[] {i, i + 1});
                }
                seats--;
            }
        }
        InternalService.PSampleTabletKeyRangesResponse.Builder out =
                InternalService.PSampleTabletKeyRangesResponse.newBuilder()
                        .setPopulationUnits(population)
                        .setSelectedUnits(selected);
        for (int[] run : runs) {
            InternalService.PKeyRangeInterval.Builder iv = InternalService.PKeyRangeInterval.newBuilder();
            if (run[0] > 0) {
                iv.setLower(cuts.get(run[0] - 1));
            }
            if (run[1] < population) {
                iv.setUpper(cuts.get(run[1] - 1));
            }
            out.addIntervals(iv);
        }
        return out.build();
    }

    private static int compareBounds(InternalService.PKeyRangeBound a, InternalService.PKeyRangeBound b) {
        Comparator<ByteString> unsigned = ByteString.unsignedLexicographicalComparator();
        int cmp = unsigned.compare(a.getPrefix(), b.getPrefix());
        if (cmp != 0) {
            return cmp;
        }
        int common = Math.min(a.getTupleCount(), b.getTupleCount());
        for (int i = 0; i < common; i++) {
            cmp = unsigned.compare(a.getTuple(i), b.getTuple(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.getTupleCount(), b.getTupleCount());
    }

    private static int roundCumulative(double probability, long seed, long domainId, int population) {
        if (population <= 0 || probability <= 0.0) {
            return 0;
        }
        if (probability >= 1.0) {
            return population;
        }
        int fractionBits = 1074;
        long bits = Double.doubleToRawLongBits(probability);
        int exponent = (int) (bits >>> 52);
        long mantissa = bits & ((1L << 52) - 1L);
        BigInteger rate = exponent == 0 ? BigInteger.valueOf(mantissa)
                : BigInteger.valueOf(mantissa | (1L << 52)).shiftLeft(exponent - 1);
        long uBits = mix64(mix64(seed ^ 0x4445505f524e4431L) + domainId);
        BigInteger remainder = BigInteger.valueOf(uBits >>> 1).shiftLeft(1)
                .add(BigInteger.valueOf(uBits & 1L))
                .shiftLeft(fractionBits - Long.SIZE);
        return remainder.add(rate.multiply(BigInteger.valueOf(population)))
                .shiftRight(fractionBits)
                .intValueExact();
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    private static TKeyRangeBound toThriftBound(InternalService.PKeyRangeBound src) {
        TKeyRangeBound bound = new TKeyRangeBound();
        bound.setPrefix(src.getPrefix().asReadOnlyByteBuffer());
        if (src.getTupleCount() > 0) {
            List<ByteBuffer> tuple = new ArrayList<>(src.getTupleCount());
            for (ByteString field : src.getTupleList()) {
                tuple.add(field.asReadOnlyByteBuffer());
            }
            bound.setTuple(tuple);
        }
        return bound;
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
