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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFixedRowIdBlockSampleParams;
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

    public final long unitRows;
    public final long seed;
    public final List<Group> groups;
    private final Map<Long, Group> byTablet;

    public FixedBlockSampleSpec(long unitRows, long seed, List<Group> groups) {
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
            if (index.isChangelog()) {
                throw new UserException("FIXED_BLOCK cannot read a changelog index");
            }
        }
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
        TFixedRowIdBlockSampleParams params = new TFixedRowIdBlockSampleParams();
        params.setSampleUnitRows(unitRows);
        params.setMaxSampleUnitsPerTablet(group.maxUnits);
        TTabletSampleOptions options = new TTabletSampleOptions();
        options.setProtocolVersion(1);
        options.setAlgorithm(TTabletSamplingAlgorithm.FIXED_ROWID_BLOCK);
        options.setIntraTabletUnitProbability(group.unitRate);
        options.setSeed(seed);
        options.setFixedRowidBlockParams(params);
        options.setTabletInclusionProbability(group.tabletRate);
        return options;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof FixedBlockSampleSpec)) {
            return false;
        }
        FixedBlockSampleSpec that = (FixedBlockSampleSpec) other;
        return unitRows == that.unitRows && seed == that.seed && groups.equals(that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unitRows, seed, groups);
    }

    @Override
    public String toString() {
        return "FIXED_BLOCK(unit_rows=" + unitRows + ", groups=" + groups + ") REPEATABLE " + seed;
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
