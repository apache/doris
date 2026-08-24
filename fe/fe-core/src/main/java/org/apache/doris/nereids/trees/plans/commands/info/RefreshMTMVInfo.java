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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * refresh mtmv info
 */
public class RefreshMTMVInfo {
    private final TableNameInfo mvName;
    private List<String> partitions;
    private boolean isComplete;

    public RefreshMTMVInfo(TableNameInfo mvName, List<String> partitions, boolean isComplete) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.partitions = Utils.copyRequiredList(partitions);
        this.isComplete = Objects.requireNonNull(isComplete, "require isComplete object");
    }

    /**
     * analyze refresh info
     *
     * @param ctx ConnectContext
     */
    public void analyze(ConnectContext ctx) {
        mvName.analyze(ctx.getNameSpaceContext());
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, mvName.getCtl(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.CREATE)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("CREATE",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(),
                    mvName.getDb() + ": " + mvName.getTbl());
            throw new AnalysisException(message);
        }
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(mvName.getDb());
            MTMV mtmv = (MTMV) db.getTableOrMetaException(mvName.getTbl(), TableType.MATERIALIZED_VIEW);
            if (!CollectionUtils.isEmpty(partitions)) {
                checkPartitionExist(mtmv);
            }
        } catch (org.apache.doris.common.AnalysisException | MetaNotFoundException | DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    private void checkPartitionExist(MTMV mtmv) throws org.apache.doris.common.AnalysisException {
        Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
        List<TableIf> tables = Lists.newArrayList(pctTables);
        tables.add(mtmv);
        tables.sort(Comparator.comparing(TableIf::getId));
        MetaLockUtils.readLockTables(tables);
        try {
            if (mtmv.getMvPartitionInfo().getPartitionType().equals(MTMVPartitionType.SELF_MANAGE)) {
                throw new AnalysisException(
                        "The partition method of this asynchronous materialized view "
                                + "does not support refreshing by partition");
            }
            // First validate against the real physical partition names already stored in the MTMV metadata.
            // SHOW PARTITIONS returns these names, and MVs created before partition name generation was made
            // deterministic may carry a historical time suffix, so regenerating names here could produce a
            // different string than the stored one and wrongly reject a valid refresh request.
            Set<String> existPartitionNames = mtmv.getPartitionNames();
            // Secondly validate against the partition names that would be generated (and aligned) from the
            // related base table partition descs, so that refreshing a not-yet-created partition is allowed.
            Set<PartitionKeyDesc> relatedPartitionDescs = MTMVPartitionUtil.generateRelatedPartitionDescs(
                    mtmv.getMvPartitionInfo(), mtmv.getMvProperties(), mtmv.getPartitionColumns(),
                    Maps.newHashMap()).keySet();
            // Index every related partition desc by its generated (regenerated/alias) name so each requested
            // alias below is resolved in O(1). Rescanning relatedPartitionDescs for every alias would
            // re-serialize every descriptor (including SHA-256 work for long names) per alias, which is
            // O(n * m) while the MTMV and all PCT tables stay read-locked. Constructing the index is also
            // the right place to reject duplicate generated names: two distinct descriptors mapping to the
            // same name would make the alias ambiguous.
            Map<String, PartitionKeyDesc> generatedNameToDesc = Maps.newHashMap();
            for (PartitionKeyDesc desc : relatedPartitionDescs) {
                String generatedName = MTMVPartitionUtil.generatePartitionName(desc);
                PartitionKeyDesc previous = generatedNameToDesc.putIfAbsent(generatedName, desc);
                if (previous != null) {
                    throw new org.apache.doris.common.AnalysisException(
                            "duplicate generated partition name: " + generatedName);
                }
            }
            Set<String> shouldExistPartitionNames = generatedNameToDesc.keySet();
            // Map every stored physical partition desc back to its physical name. A regenerated (alias)
            // name whose descriptor is already physically present under a legacy time-suffixed name must be
            // remapped to that physical name, otherwise alignMvPartition sees the descriptor as already
            // represented (and adds nothing) while calculateNeedRefreshPartitions drops the nonphysical
            // alias, and the manual refresh completes as NOT_REFRESH without refreshing anything.
            Map<PartitionKeyDesc, String> descToPhysicalName = Maps.newHashMap();
            for (String partitionName : existPartitionNames) {
                descToPhysicalName.putIfAbsent(
                        mtmv.getPartitionItemOrAnalysisException(partitionName).toPartitionKeyDesc(),
                        partitionName);
            }
            // Resolve into an insertion-ordered set so several requested names that map to the same
            // physical partition (e.g. a legacy time-suffixed name and its regenerated SHA alias) collapse
            // to a single target. Keeping the duplicates would make the refresh task run separate INSERT
            // OVERWRITE statements for the same partition (with refresh_partition_num=1) or over-count the
            // distinct targets when batching / generating the refresh mode.
            Set<String> resolvedPartitions = new LinkedHashSet<>();
            for (String partition : partitions) {
                if (shouldExistPartitionNames.contains(partition)) {
                    if (existPartitionNames.contains(partition)) {
                        // regenerated name equals the stored physical name (deterministic naming)
                        resolvedPartitions.add(partition);
                        continue;
                    }
                    // The partition is addressed by its regenerated (SHA) name. If a physical partition with
                    // the same descriptor already exists under a legacy name, remap to it; otherwise the
                    // alias is a not-yet-created partition that alignMvPartition will materialize.
                    // partition is in shouldExistPartitionNames (the map's key set), so the lookup is
                    // guaranteed to succeed.
                    String physicalName = descToPhysicalName.get(generatedNameToDesc.get(partition));
                    resolvedPartitions.add(physicalName != null ? physicalName : partition);
                    continue;
                }
                if (!existPartitionNames.contains(partition)) {
                    throw new org.apache.doris.common.AnalysisException("partition not exist: " + partition);
                }
                // A real stored physical name (possibly with a historical time suffix) is accepted only while
                // its descriptor is still derivable from the current base table partitions. A stale name whose
                // base partition was dropped / re-partitioned must be rejected so the async task does not
                // later dereference a partition removed by alignMvPartition.
                PartitionKeyDesc storedDesc = mtmv.getPartitionItemOrAnalysisException(partition)
                        .toPartitionKeyDesc();
                if (!relatedPartitionDescs.contains(storedDesc)) {
                    throw new org.apache.doris.common.AnalysisException("partition not exist: " + partition
                            + " (its base partition is no longer valid)");
                }
                resolvedPartitions.add(partition);
            }
            this.partitions = Lists.newArrayList(resolvedPartitions);
        } finally {
            MetaLockUtils.readUnlockTables(tables);
        }
    }

    /**
     * getMvName
     *
     * @return TableNameInfo
     */
    public TableNameInfo getMvName() {
        return mvName;
    }

    /**
     * getPartitions
     *
     * @return partitionNames
     */
    public List<String> getPartitions() {
        return partitions;
    }

    /**
     * isComplete
     *
     * @return isComplete
     */
    public boolean isComplete() {
        return isComplete;
    }

    @Override
    public String toString() {
        return "RefreshMTMVInfo{"
                + "mvName=" + mvName
                + ", partitions=" + partitions
                + ", isComplete=" + isComplete
                + '}';
    }
}
