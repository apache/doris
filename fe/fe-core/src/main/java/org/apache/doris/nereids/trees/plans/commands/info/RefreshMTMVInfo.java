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

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
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
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * refresh mtmv info
 */
public class RefreshMTMVInfo {

    /**
     * Explicit refresh mode for manual REFRESH MATERIALIZED VIEW command.
     */
    public enum RefreshMode {
        AUTO,
        COMPLETE,
        INCREMENTAL,
        PARTITIONS
    }

    private final TableNameInfo mvName;
    private List<String> partitions;
    private RefreshMode refreshMode;
    // Manual refresh fallback is always explicit in the task request. MV default
    // policy is handled by scheduled/on-commit tasks, not by this command.
    private boolean allowFallback;

    public RefreshMTMVInfo(TableNameInfo mvName, List<String> partitions, RefreshMode refreshMode) {
        this(mvName, partitions, refreshMode, defaultAllowFallback(refreshMode));
    }

    public RefreshMTMVInfo(TableNameInfo mvName, List<String> partitions, RefreshMode refreshMode,
            boolean allowFallback) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.partitions = Utils.copyRequiredList(partitions);
        this.refreshMode = Objects.requireNonNull(refreshMode, "require refreshMode object");
        this.allowFallback = allowFallback;
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
            validateRefreshModeCompat(mtmv);
            if (!CollectionUtils.isEmpty(partitions)) {
                checkPartitionExist(mtmv);
            }
        } catch (org.apache.doris.common.AnalysisException | MetaNotFoundException | DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    private void validateRefreshModeCompat(MTMV mtmv) {
        if (refreshMode == RefreshMode.COMPLETE && allowFallback) {
            // COMPLETE is already the terminal refresh method; FALLBACK would
            // not add another safe attempt.
            throw new AnalysisException("COMPLETE refresh does not support FALLBACK");
        }
        if (!CollectionUtils.isEmpty(partitions) && allowFallback) {
            // An explicit partitionSpec is an exact user scope and must not be
            // expanded to a full refresh by fallback.
            throw new AnalysisException("partitionSpec does not support FALLBACK");
        }
        RefreshMethod mvRefreshMethod = mtmv.getRefreshInfo() == null
                ? null : mtmv.getRefreshInfo().getRefreshMethod();
        if (mvRefreshMethod == null) {
            throw new AnalysisException("Materialized view has unknown refresh method.");
        }
        if (!isRefreshModeCompatible(mtmv, mvRefreshMethod)) {
            throw new AnalysisException("Cannot use " + refreshMode
                    + " refresh on a materialized view with " + mvRefreshMethod + " refresh policy.");
        }
        if (mtmv.isIvm() && !CollectionUtils.isEmpty(partitions)) {
            // IVM MVs should not bypass incremental semantics through a legacy
            // partitionSpec. Users can explicitly choose the PARTITIONS keyword
            // when they want the partition-refresh strategy.
            throw new AnalysisException(
                    "partitionSpec is not allowed on a materialized view with INCREMENTAL capability, "
                            + "use PARTITIONS keyword instead.");
        }
    }

    private boolean isRefreshModeCompatible(MTMV mtmv, RefreshMethod mvRefreshMethod) {
        if (refreshMode == RefreshMode.AUTO) {
            return false;
        }
        switch (mvRefreshMethod) {
            case COMPLETE:
                return refreshMode == RefreshMode.COMPLETE;
            case PARTITIONS:
                return refreshMode == RefreshMode.PARTITIONS || refreshMode == RefreshMode.COMPLETE;
            case INCREMENTAL:
                return refreshMode == RefreshMode.INCREMENTAL
                        || refreshMode == RefreshMode.PARTITIONS
                        || refreshMode == RefreshMode.COMPLETE;
            case AUTO:
                if (mtmv.isIvm()) {
                    return refreshMode == RefreshMode.INCREMENTAL
                            || refreshMode == RefreshMode.PARTITIONS
                            || refreshMode == RefreshMode.COMPLETE;
                }
                if (mtmv.getMvPartitionInfo() != null
                        && mtmv.getMvPartitionInfo().getPartitionType() != MTMVPartitionType.SELF_MANAGE) {
                    return refreshMode == RefreshMode.PARTITIONS || refreshMode == RefreshMode.COMPLETE;
                }
                return refreshMode == RefreshMode.COMPLETE;
            default:
                throw new IllegalStateException("Unsupported refresh method: " + mvRefreshMethod);
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
            List<AllPartitionDesc> partitionDescs = MTMVPartitionUtil.getPartitionDescsByRelatedTable(
                    mtmv.getTableProperty().getProperties(), mtmv.getMvPartitionInfo(), mtmv.getMvProperties(),
                    mtmv.getPartitionColumns());
            Set<String> shouldExistPartitionNames = Sets.newHashSetWithExpectedSize(partitionDescs.size());
            partitionDescs.stream().forEach(desc -> {
                shouldExistPartitionNames.add(((SinglePartitionDesc) desc).getPartitionName());
            });
            for (String partition : partitions) {
                if (!shouldExistPartitionNames.contains(partition)) {
                    throw new org.apache.doris.common.AnalysisException("partition not exist: " + partition);
                }
            }
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
     * getRefreshMode
     *
     * @return RefreshMode
     */
    public RefreshMode getRefreshMode() {
        return refreshMode;
    }

    public boolean allowFallback() {
        return allowFallback;
    }

    public static boolean defaultAllowFallback(RefreshMode refreshMode) {
        return refreshMode == RefreshMode.AUTO;
    }

    /**
     * isComplete - backward compatibility helper
     *
     * @return true if refresh mode is COMPLETE
     */
    public boolean isComplete() {
        return refreshMode == RefreshMode.COMPLETE;
    }

    @Override
    public String toString() {
        return "RefreshMTMVInfo{"
                + "mvName=" + mvName
                + ", partitions=" + partitions
                + ", refreshMode=" + refreshMode
                + ", allowFallback=" + allowFallback
                + '}';
    }
}
