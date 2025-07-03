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
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.Comparator;
import java.util.List;
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
        mvName.analyze(ctx);
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
        MTMVRelatedTableIf relatedTable = mtmv.getMvPartitionInfo().getRelatedTable();
        List<TableIf> tables = Lists.newArrayList(mtmv, relatedTable);
        tables.sort(Comparator.comparing(TableIf::getId));
        MetaLockUtils.readLockTables(tables);
        try {
            if (mtmv.getMvPartitionInfo().getPartitionType().equals(MTMVPartitionType.SELF_MANAGE)) {
                throw new AnalysisException(
                        "The partition method of this asynchronous materialized view "
                                + "does not support refreshing by partition");
            }
            List<AllPartitionDesc> partitionDescs = MTMVPartitionUtil.getPartitionDescsByRelatedTable(
                    mtmv.getTableProperty().getProperties(), mtmv.getMvPartitionInfo(), mtmv.getMvProperties());
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
