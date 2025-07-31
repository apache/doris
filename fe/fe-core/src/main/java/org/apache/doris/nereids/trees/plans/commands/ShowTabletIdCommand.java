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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.query.QueryStatsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * show tablet id command
 */
public class ShowTabletIdCommand extends ShowCommand {
    private final long tabletId;
    private String dbName;

    /**
     * constructor
     */
    public ShowTabletIdCommand(long tabletId) {
        super(PlanType.SHOW_TABLET_ID_COMMAND);
        this.tabletId = tabletId;
    }

    /**
     * get meta for show tabletId
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("DbName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("TableName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("PartitionName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("IndexName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("DbId", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("TableId", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("PartitionId", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("IndexId", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("IsSync", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Order", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("QueryHits", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("DetailCmd", ScalarType.createVarchar(30)));
        return builder.build();
    }

    /**
     * validate
     */
    @VisibleForTesting
    protected void validate(ConnectContext ctx) throws AnalysisException {
        // check access first
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW TABLET");
        }

        dbName = ctx.getDatabase();
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
    }

    private ShowResultSet handleShowTabletId() {
        List<List<String>> rows = Lists.newArrayList();
        Env env = Env.getCurrentEnv();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        Long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
        String dbName = FeConstants.null_string;
        Long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;
        String tableName = FeConstants.null_string;
        Long partitionId = tabletMeta != null ? tabletMeta.getPartitionId() : TabletInvertedIndex.NOT_EXIST_VALUE;
        String partitionName = FeConstants.null_string;
        Long indexId = tabletMeta != null ? tabletMeta.getIndexId() : TabletInvertedIndex.NOT_EXIST_VALUE;
        String indexName = FeConstants.null_string;
        Boolean isSync = true;
        long queryHits = 0L;

        int tabletIdx = -1;
        // check real meta
        do {
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                isSync = false;
                break;
            }
            dbName = db.getFullName();
            Table table = db.getTableNullable(tableId);
            if (!(table instanceof OlapTable)) {
                isSync = false;
                break;
            }
            if (Config.enable_query_hit_stats) {
                MaterializedIndex mi = ((OlapTable) table).getPartition(partitionId).getIndex(indexId);
                if (mi != null) {
                    Tablet t = mi.getTablet(tabletId);
                    for (Replica r : t.getReplicas()) {
                        queryHits += QueryStatsUtil.getMergedReplicaStats(r.getId());
                    }
                }
            }

            table.readLock();
            try {
                tableName = table.getName();
                OlapTable olapTable = (OlapTable) table;
                Partition partition = olapTable.getPartition(partitionId);
                if (partition == null) {
                    isSync = false;
                    break;
                }
                partitionName = partition.getName();

                MaterializedIndex index = partition.getIndex(indexId);
                if (index == null) {
                    isSync = false;
                    break;
                }
                indexName = olapTable.getIndexNameById(indexId);

                Tablet tablet = index.getTablet(tabletId);
                if (tablet == null) {
                    isSync = false;
                    break;
                }

                tabletIdx = index.getTabletOrderIdx(tablet.getId());

                List<Replica> replicas = tablet.getReplicas();
                for (Replica replica : replicas) {
                    Replica tmp = invertedIndex.getReplica(tabletId, replica.getBackendIdWithoutException());
                    if (tmp == null) {
                        isSync = false;
                        break;
                    }
                    // use !=, not equals(), because this should be the same object.
                    if (tmp != replica) {
                        isSync = false;
                        break;
                    }
                }

            } finally {
                table.readUnlock();
            }
        } while (false);

        String detailCmd = String.format("SHOW PROC '/dbs/%d/%d/partitions/%d/%d/%d';",
                dbId, tableId, partitionId, indexId, tabletId);
        rows.add(Lists.newArrayList(dbName, tableName, partitionName, indexName,
                dbId.toString(), tableId.toString(),
                partitionId.toString(), indexId.toString(),
                isSync.toString(), String.valueOf(tabletIdx), String.valueOf(queryHits), detailCmd));
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowTabletId();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTabletIdCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
