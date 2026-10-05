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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * show tablet belong command
 */
public class ShowTabletsBelongCommand extends ShowCommand {
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbName")
            .add("TableName")
            .add("TableSize")
            .add("PartitionNum")
            .add("BucketNum")
            .add("ReplicaCount")
            .add("TabletIds")
            .build();
    private final List<Long> tabletIds;

    /**
     * constructor
     */
    public ShowTabletsBelongCommand(List<Long> tabletIds) {
        super(PlanType.SHOW_TABLETS_BELONG_COMMAND);
        this.tabletIds = tabletIds;
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        //validation logic
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }
        if (tabletIds == null || tabletIds.isEmpty()) {
            throw new AnalysisException("Please supply at least one tablet id");
        }

        // main logic.
        List<List<String>> rows = new ArrayList<>();
        Env env = Env.getCurrentEnv();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        Map<Long, HashSet<Long>> tableToTabletIdsMap = new HashMap<>();
        for (long tabletId : tabletIds) {
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
            }
            Database db = env.getInternalCatalog().getDbNullable(tabletMeta.getDbId());
            if (db == null) {
                continue;
            }
            long tableId = tabletMeta.getTableId();
            Table table = db.getTableNullable(tableId);
            if (table == null) {
                continue;
            }

            if (!tableToTabletIdsMap.containsKey(tableId)) {
                tableToTabletIdsMap.put(tableId, new HashSet<>());
            }
            tableToTabletIdsMap.get(tableId).add(tabletId);
        }

        for (long tableId : tableToTabletIdsMap.keySet()) {
            Table table = env.getInternalCatalog().getTableByTableId(tableId);
            List<String> line = new ArrayList<>();
            line.add(table.getDatabase().getFullName());
            line.add(table.getName());

            OlapTable olapTable = (OlapTable) table;
            Pair<Double, String> tableSizePair = DebugUtil.getByteUint((long) olapTable.getDataSize());
            String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first) + " "
                    + tableSizePair.second;
            line.add(readableSize);
            line.add(new Long(olapTable.getPartitionNum()).toString());
            int totalBucketNum = 0;
            Set<String> partitionNamesSet = table.getPartitionNames();
            for (String partitionName : partitionNamesSet) {
                totalBucketNum += table.getPartition(partitionName).getDistributionInfo().getBucketNum();
            }
            line.add(new Long(totalBucketNum).toString());
            line.add(new Long(olapTable.getReplicaCount()).toString());
            line.add(tableToTabletIdsMap.get(tableId).toString());

            rows.add(line);
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTabletsBelongCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
