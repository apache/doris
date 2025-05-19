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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.StatsType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Manually inject statistics for table.
 * <p>
 * Syntax:
 * ALTER TABLE table_name SET STATS ('k1' = 'v1', ...);
 * <p>
 * e.g.
 * ALTER TABLE stats_test.example_tbl SET STATS ('row_count'='6001215');
 */
public class AlterTableStatsCommand extends AlterCommand {
    private static final ImmutableSet<StatsType> CONFIGURABLE_PROPERTIES_SET =
            new ImmutableSet.Builder<StatsType>()
                .add(StatsType.ROW_COUNT)
                .build();
    private final TableNameInfo tableNameInfo;
    private final Map<String, String> properties;
    private final PartitionNamesInfo opPartitionNamesInfo;
    private final List<Long> partitionIds = Lists.newArrayList();
    private long tableId;
    private final Map<StatsType, String> statsTypeToValue = Maps.newHashMap();

    public AlterTableStatsCommand(TableNameInfo tableNameInfo,
                                  PartitionNamesInfo opPartitionNamesInfo,
                                  Map<String, String> properties) {
        super(PlanType.ALTER_TABLE_STATS_COMMAND);
        this.tableNameInfo = tableNameInfo;
        this.opPartitionNamesInfo = opPartitionNamesInfo;
        this.properties = properties;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public long getTableId() {
        return tableId;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        if (!ConnectContext.get().getSessionVariable().enableStats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                + " in your FE conf file");
        }

        tableNameInfo.analyze(ctx);

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER TABLE STATS",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getDb() + ": " + tableNameInfo.getTbl());
        }

        Optional<StatsType> optional = properties.keySet().stream().map(StatsType::fromString)
                .filter(statsType -> !CONFIGURABLE_PROPERTIES_SET.contains(statsType))
                .findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid statistics");
        }

        properties.forEach((key, value) -> {
            StatsType statsType = StatsType.fromString(key);
            statsTypeToValue.put(statsType, value);
        });

        CatalogIf catalog = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(tableNameInfo.getCtl());
        DatabaseIf db = catalog.getDbOrAnalysisException(tableNameInfo.getDb());
        TableIf table = db.getTableOrAnalysisException(tableNameInfo.getTbl());
        tableId = table.getId();

        if (opPartitionNamesInfo != null && table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getPartitionInfo().getType().equals(PartitionType.UNPARTITIONED)) {
                throw new AnalysisException("Not a partitioned table: " + olapTable.getName());
            }

            opPartitionNamesInfo.validate();
            List<String> partitionNames = opPartitionNamesInfo.getPartitionNames();
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new AnalysisException("Partition does not exist: " + partitionName);
                }
                partitionIds.add(partition.getId());
            }
        }
    }

    public String getValue(StatsType statsType) {
        return statsTypeToValue.get(statsType);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableStatsCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
