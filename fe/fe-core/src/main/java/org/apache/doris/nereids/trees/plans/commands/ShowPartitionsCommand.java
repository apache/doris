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

import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * show partitions command
 */
public class ShowPartitionsCommand extends ShowCommand {
    public static final String FILTER_PARTITION_NAME = "PartitionName";
    private static final Logger LOG = LogManager.getLogger(ShowPartitionsCommand.class);
    private static final String FILTER_PARTITION_ID = "PartitionId";
    private static final String FILTER_STATE = "State";
    private static final String FILTER_BUCKETS = "Buckets";
    private static final String FILTER_REPLICATION_NUM = "ReplicationNum";
    private static final String FILTER_LAST_CONSISTENCY_CHECK_TIME = "LastConsistencyCheckTime";
    private final TableNameInfo tableName;
    private final Expression wildWhere;
    private final long limit;
    private final long offset;
    private final List<OrderKey> orderKeys;
    private boolean isTempPartition;
    private CatalogIf catalog;
    private ProcNodeInterface node;
    private Map<String, Expression> filterMap;
    private ArrayList<OrderByPair> orderByPairs;

    /**
     * constructor for show partitions
     */
    public ShowPartitionsCommand(TableNameInfo tableName, Expression wildWhere,
                                     List<OrderKey> orderKeys, long limit, long offset, boolean isTempPartition) {
        super(PlanType.SHOW_PARTITIONS_COMMAND);
        this.tableName = tableName;
        this.wildWhere = wildWhere;
        if (wildWhere != null) {
            this.filterMap = new HashMap<>();
        }
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
        this.isTempPartition = isTempPartition;
    }

    public CatalogIf getCatalog() {
        return catalog;
    }

    private void analyzeSubExpression(Expression subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }

        if (subExpr instanceof CompoundPredicate) {
            if (!(subExpr instanceof And)) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            for (Expression child : subExpr.children()) {
                analyzeSubExpression(child);
            }
            return;
        }

        boolean isNotExpr = false;
        if (subExpr instanceof Not) {
            isNotExpr = true;
            subExpr = subExpr.child(0);
            if (!(subExpr instanceof EqualTo)) {
                throw new AnalysisException("Only operator =|>=|<=|>|<|!=|like are supported.");
            }
        }

        if (!(subExpr.child(0) instanceof UnboundSlot)) {
            throw new AnalysisException("Only allow column in filter");
        }
        String leftKey = ((UnboundSlot) subExpr.child(0)).getName();

        // FILTER_LAST_CONSISTENCY_CHECK_TIME != 'abc'
        if (subExpr instanceof ComparisonPredicate) {
            if (leftKey.equalsIgnoreCase(FILTER_PARTITION_NAME) || leftKey.equalsIgnoreCase(FILTER_STATE)) {
                if (!(subExpr instanceof EqualTo)) {
                    throw new AnalysisException(String.format("Only operator =|like are supported for %s", leftKey));
                }
            } else if (leftKey.equalsIgnoreCase(FILTER_LAST_CONSISTENCY_CHECK_TIME)) {
                if (!(subExpr.child(1) instanceof StringLikeLiteral)) {
                    throw new AnalysisException("Where clause : LastConsistencyCheckTime =|>=|<=|>|<|!= "
                        + "\"2019-12-22|2019-12-22 22:22:00\"");
                }
                Expression left = subExpr.child(0);
                Expression right = subExpr.child(1).castTo(Config.enable_date_conversion
                        ? DateTimeV2Type.MAX : DateTimeType.INSTANCE);
                subExpr.withChildren(left, right);
            } else if (!leftKey.equalsIgnoreCase(FILTER_PARTITION_ID) && !leftKey.equalsIgnoreCase(FILTER_BUCKETS)
                    && !leftKey.equalsIgnoreCase(FILTER_REPLICATION_NUM)) {
                throw new AnalysisException("Only the columns of PartitionId/PartitionName/"
                    + "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
            }
        } else if (subExpr instanceof Like) {
            if (!leftKey.equalsIgnoreCase(FILTER_PARTITION_NAME) && !leftKey.equalsIgnoreCase(FILTER_STATE)) {
                throw new AnalysisException("Where clause : PartitionName|State like \"p20191012|NORMAL\"");
            }
        } else {
            throw new AnalysisException("Only operator =|>=|<=|>|<|!=|like are supported.");
        }

        filterMap.put(leftKey.toLowerCase(), isNotExpr ? new Not(subExpr) : subExpr);
    }

    protected void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            tableName.setDb(ctx.getDatabase());
        }
        tableName.analyze(ctx.getNameSpaceContext());

        catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (catalog == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
        }

        // disallow unsupported catalog
        if (!(catalog.isInternalCatalog() || catalog instanceof PluginDrivenExternalCatalog)) {
            throw new AnalysisException(String.format("Catalog of type '%s' is not allowed in ShowPartitionsCommand",
                    catalog.getType()));
        }

        // where
        if (wildWhere != null) {
            analyzeSubExpression(wildWhere);
        }

        // order by
        if (orderKeys != null && !orderKeys.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderKey orderKey : orderKeys) {
                if (!(orderKey.getExpr() instanceof UnboundSlot)) {
                    throw new AnalysisException("Should order by column");
                }
                UnboundSlot slot = (UnboundSlot) orderKey.getExpr();
                String colName = slot.getName();

                // analyze column
                int index = -1;
                for (String title : PartitionsProcDir.TITLE_NAMES) {
                    if (title.equalsIgnoreCase(colName)) {
                        index = PartitionsProcDir.TITLE_NAMES.indexOf(title);
                    }
                }
                if (index == -1) {
                    throw new AnalysisException("Title name[" + colName + "] does not exist");
                }
                OrderByPair orderByPair = new OrderByPair(index, !orderKey.isAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    protected void analyze() throws UserException {
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), catalog.getName(), dbName,
                tblName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW PARTITIONS",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(), dbName + ": " + tblName);
        }

        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        TableIf table = db.getTableOrMetaException(tblName, TableType.OLAP,
                TableType.HMS_EXTERNAL_TABLE, TableType.MAX_COMPUTE_EXTERNAL_TABLE, TableType.PAIMON_EXTERNAL_TABLE,
                TableType.PLUGIN_EXTERNAL_TABLE);

        if (!catalog.isInternalCatalog()) {
            if (!table.isPartitionedTable()) {
                throw new AnalysisException("Table " + tblName + " is not a partitioned table");
            }
        } else {
            table.readLock();
            try {
                // build proc path
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("/dbs/");
                stringBuilder.append(db.getId());
                stringBuilder.append("/").append(table.getId());
                if (isTempPartition) {
                    stringBuilder.append("/temp_partitions");
                } else {
                    stringBuilder.append("/partitions");
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("process SHOW PROC '{}';", stringBuilder.toString());
                }

                node = ProcService.getInstance().open(stringBuilder.toString());
            } finally {
                table.readUnlock();
            }
        }
    }

    private ShowResultSet handleShowPluginDrivenTablePartitions() throws AnalysisException {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        String dbName = tableName.getDb();
        ExternalTable dorisTable = pluginCatalog.getDbOrAnalysisException(dbName)
                .getTableOrAnalysisException(tableName.getTbl());

        // Route partition listing through the connector SPI.
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = pluginCatalog.getConnector().getMetadata(session);
        ConnectorTableHandle handle = metadata
                .getTableHandle(session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName())
                .orElseThrow(() -> new AnalysisException(
                        "table not found: " + dbName + "." + tableName.getTbl()));

        List<List<String>> rows = new ArrayList<>();
        if (hasPartitionStatsCapability()) {
            // Rich 5-column result (Partition / PartitionKey / RecordCount / FileSizeInBytes /
            // FileCount), matching the legacy paimon SHOW PARTITIONS (D-045). PartitionKey is the
            // table's partition-column names comma-joined, identical on every row (legacy semantics).
            String partitionColumnsStr = ((PluginDrivenExternalTable) dorisTable).getPartitionColumns()
                    .stream().map(Column::getName).collect(Collectors.joining(","));
            for (ConnectorPartitionInfo partition
                    : metadata.listPartitions(session, handle, Optional.empty())) {
                String partitionName = partition.getPartitionName();
                if (filterMap != null && !filterMap.isEmpty()
                        && !PartitionsProcDir.filterExpression(FILTER_PARTITION_NAME, partitionName, filterMap)) {
                    continue;
                }
                List<String> row = new ArrayList<>(5);
                row.add(partitionName);
                row.add(partitionColumnsStr);
                row.add(String.valueOf(partition.getRowCount()));
                row.add(String.valueOf(partition.getSizeBytes()));
                row.add(String.valueOf(partition.getFileCount()));
                rows.add(row);
            }
        } else {
            // Single-column result (partition name only). The SPI's listPartitionNames has no
            // offset/limit, so paging is applied FE-side below.
            for (String partition : metadata.listPartitionNames(session, handle)) {
                if (filterMap != null && !filterMap.isEmpty()
                        && !PartitionsProcDir.filterExpression(FILTER_PARTITION_NAME, partition, filterMap)) {
                    continue;
                }
                List<String> row = new ArrayList<>(1);
                row.add(partition);
                rows.add(row);
            }
        }
        // sort by partition name
        if (orderByPairs != null && orderByPairs.get(0).isDesc()) {
            rows.sort(Comparator.comparing(x -> x.get(0), Comparator.reverseOrder()));
        } else {
            rows.sort(Comparator.comparing(x -> x.get(0)));
        }
        rows = applyLimit(limit, offset, rows);
        return new ShowResultSet(getMetaData(), rows);
    }

    /**
     * Whether the current (plugin) catalog's connector exposes per-partition statistics
     * ({@link ConnectorCapability#SUPPORTS_PARTITION_STATS}). Drives the 5-column SHOW PARTITIONS
     * result for paimon while non-declaring connectors (e.g. MaxCompute) stay single-column. Both
     * {@link #handleShowPluginDrivenTablePartitions()} and {@link #getMetaData()} consult this so the
     * column headers and the row width never disagree.
     */
    private boolean hasPartitionStatsCapability() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_PARTITION_STATS);
    }

    protected ShowResultSet handleShowPartitions(ConnectContext ctx, StmtExecutor executor) throws UserException {
        // validate the where clause
        validate(ctx);

        // analyze catalog
        analyze();

        // get partition info
        if (catalog.isInternalCatalog()) {
            Preconditions.checkNotNull(node);
            LimitElement limitElement = null;
            if (limit > 0) {
                limitElement = new LimitElement(offset == -1L ? 0 : offset, limit);
            }
            List<List<String>> rows = ((PartitionsProcDir) node).fetchResultByExpressionFilter(filterMap,
                    orderByPairs, limitElement).getRows();
            return new ShowResultSet(getMetaData(), rows);
        } else {
            // After the disallow gate, a non-internal catalog is a PluginDrivenExternalCatalog.
            return handleShowPluginDrivenTablePartitions();
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (catalog.isInternalCatalog()) {
            ProcResult result = null;
            try {
                result = node.fetchResult();
            } catch (AnalysisException e) {
                return builder.build();
            }

            for (String col : result.getColumnNames()) {
                builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
            }
        } else if (hasPartitionStatsCapability()) {
            // A plugin connector that declares SUPPORTS_PARTITION_STATS (paimon after cutover):
            // 5-column rich result. Must match the row width built in
            // handleShowPluginDrivenTablePartitions().
            builder.addColumn(new Column("Partition", ScalarType.createVarchar(300)))
                    .addColumn(new Column("PartitionKey", ScalarType.createVarchar(300)))
                    .addColumn(new Column("RecordCount", ScalarType.createVarchar(300)))
                    .addColumn(new Column("FileSizeInBytes", ScalarType.createVarchar(300)))
                    .addColumn(new Column("FileCount", ScalarType.createVarchar(300)));

        } else {
            builder.addColumn(new Column("Partition", ScalarType.createVarchar(60)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowPartitions(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowPartitionsCommand(this, context);
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
