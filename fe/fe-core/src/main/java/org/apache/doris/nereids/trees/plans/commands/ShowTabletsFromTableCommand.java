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
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.TabletsProcDir;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.SortAndLimit;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * ShowTabletsFromTableCommand
 */
public class ShowTabletsFromTableCommand extends ShowCommand {
    private TableNameInfo dbTableName;
    private PartitionNamesInfo partitionNames;
    private Expression whereClause;
    private List<OrderKey> orderKeys;
    private long limit = 0;
    private long offset = 0;

    private long version;
    private long backendId;
    private Replica.ReplicaState replicaState;
    private ArrayList<OrderByPair> orderByPairs;

    /**
     * ShowTabletsFromTableCommand
     */
    public ShowTabletsFromTableCommand(TableNameInfo dbTableNameInfo, PartitionNamesInfo partitionNames,
                                       Expression whereClause, List<OrderKey> orderKeys, long limit, long offset) {
        super(PlanType.SHOW_TABLETS);
        this.dbTableName = dbTableNameInfo;
        this.partitionNames = partitionNames;
        this.whereClause = whereClause;
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;

        this.version = -1;
        this.backendId = -1;
        this.replicaState = null;
        this.orderByPairs = null;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        // check access first
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW TABLETS");
        }

        dbTableName.analyze(ctx.getNameSpaceContext());
        Util.prohibitExternalCatalog(dbTableName.getCtl(), this.getClass().getSimpleName());

        if (partitionNames != null) {
            partitionNames.validate();
        }

        if (whereClause != null) {
            List<Expression> andExprs = ExpressionUtils.extractConjunction(whereClause);
            boolean valid = true;
            for (Expression expr : andExprs) {
                if (!(expr instanceof EqualTo)) {
                    valid = false;
                    break;
                }
                EqualTo equalTo = (EqualTo) expr;
                if (equalTo.left().isConstant() && !equalTo.right().isConstant()) {
                    equalTo = equalTo.commute();
                }
                Expression right = ExpressionUtils.analyzeAndFoldToLiteral(ctx, equalTo.right());
                if (equalTo.left() instanceof UnboundSlot) {
                    String name = ((UnboundSlot) equalTo.left()).toSlot().getName().toLowerCase(Locale.ROOT);
                    switch (name) {
                        case "version":
                            if (right instanceof IntegerLikeLiteral) {
                                version = ((IntegerLikeLiteral) right).getLongValue();
                                continue;
                            }
                            valid = false;
                            break;
                        case "backendid":
                            if (right instanceof IntegerLikeLiteral) {
                                backendId = ((IntegerLikeLiteral) right).getLongValue();
                                continue;
                            }
                            valid = false;
                            break;
                        case "state":
                            if (right instanceof StringLikeLiteral) {
                                try {
                                    replicaState = Replica.ReplicaState.valueOf(((StringLikeLiteral) right).getValue());
                                    continue;
                                } catch (Exception e) {
                                    replicaState = null;
                                }
                            }
                            valid = false;
                            break;
                        default:
                            valid = false;
                    }
                }
            }

            if (!valid) {
                throw new AnalysisException("Where clause should looks like: Version = \"version\","
                    + " or state = \"NORMAL|ROLLUP|CLONE|DECOMMISSION\", or BackendId = 10000"
                    + " or compound predicate with operator AND");
            }
        }

        // order by
        if (orderKeys != null && !orderKeys.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderKey orderKey : orderKeys) {
                if (!(orderKey.getExpr() instanceof Slot)) {
                    throw new AnalysisException("Should order by column");
                }
                Slot slot = (Slot) orderKey.getExpr();
                int index = analyzeColumn(slot.getName());
                OrderByPair orderByPair = new OrderByPair(index, !orderKey.isAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private int analyzeColumn(String columnName) throws AnalysisException {
        List<Column> titles = getMetaData().getColumns();
        for (Column title : titles) {
            if (title.getName().equalsIgnoreCase(columnName)) {
                return titles.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        List<List<String>> rows = Lists.newArrayList();
        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrAnalysisException(dbTableName.getDb());
        OlapTable olapTable = db.getOlapTableOrAnalysisException(dbTableName.getTbl());
        olapTable.readLock();
        try {
            // sizeLimit bounds how many rows need to be collected/sorted: LIMIT rows plus the
            // OFFSET rows that get skipped afterwards.
            Optional<Integer> sizeLimit = Optional.empty();
            if (limit >= 0) {
                long capped = Math.min(limit, Integer.MAX_VALUE);
                if (offset > 0) {
                    capped += Math.min(offset, Integer.MAX_VALUE);
                }
                sizeLimit = Optional.of((int) Math.min(capped, Integer.MAX_VALUE));
            }

            boolean stop = false;
            Collection<Partition> partitions = new ArrayList<Partition>();
            if (partitionNames != null) {
                List<String> paNames = partitionNames.getPartitionNames();
                if (!paNames.isEmpty()) {
                    for (String partName : partitionNames.getPartitionNames()) {
                        Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                        if (partition == null) {
                            throw new AnalysisException("Unknown partition: " + partName);
                        }
                        partitions.add(partition);
                    }
                }
            } else {
                partitions = olapTable.getPartitions();
            }
            List<List<Comparable>> tabletInfos = new ArrayList<>();
            for (Partition partition : partitions) {
                if (stop) {
                    break;
                }
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    TabletsProcDir procDir = new TabletsProcDir(olapTable, index);
                    tabletInfos.addAll(procDir.fetchComparableResult(
                            version, backendId, replicaState));

                    // Stop once the size is reached when no ORDER BY is given by the user explicitly.
                    if (sizeLimit.isPresent() && tabletInfos.size() >= sizeLimit.get() && orderByPairs == null) {
                        stop = true;
                        break;
                    }
                }
            }
            //If offset is greater than tabletinfo's size, we shall skip further processing
            // and return the empty list 'rows'.
            if (offset < tabletInfos.size()) {
                List<List<Comparable>> orderedTableInfos;

                if (orderByPairs != null) {
                    // order by
                    OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
                    ListComparator<List<Comparable>> comparator =
                            new ListComparator<>(orderByPairs.toArray(orderByPairArr));
                    orderedTableInfos = SortAndLimit.sortAndLimit(tabletInfos, comparator, sizeLimit);
                } else {
                    orderedTableInfos = tabletInfos;
                }

                int resultOffset = (int) Math.min(offset, orderedTableInfos.size());
                for (List<Comparable> tabletInfo
                        : orderedTableInfos.subList(resultOffset, orderedTableInfos.size())) {
                    List<String> oneTablet = new ArrayList<String>(tabletInfo.size());
                    for (Comparable column : tabletInfo) {
                        oneTablet.add(column.toString());
                    }
                    rows.add(oneTablet);
                }
            }
        } finally {
            olapTable.readUnlock();
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    /**
     * getMetaData
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TabletsProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTabletsFromTableCommand(this, context);
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
