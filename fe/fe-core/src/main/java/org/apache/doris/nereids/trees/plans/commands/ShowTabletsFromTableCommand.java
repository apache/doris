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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.TabletsProcDir;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

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
    public void validate(ConnectContext ctx) throws AnalysisException {
        // check access first
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW TABLET");
        }

        dbTableName.analyze(ctx);
        Util.prohibitExternalCatalog(dbTableName.getCtl(), this.getClass().getSimpleName());

        if (partitionNames != null) {
            partitionNames.validate();
        }

        if (whereClause != null) {
            boolean valid = whereClause.accept(new DefaultExpressionVisitor<Boolean, Void>() {
                @Override
                public Boolean visitComparisonPredicate(ComparisonPredicate cp, Void context) {
                    if (cp.left() instanceof UnboundSlot && cp.right() instanceof Literal) {
                        String name = ((UnboundSlot) cp.left()).toSlot().getName().toLowerCase(Locale.ROOT);
                        if (name.equals("version")) {
                            if (cp.right() instanceof IntegerLikeLiteral) {
                                version = ((IntegerLikeLiteral) cp.right()).getLongValue();
                                return true;
                            }
                        } else if (name.equals("backendid")) {
                            if (cp.right() instanceof IntegerLikeLiteral) {
                                backendId = ((IntegerLikeLiteral) cp.right()).getLongValue();
                                return true;
                            }
                        } else if (name.equals("state")) {
                            if (cp.right() instanceof StringLikeLiteral) {
                                String state = ((StringLikeLiteral) cp.right()).getValue();
                                try {
                                    replicaState = Replica.ReplicaState.valueOf(state);
                                } catch (Exception e) {
                                    return false;
                                }
                                return true;
                            }
                        }
                    }
                    return false;
                }

                @Override
                public Boolean visit(Expression expr, Void context) {
                    for (Expression child : expr.children()) {
                        if (!child.accept(this, context)) {
                            return false;
                        }
                    }
                    return true;
                }
            }, null);

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
            long sizeLimit = -1;
            if (offset > 0 && limit > 0) {
                sizeLimit = offset + limit;
            } else if (limit > 0) {
                sizeLimit = limit;
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
                    if (sizeLimit > -1 && tabletInfos.size() >= sizeLimit) {
                        stop = true;
                        break;
                    }
                }
            }
            if (offset >= tabletInfos.size()) {
                tabletInfos.clear();
            } else {
                // order by
                ListComparator<List<Comparable>> comparator = null;
                if (orderByPairs != null) {
                    OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
                    comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
                } else {
                    // order by tabletId, replicaId
                    comparator = new ListComparator<>(0, 1);
                }
                Collections.sort(tabletInfos, comparator);
                if (sizeLimit > -1) {
                    tabletInfos = tabletInfos.subList((int) offset,
                            Math.min((int) sizeLimit, tabletInfos.size()));
                }

                for (List<Comparable> tabletInfo : tabletInfos) {
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
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTabletsFromTableCommand(this, context);
    }
}
