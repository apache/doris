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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowPartitionsStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowPartitionsStmt.class);

    private static final String FILTER_PARTITION_ID = "PartitionId";
    private static final String FILTER_PARTITION_NAME = "PartitionName";
    private static final String FILTER_STATE = "State";
    private static final String FILTER_BUCKETS = "Buckets";
    private static final String FILTER_REPLICATION_NUM = "ReplicationNum";
    private static final String FILTER_LAST_CONSISTENCY_CHECK_TIME = "LastConsistencyCheckTime";

    private String dbName;
    private String tableName;
    private Expr whereClause;
    private List<OrderByElement> orderByElements;
    private LimitElement limitElement;
    private boolean isTempPartition = false;

    private List<OrderByPair> orderByPairs;
    private Map<String, Expr> filterMap;

    private ProcNodeInterface node;

    public ShowPartitionsStmt(TableName tableName, Expr whereClause, List<OrderByElement> orderByElements,
            LimitElement limitElement, boolean isTempPartition) {
        this.dbName = tableName.getDb();
        this.tableName = tableName.getTbl();
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
        if (whereClause != null) {
            this.filterMap = new HashMap<>();
        }
        this.isTempPartition = isTempPartition;
    }

    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    public Map<String, Expr> getFilterMap() {
      return filterMap;
    }

    public ProcNodeInterface getNode() {
        return node;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        analyzeImpl(analyzer);
        // check access
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName, tableName,
                                                                PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW PARTITIONS",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tableName);
        }
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Table table = db.getTableOrThrowException(tableName, Table.TableType.OLAP);
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

    public void analyzeImpl(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        // analyze where clause if not null
        if (whereClause != null) {
            analyzeSubPredicate(whereClause);
        }

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = PartitionsProcDir.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }

        if (limitElement != null) {
            limitElement.analyze(analyzer);
        }
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }
        if (subExpr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) subExpr;
            if (cp.getOp() != CompoundPredicate.Operator.AND) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            analyzeSubPredicate(cp.getChild(0));
            analyzeSubPredicate(cp.getChild(1));
            return;
        }

        if (!(subExpr.getChild(0) instanceof SlotRef)) {
            throw new AnalysisException("Show filter by column");
        }

        String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
        if (subExpr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
            if (leftKey.equalsIgnoreCase(FILTER_PARTITION_NAME) || leftKey.equalsIgnoreCase(FILTER_STATE)) {
                if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                  throw new AnalysisException(String.format("Only operator =|like are supported for %s", leftKey));
                }
            } else if (leftKey.equalsIgnoreCase(FILTER_LAST_CONSISTENCY_CHECK_TIME)) {
                if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                    throw new AnalysisException("Where clause : LastConsistencyCheckTime =|>=|<=|>|<|!= "
                        + "\"2019-12-22|2019-12-22 22:22:00\"");
                }
                subExpr.setChild(1,(subExpr.getChild(1)).castTo(Type.DATETIME));
            } else if (!leftKey.equalsIgnoreCase(FILTER_PARTITION_ID) && !leftKey.equalsIgnoreCase(FILTER_BUCKETS) &&
                !leftKey.equalsIgnoreCase(FILTER_REPLICATION_NUM)) {
                throw new AnalysisException("Only the columns of PartitionId/PartitionName/" +
                    "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
            }
        } else if (subExpr instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) subExpr;
            if (leftKey.equalsIgnoreCase(FILTER_PARTITION_NAME) || leftKey.equalsIgnoreCase(FILTER_STATE)) {
                if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                    throw new AnalysisException("Where clause : PartitionName|State like "
                        + "\"p20191012|NORMAL\"");
                }
            } else {
                throw new AnalysisException("Where clause : PartitionName|State like \"p20191012|NORMAL\"");
            }
        } else {
            throw new AnalysisException("Only operator =|>=|<=|>|<|!=|like are supported.");
        }
        filterMap.put(leftKey.toLowerCase(), subExpr);
    }


    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ProcResult result = null;
        try {
            result = node.fetchResult();
        } catch (AnalysisException e) {
            return builder.build();
        }

        for (String col : result.getColumnNames()) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW ");
        if (isTempPartition) {
            sb.append("TEMPORARY ");
        }
        sb.append("PARTITIONS FROM ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("`").append(dbName).append("`");
        }
        if (!Strings.isNullOrEmpty(tableName)) {
            sb.append(".`").append(tableName).append("`");
        }
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }
        // Order By clause
        if (orderByElements != null) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                sb.append(orderByElements.get(i).toSql());
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }

        if (limitElement != null) {
            sb.append(limitElement.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
