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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.TabletsProcDir;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;

public class ShowTabletStmt extends ShowStmt implements NotFallbackInParser {
    private TableName dbTableName;
    private String dbName;
    private String tableName;
    private long tabletId;
    private PartitionNames partitionNames;
    private Expr whereClause;
    private List<OrderByElement> orderByElements;
    private LimitElement limitElement;

    private long version;
    private long backendId;
    private String indexName;
    private Replica.ReplicaState replicaState;
    private ArrayList<OrderByPair> orderByPairs;

    private boolean isShowSingleTablet;

    public ShowTabletStmt(TableName dbTableName, long tabletId) {
        this(dbTableName, tabletId, null, null, null, null);
    }

    public ShowTabletStmt(TableName dbTableName, long tabletId, PartitionNames partitionNames,
            Expr whereClause, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.dbTableName = dbTableName;
        if (dbTableName == null) {
            this.dbName = null;
            this.tableName = null;
            this.isShowSingleTablet = true;
            this.indexName = null;
        } else {
            this.dbName = dbTableName.getDb();
            this.tableName = dbTableName.getTbl();
            this.isShowSingleTablet = false;
            this.indexName = Strings.emptyToNull(indexName);
        }
        this.tabletId = tabletId;
        this.partitionNames = partitionNames;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;

        this.version = -1;
        this.backendId = -1;
        this.indexName = null;
        this.replicaState = null;
        this.orderByPairs = null;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTabletId() {
        return tabletId;
    }

    public boolean isShowSingleTablet() {
        return isShowSingleTablet;
    }

    public boolean hasOffset() {
        return limitElement != null && limitElement.hasOffset();
    }

    public long getOffset() {
        return limitElement.getOffset();
    }

    public boolean hasPartition() {
        return partitionNames != null;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public boolean hasLimit() {
        return limitElement != null && limitElement.hasLimit();
    }

    public long getLimit() {
        return limitElement.getLimit();
    }

    public long getVersion() {
        return version;
    }

    public long getBackendId() {
        return backendId;
    }

    public String getIndexName() {
        return indexName;
    }

    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    public Replica.ReplicaState getReplicaState() {
        return replicaState;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // check access first
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW TABLET");
        }

        super.analyze(analyzer);
        if (dbTableName != null) {
            dbTableName.analyze(analyzer);
            // disallow external catalog
            Util.prohibitExternalCatalog(dbTableName.getCtl(), this.getClass().getSimpleName());
        }
        if (!isShowSingleTablet && Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
        }

        if (limitElement != null) {
            limitElement.analyze(analyzer);
        }

        // analyze where clause if not null
        if (whereClause != null) {
            if (whereClause instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) whereClause;
                if (cp.getOp() != org.apache.doris.analysis.CompoundPredicate.Operator.AND) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }

                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
            } else {
                analyzeSubPredicate(whereClause);
            }
        }

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<OrderByPair>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = TabletsProcDir.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }
        if (subExpr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) subExpr;
            if (cp.getOp() != org.apache.doris.analysis.CompoundPredicate.Operator.AND) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }

            analyzeSubPredicate(cp.getChild(0));
            analyzeSubPredicate(cp.getChild(1));
            return;
        }
        boolean valid = true;
        do {
            if (!(subExpr instanceof  BinaryPredicate)) {
                valid = false;
                break;
            }
            BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
            if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                valid = false;
                break;
            }

            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break;
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
            if (leftKey.equalsIgnoreCase("version")) {
                if (!(subExpr.getChild(1) instanceof IntLiteral) || version > -1) {
                    valid = false;
                    break;
                }
                version = ((IntLiteral) subExpr.getChild(1)).getValue();
            } else if (leftKey.equalsIgnoreCase("backendid")) {
                if (!(subExpr.getChild(1) instanceof IntLiteral) || backendId > -1) {
                    valid = false;
                    break;
                }
                backendId = ((IntLiteral) subExpr.getChild(1)).getValue();
            } else if (leftKey.equalsIgnoreCase("indexname")) {
                if (!(subExpr.getChild(1) instanceof StringLiteral) || indexName != null) {
                    valid = false;
                    break;
                }
                indexName = ((StringLiteral) subExpr.getChild(1)).getValue();
            } else if (leftKey.equalsIgnoreCase("state")) {
                if (!(subExpr.getChild(1) instanceof StringLiteral) || replicaState != null) {
                    valid = false;
                    break;
                }
                String state = ((StringLiteral) subExpr.getChild(1)).getValue().toUpperCase();
                try {
                    replicaState = Replica.ReplicaState.valueOf(state);
                } catch (Exception e) {
                    replicaState = null;
                    valid = false;
                    break;
                }
            } else {
                valid = false;
                break;
            }
        } while (false);

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: Version = \"version\","
                    + " or state = \"NORMAL|ROLLUP|CLONE|DECOMMISSION\", or BackendId = 10000,"
                    + " indexname=\"rollup_name\" or compound predicate with operator AND");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW TABLET ");
        if (isShowSingleTablet) {
            sb.append(tabletId);
        } else {
            sb.append(" FROM ").append("`").append(dbName).append("`.`").append(tableName).append("`");
        }
        if (limitElement != null) {
            if (limitElement.hasOffset() && limitElement.hasLimit()) {
                sb.append(" ").append(limitElement.getOffset()).append(",").append(limitElement.getLimit());
            } else if (limitElement.hasLimit()) {
                sb.append(" ").append(limitElement.getLimit());
            }
        }
        return sb.toString();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (isShowSingleTablet) {
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
        } else {
            for (String title : TabletsProcDir.TITLE_NAMES) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
