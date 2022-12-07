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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * ShowAnalyzeStmt is used to show statistics job info.
 * syntax:
 *    SHOW ANALYZE
 *        [TABLE | ID]
 *        [
 *            WHERE
 *            [STATE = ["PENDING"|"SCHEDULING"|"RUNNING"|"FINISHED"|"FAILED"|"CANCELLED"]]
 *        ]
 *        [ORDER BY ...]
 *        [LIMIT limit][OFFSET offset];
 */
public class ShowAnalyzeStmt extends ShowStmt {
    private static final String STATE_NAME = "state";
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("id")
            .add("create_time")
            .add("start_time")
            .add("finish_time")
            .add("error_msg")
            .add("scope")
            .add("progress")
            .add("state")
            .build();

    private List<Long> jobIds;
    private TableName dbTableName;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    // after analyzed
    private long dbId;
    private final Set<Long> tblIds = Sets.newHashSet();

    private String stateValue;
    private ArrayList<OrderByPair> orderByPairs;

    public ShowAnalyzeStmt() {
    }

    public ShowAnalyzeStmt(List<Long> jobIds) {
        this.jobIds = jobIds;
    }

    public ShowAnalyzeStmt(TableName dbTableName,
                           Expr whereClause,
                           List<OrderByElement> orderByElements,
                           LimitElement limitElement) {
        this.dbTableName = dbTableName;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public List<Long> getJobIds() {
        return jobIds;
    }

    public long getDbId() {
        Preconditions.checkArgument(isAnalyzed(),
                "The dbId must be obtained after the parsing is complete");
        return dbId;
    }

    public Set<Long> getTblIds() {
        Preconditions.checkArgument(isAnalyzed(),
                "The dbId must be obtained after the parsing is complete");
        return tblIds;
    }

    public String getStateValue() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tbl name must be obtained after the parsing is complete");
        return stateValue;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tbl name must be obtained after the parsing is complete");
        return orderByPairs;
    }

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public long getOffset() {
        if (limitElement != null && limitElement.hasOffset()) {
            return limitElement.getOffset();
        }
        return -1L;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (dbTableName != null) {
            dbTableName.analyze(analyzer);
            String dbName = dbTableName.getDb();
            String tblName = dbTableName.getTbl();
            checkShowAnalyzePriv(dbName, tblName);

            Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);
            Table table = db.getTableOrAnalysisException(tblName);
            dbId = db.getId();
            tblIds.add(table.getId());
        } else {
            // analyze the current default db
            String dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);

            db.readLock();
            try {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    checkShowAnalyzePriv(dbName, table.getName());
                }

                dbId = db.getId();
                for (Table table : tables) {
                    long tblId = table.getId();
                    tblIds.add(tblId);
                }
            } finally {
                db.readUnlock();
            }
        }

        // analyze where clause if not null
        if (whereClause != null) {
            analyzeSubPredicate(whereClause);
        }

        // analyze order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderByElement orderByElement : orderByElements) {
                if (orderByElement.getExpr() instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = analyzeColumn(slotRef.getColumnName());
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                } else {
                    throw new AnalysisException("Should order by column");
                }
            }
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private void checkShowAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        PaloAuth auth = Env.getCurrentEnv().getAuth();
        if (!auth.checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "SHOW ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }

        boolean valid = true;

        CHECK: {
            if (subExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }

            // left child
            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
            if (!STATE_NAME.equalsIgnoreCase(leftKey)) {
                valid = false;
                break CHECK;
            }

            // right child
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break CHECK;
            }

            String value = subExpr.getChild(1).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                valid = false;
                break CHECK;
            }

            stateValue = value.toUpperCase();
            try {
                // support it later
            } catch (Exception e) {
                valid = false;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: "
                    + "STATE = \"PENDING|SCHEDULING|RUNNING|FINISHED|FAILED|CANCELLED\"");
        }
    }

    private int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ANALYZE");
        if (dbTableName != null) {
            sb.append(" ");
            sb.append(dbTableName.toSql());
        }

        if (whereClause != null) {
            sb.append(" ");
            sb.append("WHERE");
            sb.append(" ");
            sb.append(whereClause.toSql());
        }

        // Order By clause
        if (orderByElements != null) {
            sb.append(" ");
            sb.append("ORDER BY");
            sb.append(" ");
            IntStream.range(0, orderByElements.size()).forEach(i -> {
                sb.append(orderByElements.get(i).getExpr().toSql());
                sb.append((orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            });
        }

        if (getLimit() != -1L) {
            sb.append(" ");
            sb.append("LIMIT");
            sb.append(" ");
            sb.append(getLimit());
        }

        if (getOffset() != -1L) {
            sb.append(" ");
            sb.append("OFFSET");
            sb.append(" ");
            sb.append(getOffset());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
