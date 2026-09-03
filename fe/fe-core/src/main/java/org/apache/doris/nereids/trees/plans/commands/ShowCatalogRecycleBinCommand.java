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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * ShowCatalogRecycleBinCommand
 */
public class ShowCatalogRecycleBinCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Type")
            .add("Name")
            .add("DbId")
            .add("TableId")
            .add("PartitionId")
            .add("DropTime")
            .add("DataSize")
            .add("RemoteDataSize")
            .build();

    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String DB_ID = "dbid";
    private static final String TABLE_ID = "tableid";

    private Expression whereClause;
    private String nameValue;
    private boolean isAccurateMatch;
    private String typeValue;
    private String dbIdValue;
    private String tableIdValue;

    /**
     * constructor
     */
    public ShowCatalogRecycleBinCommand(Expression whereClause) {
        super(PlanType.SHOW_CATALOG_RECYCLE_BIN_COMMAND);
        this.whereClause = whereClause;
    }

    /**
     * get meta for show tabletId
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (!analyzeWhereClause()) {
            throw new AnalysisException("Where clause should like: Name = \"name\", "
                    + "or Name LIKE \"matcher\", or Type = \"Database|Table|Partition\", "
                    + "or DbId = \"db_id\", or TableId = \"table_id\", "
                    + "or compound predicate with operator AND. "
                    + "Duplicate filters on the same column are not allowed.");
        }
    }

    private boolean analyzeWhereClause() {
        if (whereClause == null) {
            return true;
        }
        List<Expression> andExprs = ExpressionUtils.extractConjunction(whereClause);
        for (Expression expr : andExprs) {
            if (!analyzeSinglePredicate(expr)) {
                return false;
            }
        }
        return true;
    }

    private boolean analyzeSinglePredicate(Expression expr) {
        if (expr instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) expr;
            if (!(equalTo.left() instanceof UnboundSlot) || !(equalTo.right() instanceof Literal)) {
                return false;
            }
            String colName = equalTo.child(0).toSql().toLowerCase(Locale.ROOT);
            String value = ((Literal) equalTo.right()).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                return false;
            }
            boolean isDuplicateFilter = false;
            switch (colName) {
                case NAME:
                    if (nameValue != null) {
                        isDuplicateFilter = true;
                    } else {
                        nameValue = value;
                        isAccurateMatch = true;
                    }
                    break;
                case TYPE:
                    if (typeValue != null) {
                        isDuplicateFilter = true;
                    } else {
                        typeValue = value;
                    }
                    break;
                case DB_ID:
                    if (dbIdValue != null) {
                        isDuplicateFilter = true;
                    } else {
                        dbIdValue = value;
                    }
                    break;
                case TABLE_ID:
                    if (tableIdValue != null) {
                        isDuplicateFilter = true;
                    } else {
                        tableIdValue = value;
                    }
                    break;
                default:
                    return false;
            }
            if (isDuplicateFilter) {
                return false;
            }
            return true;
        } else if (expr instanceof Like) {
            Like like = (Like) expr;
            if (!(like.left() instanceof UnboundSlot) || !(like.right() instanceof Literal)) {
                return false;
            }
            String colName = like.child(0).toSql().toLowerCase(Locale.ROOT);
            if (!colName.equals(NAME)) {
                return false;
            }
            String value = ((Literal) like.right()).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                return false;
            }
            if (nameValue != null) {
                if (!nameValue.equals(value)) {
                    return false;
                }
            } else {
                nameValue = value;
                isAccurateMatch = false;
            }
            return true;
        }
        return false;
    }

    private Predicate<List<String>> getRowPredicate() throws AnalysisException {
        Predicate<String> namePredicate = getNamePredicate();
        Predicate<String> typePredicate = getTypePredicate();
        Predicate<String> dbIdPredicate = getDbIdPredicate();
        Predicate<String> tableIdPredicate = getTableIdPredicate();
        return row -> namePredicate.test(row.get(1))
                && typePredicate.test(row.get(0))
                && dbIdPredicate.test(row.get(2))
                && tableIdPredicate.test(row.get(3));
    }

    private Predicate<String> getNamePredicate() throws AnalysisException {
        if (nameValue == null) {
            return name -> true;
        }
        if (isAccurateMatch) {
            return CaseSensibility.PARTITION.getCaseSensibility()
                    ? name -> name.equals(nameValue)
                    : name -> name.equalsIgnoreCase(nameValue);
        }
        PatternMatcher patternMatcher = PatternMatcherWrapper.createMysqlPattern(
                nameValue, CaseSensibility.PARTITION.getCaseSensibility());
        return patternMatcher::match;
    }

    private Predicate<String> getTypePredicate() {
        if (typeValue == null) {
            return type -> true;
        }
        return type -> type.equalsIgnoreCase(typeValue);
    }

    private Predicate<String> getDbIdPredicate() {
        if (dbIdValue == null) {
            return dbId -> true;
        }
        return dbId -> dbId.equals(dbIdValue);
    }

    private Predicate<String> getTableIdPredicate() {
        if (tableIdValue == null) {
            return tableId -> true;
        }
        return tableId -> tableId.equals(tableIdValue);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Predicate<List<String>> predicate = getRowPredicate();
        List<List<String>> infos = Env.getCurrentRecycleBin().getInfo().stream()
                .filter(predicate)
                .collect(Collectors.toList());
        return new ShowResultSet(getMetaData(), infos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCatalogRecycleBinCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
