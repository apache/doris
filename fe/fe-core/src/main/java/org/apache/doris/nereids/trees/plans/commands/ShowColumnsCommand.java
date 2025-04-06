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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * ShowColumnsCommand
 */
public class ShowColumnsCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Field", ScalarType.createVarchar(128)))
            .addColumn(new Column("Type", ScalarType.createVarchar(128)))
            .addColumn(new Column("Null", ScalarType.createVarchar(128)))
            .addColumn(new Column("Key", ScalarType.createVarchar(128)))
            .addColumn(new Column("Default", ScalarType.createVarchar(128)))
            .addColumn(new Column("Extra", ScalarType.createVarchar(128))).build();

    private static final ShowResultSetMetaData META_DATA_VERBOSE =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Privileges", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(128)))
                    .build();

    private static Map<String, String> ALIAS_COLUMN_MAP = ImmutableMap.<String, String>builder()
            .put("field", "COLUMN_NAME")
            .put("type", "COLUMN_TYPE")
            .put("collation", "COLLATION_NAME")
            .put("null", "IS_NULLABLE")
            .put("key", "COLUMN_KEY")
            .put("default", "COLUMN_DEFAULT")
            .put("extra", "EXTRA")
            .put("privileges", "PRIVILEGES")
            .put("comment", "COLUMN_COMMENT")
            .build();

    private TableNameInfo tableNameInfo;
    private String likePattern;
    private boolean isVerbose;
    private Expression whereClause;
    private List<Pair<String, String>> equalConditions = new ArrayList<>();

    public ShowColumnsCommand(TableNameInfo tableNameInfo, boolean isVerbose, String likePattern,
                              Expression whereClause) {
        super(PlanType.SHOW_COLUMNS_COMMAND);
        this.tableNameInfo = tableNameInfo;
        this.likePattern = likePattern;
        this.isVerbose = isVerbose;
        this.whereClause = whereClause;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        tableNameInfo.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                        tableNameInfo.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), tableNameInfo);
        }
    }

    private boolean matchesCondition(Column col, Pair<String, String> equal) {
        String columnName = equal.first.toLowerCase(Locale.ROOT);
        String columnValue = equal.second;
        switch (columnName) {
            case "field":
                return col.getName().equals(columnValue);
            case "type":
                return col.getOriginType().toString().equals(columnValue);
            case "null":
                return col.isAllowNull() == Boolean.parseBoolean(columnValue);
            case "key":
                return col.isKey() == Boolean.parseBoolean(columnValue);
            case "default":
                return col.getDefaultValue().equals(columnValue);
            case "extra":
                return col.getAggregationType().toSql().equals(columnValue);
            case "comment":
                return col.getComment().equals(columnValue);
            default:
                return false;
        }
    }

    private boolean skipColumn(Column col) {
        for (Pair<String, String> equal : equalConditions) {
            if (!matchesCondition(col, equal)) {
                return true;
            }
        }
        return false;
    }

    private ShowResultSet handleShowColumn() throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf db = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(tableNameInfo.getCtl())
                .getDbOrAnalysisException(tableNameInfo.getDb());
        TableIf table = db.getTableOrAnalysisException(tableNameInfo.getTbl());
        PatternMatcher matcher = null;
        if (likePattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(likePattern,
                    CaseSensibility.COLUMN.getCaseSensibility());
        }
        table.readLock();
        try {
            List<Column> columns = table.getBaseSchema();
            for (Column col : columns) {
                if (matcher != null && !matcher.match(col.getName())) {
                    continue;
                }
                if (!equalConditions.isEmpty() && skipColumn(col)) {
                    continue;
                }
                final String columnName = col.getName();
                final String columnType = col.getOriginType().toString().toLowerCase(Locale.ROOT);
                final String isAllowNull = col.isAllowNull() ? "YES" : "NO";
                final String isKey = col.isKey() ? "YES" : "NO";
                final String defaultValue = col.getDefaultValue();
                final String aggType = col.getAggregationType() == null ? "" : col.getAggregationType().toSql();
                if (isVerbose) {
                    // Field Type Collation Null Key Default Extra
                    // Privileges Comment
                    rows.add(Lists.newArrayList(columnName,
                            columnType,
                            "",
                            isAllowNull,
                            isKey,
                            defaultValue,
                            aggType,
                            "",
                            col.getComment()));
                } else {
                    // Field Type Null Key Default Extra
                    rows.add(Lists.newArrayList(columnName,
                            columnType,
                            isAllowNull,
                            isKey,
                            defaultValue,
                            aggType));
                }
            }
        } finally {
            table.readUnlock();
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    private void parseWhereClause() throws AnalysisException {
        if (whereClause == null) {
            return;
        }
        List<Expression> conjunctions = ExpressionUtils.extractConjunction(whereClause);
        for (Expression conj : conjunctions) {
            if (conj instanceof EqualTo) {
                EqualTo equalTo = (EqualTo) conj;
                if (equalTo.left() instanceof UnboundSlot && equalTo.right() instanceof Literal) {
                    String columnName = ((UnboundSlot) equalTo.left()).getName();
                    String columnValue = ((Literal) equalTo.right()).getStringValue();
                    if (ALIAS_COLUMN_MAP.containsKey(columnName.toLowerCase(Locale.ROOT))) {
                        equalConditions.add(Pair.of(columnName, columnValue));
                    } else {
                        throw new AnalysisException("Unknown column: " + columnName);
                    }
                } else {
                    throw new AnalysisException("Unsupported where clause: " + conj.toSql());
                }
            }
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        parseWhereClause();
        return handleShowColumn();
    }

    /**
     * getMetaData
     */
    public ShowResultSetMetaData getMetaData() {
        if (isVerbose) {
            return META_DATA_VERBOSE;
        } else {
            return META_DATA;
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowColumnsCommand(this, context);
    }
}
