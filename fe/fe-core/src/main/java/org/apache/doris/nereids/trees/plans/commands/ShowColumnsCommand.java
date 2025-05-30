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
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AliasInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Represents the SHOW COLUMNS command.
 */
public class ShowColumnsCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Field", ScalarType.createVarchar(20)))
            .addColumn(new Column("Type", ScalarType.createVarchar(20)))
            .addColumn(new Column("Null", ScalarType.createVarchar(20)))
            .addColumn(new Column("Key", ScalarType.createVarchar(20)))
            .addColumn(new Column("Default", ScalarType.createVarchar(20)))
            .addColumn(new Column("Extra", ScalarType.createVarchar(20))).build();

    private static final ShowResultSetMetaData META_DATA_VERBOSE =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Privileges", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(20)))
                    .build();

    private final boolean isFull;
    private TableNameInfo tableNameInfo;
    private final String databaseName;
    private final String likePattern;
    private final Expression whereClause;

    /**
     * SHOW COLUMNS command Constructor.
     */
    public ShowColumnsCommand(boolean isFull, TableNameInfo tableNameInfo, String databaseName, String likePattern,
                Expression whereClause) {
        super(PlanType.SHOW_COLUMNS_COMMAND);
        this.isFull = isFull;
        this.tableNameInfo = tableNameInfo;
        this.databaseName = databaseName;
        this.likePattern = likePattern;
        this.whereClause = whereClause;
    }

    /**
     * SHOW COLUMNS validate.
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!Strings.isNullOrEmpty(databaseName)) {
            tableNameInfo.setDb(databaseName);
        }
        tableNameInfo.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                        tableNameInfo.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), tableNameInfo);
        }
    }

    /**
     * replaceColumnNameVisitor
     * replace column name to real column name
     */
    private static class ReplaceColumnNameVisitor extends DefaultExpressionRewriter<Void> {
        @Override
        public Expression visitUnboundSlot(UnboundSlot slot, Void context) {
            String name = slot.getName().toLowerCase(Locale.ROOT);
            switch (name) {
                case "field":
                    return UnboundSlot.quoted("COLUMN_NAME");
                case "type":
                    return UnboundSlot.quoted("COLUMN_TYPE");
                case "null":
                    return UnboundSlot.quoted("IS_NULLABLE");
                case "default":
                    return UnboundSlot.quoted("COLUMN_DEFAULT");
                case "comment":
                    return UnboundSlot.quoted("COLUMN_COMMENT");
                default:
                    return slot;
            }
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        if (whereClause != null) {
            Expression rewritten = whereClause.accept(new ReplaceColumnNameVisitor(), null);
            String whereCondition = " WHERE TABLE_NAME = '" + tableNameInfo.getTbl() + "' AND " + rewritten.toSql();
            TableNameInfo info = new TableNameInfo(tableNameInfo.getCtl(), "information_schema", "columns");

            List<AliasInfo> selectList = new ArrayList<>();
            if (isFull) {
                selectList.add(AliasInfo.of("COLUMN_NAME", "Field"));
                selectList.add(AliasInfo.of("COLUMN_TYPE", "Type"));
                selectList.add(AliasInfo.of("COLLATION_NAME", "Collation"));
                selectList.add(AliasInfo.of("IS_NULLABLE", "Null"));
                selectList.add(AliasInfo.of("COLUMN_KEY", "Key"));
                selectList.add(AliasInfo.of("COLUMN_DEFAULT", "Default"));
                selectList.add(AliasInfo.of("EXTRA", "Extra"));
                selectList.add(AliasInfo.of("PRIVILEGES", "Privileges")); // optional, can be set to ''
                selectList.add(AliasInfo.of("COLUMN_COMMENT", "Comment"));
            } else {
                selectList.add(AliasInfo.of("COLUMN_NAME", "Field"));
                selectList.add(AliasInfo.of("COLUMN_TYPE", "Type"));
                selectList.add(AliasInfo.of("IS_NULLABLE", "Null"));
                selectList.add(AliasInfo.of("COLUMN_KEY", "Key"));
                selectList.add(AliasInfo.of("COLUMN_DEFAULT", "Default"));
                selectList.add(AliasInfo.of("EXTRA", "Extra"));
            }

            LogicalPlan plan = Utils.buildLogicalPlan(selectList, info, whereCondition);
            List<List<String>> rows = Utils.executePlan(ctx, executor, plan);
            for (List<String> row : rows) {
                String rawType = row.get(1);
                row.set(1, normalizeSqlColumnType(rawType));
            }

            return new ShowResultSet(getMetaData(), rows);
        }
        List<List<String>> rows = Lists.newArrayList();
        String ctl = tableNameInfo.getCtl();
        DatabaseIf db = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(ctl)
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
                final String columnName = col.getName();
                final String columnType = col.getOriginType().toString().toLowerCase(Locale.ROOT);
                final String isAllowNull = col.isAllowNull() ? "YES" : "NO";
                final String isKey = col.isKey() ? "YES" : "NO";
                final String defaultValue = col.getDefaultValue();
                final String aggType = col.getAggregationType() == null ? "" : col.getAggregationType().toSql();
                if (isFull) {
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

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowColumnsCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (isFull) {
            return META_DATA_VERBOSE;
        } else {
            return META_DATA;
        }
    }

    private static String normalizeSqlColumnType(String type) {
        if (type == null) {
            return null;
        }

        type = type.toLowerCase().trim();

        if (type.matches("^[a-z]+\\s*\\(.*\\)$")) {
            int parenIndex = type.indexOf('(');
            return type.substring(0, parenIndex).trim();
        }
        return type;
    }
}
