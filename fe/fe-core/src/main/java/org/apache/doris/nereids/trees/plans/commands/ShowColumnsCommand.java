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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
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
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * ShowColumnsCommand
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

    private static Map<String, String> ALIAS_COLUMN_MAP = ImmutableMap.<String, String>builder()
            .put("field", "COLUMN_NAME")
            .put("type", "DATA_TYPE")
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

    public ShowColumnsCommand(TableNameInfo tableNameInfo, boolean isVerbose, String likePattern,
                              Expression whereClause) {
        super(PlanType.SHOW_COLUMNS);
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

    /**
     * isShowTablesCaseSensitive
     */
    public boolean isShowTablesCaseSensitive() {
        if (GlobalVariable.lowerCaseTableNames == 0) {
            return CaseSensibility.TABLE.getCaseSensibility();
        }
        return false;
    }

    /**
     * execute sql and return result
     */
    private ShowResultSet execute(ConnectContext ctx, StmtExecutor executor, String whereClause) {
        List<AliasInfo> selectList = new ArrayList<>();
        ALIAS_COLUMN_MAP.forEach((key, value) -> {
            selectList.add(AliasInfo.of(value, key));
        });

        TableNameInfo fullTblName = new TableNameInfo(tableNameInfo.getCtl(), InfoSchemaDb.DATABASE_NAME, "columns");

        LogicalPlan plan = Utils.buildLogicalPlan(selectList, fullTblName, whereClause);
        List<List<String>> rows = Utils.executePlan(ctx, executor, plan);
        return new ShowResultSet(getMetaData(), rows);
    }

    /**
     * replaceColumnNameVisitor
     * replace column name to real column name
     */
    private static class ReplaceColumnNameVisitor extends DefaultExpressionRewriter<Void> {
        @Override
        public Expression visitUnboundSlot(UnboundSlot slot, Void context) {
            String columnName = ALIAS_COLUMN_MAP.get(slot.getName().toLowerCase(Locale.ROOT));
            if (columnName != null) {
                return UnboundSlot.quoted(columnName);
            }
            return slot;
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        String defaultClause = " TABLE_CATALOG = '" + tableNameInfo.getCtl()
                + "' AND TABLE_SCHEMA = '" + tableNameInfo.getDb()
                + "' AND TABLE_NAME = '" + tableNameInfo.getTbl() + "' ";
        if (whereClause != null) {
            Expression rewritten = whereClause.accept(new ReplaceColumnNameVisitor(), null);
            String whereCondition = " WHERE " + defaultClause + rewritten.toSql();
            return execute(ctx, executor, whereCondition);
        } else if (likePattern != null) {
            return execute(ctx, executor, " WHERE " + defaultClause
                    + " AND COLUMN_NAME LIKE '" + likePattern + "'");
        }
        return execute(ctx, executor, " WHERE " + defaultClause);
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
