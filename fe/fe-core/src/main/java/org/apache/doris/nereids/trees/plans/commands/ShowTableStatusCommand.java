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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ShowTableStautsCommand
 */
public class ShowTableStatusCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Name", ScalarType.createVarchar(64)))
            .addColumn(new Column("Engine", ScalarType.createVarchar(10)))
            .addColumn(new Column("Version", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Row_format", ScalarType.createVarchar(64)))
            .addColumn(new Column("Rows", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Avg_row_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Data_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Max_data_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Index_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Data_free", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Auto_increment", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Create_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("Update_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("Check_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("Collation", ScalarType.createVarchar(64)))
            .addColumn(new Column("Checksum", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Create_options", ScalarType.createVarchar(64)))
            .addColumn(new Column("Comment", ScalarType.createVarchar(64)))
            .build();

    private static Map<String, String> ALIAS_COLUMN_MAP = ImmutableMap.<String, String>builder()
            .put("name", "TABLE_NAME")
            .put("engine", "ENGINE")
            .put("version", "VERSION")
            .put("row_format", "ROW_FORMAT")
            .put("rows", "TABLE_ROWS")
            .put("avg_row_length", "AVG_ROW_LENGTH")
            .put("data_length", "DATA_LENGTH")
            .put("max_data_length", "MAX_DATA_LENGTH")
            .put("index_length", "INDEX_LENGTH")
            .put("data_free", "DATA_FREE")
            .put("auto_increment", "AUTO_INCREMENT")
            .put("create_time", "CREATE_TIME")
            .put("update_time", "UPDATE_TIME")
            .put("check_time", "CHECK_TIME")
            .put("collation", "TABLE_COLLATION")
            .put("checksum", "CHECKSUM")
            .put("create_options", "CREATE_OPTIONS")
            .put("comment", "TABLE_COMMENT")
            .build();

    private String catalog;
    private String db;
    private final String likePattern;
    private final Expression whereClause;

    public ShowTableStatusCommand(String db, String catalog) {
        this(db, catalog, null, null);
    }

    /**
     * ShowTableStautsCommand
     */
    public ShowTableStatusCommand(String db, String catalog,
                                  String likePattern, Expression whereClause) {
        super(PlanType.SHOW_TABLES_STATUS);
        this.catalog = catalog;
        this.db = db;
        this.likePattern = likePattern;
        this.whereClause = whereClause;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = ctx.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
            }
        }

        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                catalog, db, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, ctx.getQualifiedUser(), db);
        }
    }

    /**
     * sql to logical plan
     * @param sql sql
     */
    private LogicalPlan toLogicalPlan(String sql) {
        return new NereidsParser().parseSingle(sql);
    }

    /**
     * Construct a basic SQL without query conditions
     * @param columns (original name, alias)
     * @param tableName ctl.db.tbl
     */
    private String toBaseSql(List<Pair<String, String>> columns, String tableName) throws AnalysisException {
        if (columns == null || columns.isEmpty()) {
            throw new AnalysisException("columns cannot be empty");
        }
        if (tableName == null || tableName.isEmpty()) {
            throw new AnalysisException("tableName cannot be empty");
        }

        StringBuilder sb = new StringBuilder("SELECT ");
        columns.forEach(column -> {
            sb.append(column.first);
            if (column.second != null && !column.second.isEmpty()) {
                sb.append(" AS ").append(column.second);
            }
            sb.append(", ");
        });
        sb.setLength(sb.length() - 2);
        sb.append(" FROM ").append(tableName);
        return sb.toString();
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

    /**
     * execute sql and return result
     */
    private ShowResultSet execute(ConnectContext ctx, StmtExecutor executor, String whereClause)
            throws AnalysisException {
        List<Pair<String, String>> columns = new ArrayList<>();
        ALIAS_COLUMN_MAP.forEach((key, value) -> {
            columns.add(Pair.of("`" + value + "`", "'" + key + "'"));
        });

        String fullTblName = String.format("`%s`.`%s`.`%s`",
                catalog,
                InfoSchemaDb.DATABASE_NAME,
                "tables");

        // We need to use TABLE_SCHEMA as a condition to query When querying external catalogs.
        // This also applies to the internal catalog.
        LogicalPlan plan = toLogicalPlan(toBaseSql(columns, fullTblName) + whereClause);
        LogicalPlanAdapter adapter = new LogicalPlanAdapter(plan, ctx.getStatementContext());
        executor.setParsedStmt(adapter);
        List<ResultRow> resultRows = executor.executeInternalQuery();
        List<List<String>> rows = resultRows.stream()
                .map(ResultRow::getValues).collect(Collectors.toList());
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        if (whereClause != null) {
            Expression rewrited = whereClause.accept(new ReplaceColumnNameVisitor(), null);
            String whereCondition = " WHERE `TABLE_SCHEMA` = '" + db + "' AND " + rewrited.toSql();
            return execute(ctx, executor, whereCondition);
        } else if (likePattern != null) {
            return execute(ctx, executor, " WHERE TABLE_NAME LIKE '"
                    + likePattern + "' and `TABLE_SCHEMA` = '" + db + "'");
        }
        return execute(ctx, executor, "WHERE `TABLE_SCHEMA` = '" + db + "'");
    }

    /**
     * getMetaData
     */
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStatusCommand(this, context);
    }
}
