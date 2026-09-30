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

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.SqlDialectHelper;
import org.apache.doris.nereids.rules.rewrite.CheckPrivileges;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PrepareCommandPlanner;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DescribeCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.HelpCommand;
import org.apache.doris.nereids.trees.plans.commands.KillCommand;
import org.apache.doris.nereids.trees.plans.commands.ReplayCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDataCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPartitionsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPythonPackagesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowQueryStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowSnapshotCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableCommand;
import org.apache.doris.nereids.trees.plans.commands.TransactionCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropIndexOp;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.commands.use.SwitchCommand;
import org.apache.doris.nereids.trees.plans.commands.use.UseCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Resolves result metadata without scheduling fragments or evaluating query expressions. */
final class FlightSqlQuerySchema {
    private FlightSqlQuerySchema() {
    }

    static Schema analyze(ConnectContext context, String query) throws Exception {
        synchronized (context) {
            ConnectContext previousThreadContext = ConnectContext.get();
            StatementContext previousStatement = context.getStatementContext();
            SessionVariable previousSession = context.getSessionVariable();
            QueryState previousState = context.getState();
            StmtExecutor previousExecutor = context.getExecutor();
            String previousCatalog = context.getDefaultCatalog();
            String previousDatabase = context.getDatabase();
            List<StatementBase> statements = Collections.emptyList();
            try {
                context.setThreadLocalInfo();
                context.setCommand(MysqlCommand.COM_QUERY);
                // Parsing SET_VAR hints already mutates session variables. Isolate them even when parsing fails.
                context.setSessionVariable(VariableMgr.cloneSessionVariable(previousSession));
                context.setState(new QueryState());
                context.setExecutor(null);
                context.setStatementContext(null);
                // Match execution's HTTP/plugin conversion before the dialect parser sees the SQL.
                String converted = SqlDialectHelper.convertSqlByDialect(query, context.getSessionVariable());
                try {
                    statements = new NereidsParser().parseSQL(converted, context.getSessionVariable());
                } catch (Exception convertedError) {
                    if (!context.getSessionVariable().isRetryOriginSqlOnConvertFail() || converted.equals(query)) {
                        throw convertedError;
                    }
                    // Match execution's parse fallback while discarding any failed parser context.
                    StatementContext failed = context.getStatementContext();
                    if (failed != null) {
                        failed.close();
                        context.setStatementContext(null);
                    }
                    statements = new NereidsParser().parseSQL(query, context.getSessionVariable());
                }
                Map<String, String> scopedDatabases = new HashMap<>();
                if (statements.isEmpty()) {
                    throw CallStatus.UNIMPLEMENTED.withDescription(
                            "Schema discovery requires a statement").toRuntimeException();
                }
                // JDBC clients commonly prefix their query with USE. Resolve that namespace only within this scope.
                for (int i = 0; i < statements.size() - 1; ++i) {
                    Plan prefix = ((LogicalPlanAdapter) statements.get(i)).getLogicalPlan();
                    if (!(prefix instanceof UseCommand) && !(prefix instanceof SwitchCommand)) {
                        throw CallStatus.UNIMPLEMENTED.withDescription(
                                "Schema discovery only supports USE or SWITCH before the result statement")
                                .toRuntimeException();
                    }
                    resolveNamespace(context, prefix, scopedDatabases);
                }
                LogicalPlanAdapter statement = (LogicalPlanAdapter) statements.get(statements.size() - 1);
                StatementContext statementContext = statement.getStatementContext();
                context.setStatementContext(statementContext);
                statementContext.setParsedStatement(statement);
                if (!statementContext.getPlaceholders().isEmpty()) {
                    throw CallStatus.UNIMPLEMENTED.withDescription(
                            "Flight SQL parameter binding is not supported").toRuntimeException();
                }
                List<Field> fields = new ArrayList<>();
                Plan plan = statement.getLogicalPlan();
                if (plan instanceof Command) {
                    resolveNamespace(context, plan, scopedDatabases);
                    ResultSetMetaData metadata = commandMetadata(context, (Command) plan);
                    if (metadata == null) {
                        throw CallStatus.UNIMPLEMENTED.withDescription("Command result metadata is unavailable")
                                .toRuntimeException();
                    }
                    // FE-local result sets are serialized as nullable strings by FlightSqlChannel.
                    for (Column column : metadata.getColumns()) {
                        fields.add(Field.nullable(column.getName(), new ArrowType.Utf8()));
                    }
                    if (fields.isEmpty()) {
                        switch (((Command) plan).stmtType()) {
                            case SET:
                            case USE:
                            case SWITCH:
                            case CREATE:
                            case ALTER:
                            case DROP:
                            case TRUNCATE:
                                // Only known no-row command categories have the protocol OK schema.
                                fields.add(Field.nullable("StatusResult", new ArrowType.Utf8()));
                                break;
                            default:
                                throw CallStatus.UNIMPLEMENTED.withDescription(
                                        "Result metadata is unavailable without executing this command")
                                        .toRuntimeException();
                        }
                    }
                } else {
                    // BindResultSink preserves SQL column labels only in query mode, just as
                    // StmtExecutor does; command mode would infer synthetic aliases instead.
                    context.getState().setIsQuery(true);
                    PrepareCommandPlanner planner = new PrepareCommandPlanner(statementContext);
                    planner.plan(statement, context.getSessionVariable().toThrift());
                    CascadesContext cascades = planner.getCascadesContext();
                    Plan analyzed = cascades.getRewritePlan();
                    // PrepareCommandPlanner stops before the rewrite phase that normally checks privileges.
                    new CheckPrivileges().rewriteRoot(analyzed, cascades.getCurrentJobContext());
                    for (Slot slot : analyzed.getOutput()) {
                        fields.add(field(slot.getName(), slot.getDataType().toCatalogDataType(), slot.nullable(),
                                true, context.getSessionVariable().getTimeZone()));
                    }
                }
                return new Schema(fields);
            } finally {
                try {
                    List<AutoCloseable> resources = new ArrayList<>();
                    for (StatementBase statement : statements) {
                        if (statement instanceof LogicalPlanAdapter) {
                            resources.add(((LogicalPlanAdapter) statement).getStatementContext());
                        }
                    }
                    // A parser failure can leave a context that was never added to the returned list.
                    StatementContext current = context.getStatementContext();
                    if (current != null && current != previousStatement && !resources.contains(current)) {
                        resources.add(current);
                    }
                    AutoCloseables.close(resources);
                } finally {
                    try {
                        context.setStatementContext(previousStatement);
                        context.setSessionVariable(previousSession);
                        context.setState(previousState);
                        context.setExecutor(previousExecutor);
                        if (!previousCatalog.equals(context.getDefaultCatalog())
                                || !previousDatabase.equals(context.getDatabase())) {
                            context.changeDefaultCatalog(previousCatalog);
                            context.setDatabase(previousDatabase);
                        }
                    } finally {
                        context.setCommand(MysqlCommand.COM_SLEEP);
                        if (previousThreadContext == null) {
                            ConnectContext.remove();
                        } else {
                            previousThreadContext.setThreadLocalInfo();
                        }
                    }
                }
            }
        }
    }

    static boolean matchesExecutionSchema(Schema prepared, Schema actual, List<String> columnLabels) {
        List<Field> expectedFields = prepared.getFields();
        List<Field> actualFields = actual.getFields();
        if (columnLabels == null || expectedFields.size() != actualFields.size()
                || expectedFields.size() != columnLabels.size()
                || !prepared.getCustomMetadata().equals(actual.getCustomMetadata())) {
            return false;
        }
        for (int i = 0; i < expectedFields.size(); ++i) {
            Field expected = expectedFields.get(i);
            // Compare the final planner's semantic labels: BE labels may instead contain SQL
            // text or type_name_index. Ignoring all names would hide a concurrent column rename.
            if (!expected.getName().equals(columnLabels.get(i))
                    || !matchesExecutionField(expected, actualFields.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesExecutionField(Field expected, Field actual) {
        // Rewrites can prove an expression non-null (e.g. a folded CAST). Such narrowing is
        // compatible with Prepare's nullable field; the reverse violates its advertised contract.
        if ((!expected.isNullable() && actual.isNullable()) || !expected.getType().equals(actual.getType())
                || !expected.getMetadata().equals(actual.getMetadata())
                || !Objects.equals(expected.getFieldType().getDictionary(), actual.getFieldType().getDictionary())
                || expected.getChildren().size() != actual.getChildren().size()) {
            return false;
        }
        for (int i = 0; i < expected.getChildren().size(); ++i) {
            Field child = expected.getChildren().get(i);
            Field actualChild = actual.getChildren().get(i);
            if (!child.getName().equals(actualChild.getName()) || !matchesExecutionField(child, actualChild)) {
                return false;
            }
        }
        return true;
    }

    private static ResultSetMetaData commandMetadata(ConnectContext context, Command command) throws Exception {
        // These getters depend on execution-time state or remote responses. Do not advertise a
        // guessed schema, or run the command merely to discover it.
        if (command instanceof ShowPythonPackagesCommand || command instanceof DescribeCommand
                || command instanceof ShowDataCommand || command instanceof ShowPartitionsCommand
                || command instanceof ShowQueryStatsCommand) {
            throw CallStatus.UNIMPLEMENTED.withDescription("Command schema requires execution-time metadata")
                    .toRuntimeException();
        }
        if (command instanceof HelpCommand) {
            return ((HelpCommand) command).getMetaData(context);
        } else if (command instanceof ShowSnapshotCommand) {
            return ((ShowSnapshotCommand) command).getMetaData(context);
        } else if (command instanceof ShowTableCommand) {
            ((ShowTableCommand) command).validate(context);
        } else if (command instanceof ShowCreateTableCommand) {
            return ((ShowCreateTableCommand) command).getMetaData(context);
        } else if (command instanceof ShowProcCommand) {
            return ((ShowProcCommand) command).getMetaData(context);
        }
        if (command instanceof ExplainCommand) {
            // PLAN PROCESS has no Flight serialization path in StmtExecutor.
            if (((ExplainCommand) command).showPlanProcess()) {
                throw CallStatus.UNIMPLEMENTED.withDescription("EXPLAIN PLAN PROCESS is not supported over Flight SQL")
                        .toRuntimeException();
            }
            return stringMetadata("Explain String(Nereids Planner)");
        } else if (command instanceof ReplayCommand) {
            return stringMetadata("Plan Replayer dump url");
        } else if (command instanceof AlterTableCommand) {
            AlterTableCommand alter = (AlterTableCommand) command;
            String catalog = alter.getTbl().getCtl();
            // Lance index admission returns a JobId header even for an IF no-op. Do not run
            // validation/admission here: those paths can resolve remote tables or allocate IDs.
            if (context.getCatalog(catalog == null ? context.getDefaultCatalog() : catalog)
                    instanceof LanceExternalCatalog && alter.getNereidsOps().stream().anyMatch(op ->
                        (op instanceof CreateIndexOp && !((CreateIndexOp) op).isAlter())
                                || (op instanceof DropIndexOp && !((DropIndexOp) op).isAlter()))) {
                return stringMetadata("JobId");
            }
        }
        ResultSetMetaData metadata = command.getResultSetMetaData();
        // Concrete DML commands return OK, but subclasses such as WARM UP SELECT supply rows.
        if (metadata != null && metadata.getColumnCount() == 0 && (command instanceof InsertIntoTableCommand
                || command instanceof InsertOverwriteTableCommand || command instanceof BatchInsertIntoTableCommand
                || command instanceof InsertIntoTVFCommand || command instanceof UpdateCommand
                || command instanceof DeleteFromCommand || command instanceof MergeIntoCommand
                || command instanceof KillCommand || command instanceof TransactionCommand)) {
            return stringMetadata("StatusResult");
        }
        return metadata;
    }

    private static ResultSetMetaData stringMetadata(String name) {
        return ShowResultSetMetaData.builder().addColumn(new Column(name, Type.STRING)).build();
    }

    private static void resolveNamespace(ConnectContext context, Plan plan, Map<String, String> scopedDatabases)
            throws Exception {
        if (plan instanceof UseCommand) {
            UseCommand use = (UseCommand) plan;
            String catalog = use.getCatalogName() == null ? context.getDefaultCatalog() : use.getCatalogName();
            CatalogIf catalogObject = context.getCatalog(catalog);
            if (catalogObject == null || !context.getEnv().getAccessManager()
                    .checkDbPriv(context, catalog, use.getDatabaseName(), PrivPredicate.SHOW)) {
                throw CallStatus.UNAUTHORIZED.withDescription("Database access denied").toRuntimeException();
            }
            catalogObject.getDbOrAnalysisException(use.getDatabaseName());
            if (use.getCatalogName() != null && !context.getDatabase().isEmpty()) {
                scopedDatabases.put(context.getDefaultCatalog(), context.getDatabase());
            }
            context.changeDefaultCatalog(catalog);
            context.setDatabase(use.getDatabaseName());
        } else if (plan instanceof SwitchCommand) {
            String catalog = ((SwitchCommand) plan).getCatalogName();
            if (context.getCatalog(catalog) == null || !context.getEnv().getAccessManager()
                    .checkCtlPriv(context, catalog, PrivPredicate.SHOW)) {
                throw CallStatus.UNAUTHORIZED.withDescription("Catalog access denied").toRuntimeException();
            }
            // Mirror Env.changeCatalog, keeping remembered databases local to this analysis.
            if (!context.getDatabase().isEmpty()) {
                scopedDatabases.put(context.getDefaultCatalog(), context.getDatabase());
            }
            String database = scopedDatabases.getOrDefault(catalog, context.getLastDBOfCatalog(catalog));
            context.changeDefaultCatalog(catalog);
            if (database != null && !database.isEmpty()) {
                context.setDatabase(database);
            }
            if (context.getCatalog(catalog) instanceof EsExternalCatalog) {
                context.setDatabase(EsExternalCatalog.DEFAULT_DB);
            }
        }
    }

    private static Field field(String name, Type type, boolean nullable, boolean topLevel, String timezone) {
        // group_concat uses IAggregateFunction's string serialization, unlike fixed-size states
        // such as sum/count. Match that BE wire type instead of treating every AGG_STATE as Null.
        if (type instanceof AggStateType && "group_concat".equals(((AggStateType) type).getFunctionName())) {
            type = Type.STRING;
        }
        PrimitiveType primitive = type.getPrimitiveType();
        int precision = type instanceof ScalarType ? ((ScalarType) type).getScalarPrecision() : 0;
        int scale = type instanceof ScalarType ? ((ScalarType) type).getScalarScale() : 0;
        ArrowType arrowType = FlightSqlSchemaHelper.getArrowType(primitive, precision, scale);
        if (primitive == PrimitiveType.TIMESTAMPTZ) {
            arrowType = new ArrowType.Timestamp(((ArrowType.Timestamp) arrowType).getUnit(),
                    "Z".equals(timezone) ? "UTC" : timezone);
        }
        if (arrowType instanceof ArrowType.Null && primitive != PrimitiveType.NULL_TYPE) {
            throw CallStatus.UNIMPLEMENTED.withDescription("Unsupported Arrow result type: " + type)
                    .toRuntimeException();
        }
        List<Field> children = new ArrayList<>();
        if (type instanceof ArrayType) {
            // BE constructs ListType and MapType from data types, so item/value fields are nullable.
            children.add(field("item", ((ArrayType) type).getItemType(), true, false, timezone));
        } else if (type instanceof MapType) {
            MapType map = (MapType) type;
            children.add(new Field("entries", FieldType.notNullable(new ArrowType.Struct()), Arrays.asList(
                    field("key", map.getKeyType(), false, false, timezone),
                    field("value", map.getValueType(), true, false, timezone))));
        } else if (type instanceof StructType) {
            for (StructField child : ((StructType) type).getFields()) {
                children.add(field(child.getName(), child.getType(), child.getContainsNull(), false, timezone));
            }
        }
        Map<String, String> metadata = null;
        if (topLevel && (primitive == PrimitiveType.LARGEINT || primitive == PrimitiveType.IPV4
                || primitive == PrimitiveType.IPV6)) {
            metadata = Collections.singletonMap("doris_type", primitive.toString());
        }
        return new Field(name, new FieldType(nullable, arrowType, null, metadata), children);
    }
}
