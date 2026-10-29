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

package org.apache.doris.arrowflight;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.arrow.DorisArrowTypeMapping;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
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
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DescribeCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.HelpCommand;
import org.apache.doris.nereids.trees.plans.commands.KillCommand;
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

/**
 * Resolves the result schema of a Flight SQL statement without scheduling fragments or evaluating
 * query expressions: what Prepare advertises and GetSchema answers, and what a prepared execution
 * is checked against.
 */
final class FlightSqlQuerySchema {
    private FlightSqlQuerySchema() {
    }

    /**
     * The statement's result schema can only be told by running it: DESCRIBE and the other commands
     * whose result columns depend on what they find, a request whose statements before the last are
     * not USE or SWITCH, a query with parameter placeholders, a result type this frontend cannot
     * describe as the backend sends it. Prepare advertises the placeholder schema for such a
     * statement and does not check its executions; GetSchema answers UNIMPLEMENTED.
     */
    static final class UndiscoverableSchemaException extends Exception {
        UndiscoverableSchemaException(String message) {
            super(message);
        }
    }

    /**
     * The result schema of {@code query} in the session's current namespace and settings. It runs as
     * a command of the session ({@link org.apache.doris.arrowflight.protocol.FlightProtocolAdapter#callCommand},
     * which makes the context the thread's own), and puts back everything of the session it borrows.
     */
    static Schema analyze(ConnectContext context, String query) throws Exception {
        StatementContext previousStatement = context.getStatementContext();
        SessionVariable previousSession = context.getSessionVariable();
        QueryState previousState = context.getState();
        StmtExecutor previousExecutor = context.getExecutor();
        NameSpaceContext previousNamespace = context.getNameSpaceContext();
        List<StatementBase> statements = Collections.emptyList();
        try {
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
            if (statements.isEmpty()) {
                throw new UndiscoverableSchemaException("Schema discovery requires a statement");
            }
            Map<String, String> scopedDatabases = new HashMap<>();
            // JDBC clients commonly prefix their query with USE. Resolve that namespace only within this scope.
            for (int i = 0; i < statements.size() - 1; ++i) {
                Plan prefix = ((LogicalPlanAdapter) statements.get(i)).getLogicalPlan();
                if (!(prefix instanceof UseCommand) && !(prefix instanceof SwitchCommand)) {
                    throw new UndiscoverableSchemaException(
                            "Schema discovery only supports USE or SWITCH before the result statement");
                }
                resolveNamespace(context, prefix, scopedDatabases);
            }
            LogicalPlanAdapter statement = (LogicalPlanAdapter) statements.get(statements.size() - 1);
            StatementContext statementContext = statement.getStatementContext();
            context.setStatementContext(statementContext);
            statementContext.setParsedStatement(statement);
            if (!statementContext.getPlaceholders().isEmpty()) {
                throw new UndiscoverableSchemaException("Flight SQL parameter binding is not supported");
            }
            List<Field> fields = new ArrayList<>();
            Plan plan = statement.getLogicalPlan();
            if (plan instanceof Command) {
                resolveNamespace(context, plan, scopedDatabases);
                ResultSetMetaData metadata = commandMetadata(context, (Command) plan);
                if (metadata == null) {
                    throw new UndiscoverableSchemaException("Command result metadata is unavailable");
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
                            throw new UndiscoverableSchemaException(
                                    "Result metadata is unavailable without executing this command");
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
                // The zone BE stamps on TIMESTAMPTZ: the one it is sent, see CoordinatorContext.
                String timezone = TimeUtils.getCanonicalTimeZone(context.getSessionVariable().getTimeZone());
                for (Slot slot : analyzed.getOutput()) {
                    fields.add(field(slot.getName(), slot.getDataType().toCatalogDataType(), slot.nullable(),
                            timezone));
                }
            }
            return new Schema(fields);
        } finally {
            try {
                List<AutoCloseable> resources = new ArrayList<>();
                for (StatementBase statement : statements) {
                    addAnalysisResources(resources, ((LogicalPlanAdapter) statement).getStatementContext());
                }
                // A parser failure can leave a context that was never added to the returned list.
                StatementContext current = context.getStatementContext();
                if (current != null && current != previousStatement && !resources.contains(current)) {
                    addAnalysisResources(resources, current);
                }
                AutoCloseables.close(resources);
            } finally {
                context.setStatementContext(previousStatement);
                context.setSessionVariable(previousSession);
                context.setState(previousState);
                context.setExecutor(previousExecutor);
                // Restoring the captured namespace does not contact a catalog.
                context.restoreNameSpaceContext(previousNamespace);
                context.setCommand(MysqlCommand.COM_SLEEP);
            }
        }
    }

    private static void addAnalysisResources(List<AutoCloseable> resources, StatementContext statementContext) {
        resources.add(statementContext);
        // close() leaves the connector statement scope of a Flight statement to the query-finish
        // cleanup of the query that runs it. Nothing runs an analysis, so its scope is closed here.
        resources.add(statementContext::closeConnectorStatementScope);
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
            throw new UndiscoverableSchemaException("Command schema requires execution-time metadata");
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
            // The headers StmtExecutor sends for the plan process and for the plan text.
            if (((ExplainCommand) command).showPlanProcess()) {
                return ShowResultSetMetaData.builder()
                        .addColumn(new Column("Rule", Type.STRING))
                        .addColumn(new Column("Before", Type.STRING))
                        .addColumn(new Column("After", Type.STRING))
                        .build();
            }
            return stringMetadata("Explain String(Nereids Planner)");
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
            CatalogIf catalogObject = context.getCatalog(catalog);
            if (catalogObject == null || !context.getEnv().getAccessManager()
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
            Env.defaultDatabaseOnUse(catalogObject).ifPresent(context::setDatabase);
        }
    }

    /**
     * The Arrow field BE sends for a result column of {@code type} ({@code convert_to_arrow_type} in
     * be/src/format/arrow/arrow_row_batch.cpp), with the {@code doris_type} markers execution adds to
     * it, at every depth ({@link FlightSqlSchemaHelper#withDorisTypeMetadata}).
     */
    private static Field field(String name, Type type, boolean nullable, String timezone)
            throws UndiscoverableSchemaException {
        return FlightSqlSchemaHelper.withDorisTypeMetadata(arrowField(name, type, nullable, timezone), type);
    }

    private static Field arrowField(String name, Type type, boolean nullable, String timezone)
            throws UndiscoverableSchemaException {
        // group_concat uses IAggregateFunction's string serialization, unlike fixed-size states
        // such as sum/count. Match that BE wire type instead of treating every AGG_STATE as Null.
        if (type instanceof AggStateType && "group_concat".equals(((AggStateType) type).getFunctionName())) {
            type = Type.STRING;
        }
        PrimitiveType primitive = type.getPrimitiveType();
        if (primitive == PrimitiveType.VARIANT) {
            return FlightSqlSchemaHelper.nativeVariantField(name, nullable, Collections.emptyMap());
        }
        int precision = type instanceof ScalarType ? ((ScalarType) type).getScalarPrecision() : 0;
        int scale = type instanceof ScalarType ? ((ScalarType) type).getScalarScale() : 0;
        ArrowType arrowType = DorisArrowTypeMapping.toArrowType(primitive, precision, scale);
        if (primitive == PrimitiveType.TIMESTAMPTZ) {
            // BE stamps the session time zone, where the shared mapping still says "UTC" (#67577).
            arrowType = new ArrowType.Timestamp(((ArrowType.Timestamp) arrowType).getUnit(), timezone);
        }
        if (arrowType instanceof ArrowType.Null && primitive != PrimitiveType.NULL_TYPE) {
            throw new UndiscoverableSchemaException("Unsupported Arrow result type: " + type);
        }
        List<Field> children = new ArrayList<>();
        if (type instanceof ArrayType) {
            // BE constructs ListType and MapType from data types, so item/value fields are nullable.
            children.add(arrowField("item", ((ArrayType) type).getItemType(), true, timezone));
        } else if (type instanceof MapType) {
            MapType map = (MapType) type;
            children.add(new Field("entries", FieldType.notNullable(new ArrowType.Struct()), Arrays.asList(
                    arrowField("key", map.getKeyType(), false, timezone),
                    arrowField("value", map.getValueType(), true, timezone))));
        } else if (type instanceof StructType) {
            for (StructField child : ((StructType) type).getFields()) {
                children.add(arrowField(child.getName(), child.getType(), child.getContainsNull(), timezone));
            }
        }
        return new Field(name, new FieldType(nullable, arrowType, null), children);
    }
}
