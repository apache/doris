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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.thrift.TStatusCode;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Spark 4.1 / Iceberg 1.11 remove_orphan_files, using the branch-4.1 EXECUTE entry point. */
public class IcebergRemoveOrphanFilesAction extends BaseIcebergAction {
    private static final Pattern SUB_MICROSECONDS = Pattern.compile("(\\.\\d{6})\\d+");
    private volatile Status cancellation;
    private volatile StmtExecutor fileListQuery;
    private long deadlineMillis = Long.MAX_VALUE;
    private ZoneId zone;
    private long olderThanMillis;
    private String fileListSql;

    public IcebergRemoveOrphanFilesAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitions, Optional<Expression> where) {
        super("remove_orphan_files", properties, partitions, where);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerOptionalArgument("older_than", "File retention cutoff in the session timezone",
                null, ArgumentParsers.nonEmptyString("older_than"));
        namedArguments.registerOptionalArgument("location", "Directory or file-list prefix to examine",
                null, value -> value);
        namedArguments.registerOptionalArgument("dry_run", "Return candidates without deleting files",
                false, ArgumentParsers.booleanValue("dry_run"));
        namedArguments.registerOptionalArgument("max_concurrent_deletes", "Non-bulk delete thread count",
                null, ArgumentParsers.positiveInt("max_concurrent_deletes"));
        namedArguments.registerOptionalArgument("file_list_view", "A queryable file_path/last_modified relation",
                null, ArgumentParsers.nonEmptyString("file_list_view"));
        namedArguments.registerOptionalArgument("equal_schemes", "JSON object of equivalent schemes",
                Collections.emptyMap(), IcebergRemoveOrphanFilesAction::parseEquivalences);
        namedArguments.registerOptionalArgument("equal_authorities", "JSON object of equivalent authorities",
                Collections.emptyMap(), IcebergRemoveOrphanFilesAction::parseEquivalences);
        namedArguments.registerOptionalArgument("prefix_mismatch_mode", "ERROR, IGNORE or DELETE",
                PrefixMismatchMode.ERROR, PrefixMismatchMode::fromString);
        namedArguments.registerOptionalArgument("prefix_listing", "List via SupportsPrefixOperations",
                false, ArgumentParsers.booleanValue("prefix_listing"));
        namedArguments.registerOptionalArgument("stream_results", "Consume all candidates, return at most 20000 paths",
                false, ArgumentParsers.booleanValue("stream_results"));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        validateNoPartitions();
        validateNoWhereCondition();
        zone = TimeUtils.getDorisZoneId();
        olderThanMillis = retentionCutoff(namedArguments.getString("older_than"), zone, System.currentTimeMillis());
        String view = namedArguments.getString("file_list_view");
        fileListSql = view == null ? null : fileListSql(view);
    }

    static long retentionCutoff(String text, ZoneId zone, long nowMillis) throws AnalysisException {
        if (text == null) {
            return nowMillis - TimeUnit.DAYS.toMillis(3);
        }
        Instant instant = timestamp(text, zone);
        // ProcedureInput.asTimestampMillis converts Spark's microsecond value with TimeUnit.
        long micros = Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000);
        long cutoff = TimeUnit.MICROSECONDS.toMillis(micros);
        if (cutoff > nowMillis - TimeUnit.DAYS.toMillis(1)) {
            throw new AnalysisException("Cannot remove orphan files with an interval less than 24 hours");
        }
        return cutoff;
    }

    static Instant timestamp(String text, ZoneId zone) throws AnalysisException {
        try {
            // Spark timestamps truncate beyond microseconds; Doris literals otherwise round.
            String microseconds = SUB_MICROSECONDS.matcher(text).replaceAll("$1");
            TemporalAccessor parsed = DateLiteral.parseDateTime(microseconds).get();
            LocalDateTime local = LocalDateTime.of(DateUtils.getOrDefault(parsed, ChronoField.YEAR),
                    DateUtils.getOrDefault(parsed, ChronoField.MONTH_OF_YEAR),
                    DateUtils.getOrDefault(parsed, ChronoField.DAY_OF_MONTH),
                    DateUtils.getOrDefault(parsed, ChronoField.HOUR_OF_DAY),
                    DateUtils.getOrDefault(parsed, ChronoField.MINUTE_OF_HOUR),
                    DateUtils.getOrDefault(parsed, ChronoField.SECOND_OF_MINUTE),
                    DateUtils.getOrDefault(parsed, ChronoField.NANO_OF_SECOND));
            ZoneId specifiedZone = parsed.query(TemporalQueries.zone());
            // Resolve directly to an instant: a session-wall-time roundtrip loses explicit
            // offsets during DST overlaps. Java's overlap/gap resolution matches Spark.
            return local.atZone(specifiedZone == null ? zone : specifiedZone).toInstant();
        } catch (RuntimeException e) {
            throw new AnalysisException("Invalid timestamp: " + text, e);
        }
    }

    static Map<String, String> parseEquivalences(String text) {
        try {
            JsonElement parsed = JsonParser.parseString(text);
            if (!parsed.isJsonObject()) {
                throw new IllegalArgumentException("Expected a JSON object with string values");
            }
            JsonObject object = parsed.getAsJsonObject();
            Map<String, String> result = new LinkedHashMap<>();
            for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
                if (!entry.getValue().isJsonPrimitive() || !entry.getValue().getAsJsonPrimitive().isString()) {
                    throw new IllegalArgumentException("Expected a string value for " + entry.getKey());
                }
                for (String key : entry.getKey().split(",", -1)) {
                    result.put(key.trim(), entry.getValue().getAsString().trim());
                }
            }
            return result;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid equivalence JSON object", e);
        }
    }

    static String fileListSql(String name) throws AnalysisException {
        try {
            Expression expression = new NereidsParser().parseExpression(name);
            if (!(expression instanceof UnboundSlot)) {
                throw new IllegalArgumentException("Expected a table or view name");
            }
            List<String> parts = ((UnboundSlot) expression).getNameParts();
            if (parts.isEmpty() || parts.size() > 3) {
                throw new IllegalArgumentException("Expected [catalog.][database.]table");
            }
            String quoted = parts.stream().map(part -> "`" + part.replace("`", "``") + "`")
                    .collect(Collectors.joining("."));
            return "SELECT `file_path`, `last_modified` FROM " + quoted;
        } catch (RuntimeException e) {
            throw new AnalysisException("Invalid file_list_view name: " + name, e);
        }
    }

    @Override
    protected List<Column> getResultSchema() {
        return Collections.singletonList(new Column("orphan_file_location", Type.STRING, false));
    }

    @Override
    protected List<List<String>> executeAction(TableIf table) throws UserException {
        ConnectContext parent = ConnectContext.get();
        long started = parent.getStartTime() > 0 ? parent.getStartTime() : System.currentTimeMillis();
        deadlineMillis = started + TimeUnit.SECONDS.toMillis(parent.getExecTimeoutS());
        checkCancelled();
        IcebergExternalTable dorisTable = (IcebergExternalTable) table;
        Table writable = dorisTable.getWritableIcebergTable();
        IcebergExternalCatalog catalog = (IcebergExternalCatalog) dorisTable.getCatalog();
        ExecutionAuthenticator authenticator = catalog.getExecutionAuthenticator();
        Configuration conf = new Configuration(catalog.getConfiguration());
        try {
            return authenticator.execute(() -> {
                TableMetadata metadata = ((HasTableOperations) writable).operations().current();
                Table frozen = new BaseTable(new StaticTableOperations(metadata, writable.io()), writable.name());
                Map<String, String> schemes = new LinkedHashMap<>();
                schemes.put("s3a", "s3");
                schemes.put("s3n", "s3");
                schemes.putAll(namedArguments.getValue("equal_schemes"));
                String location = namedArguments.getString("location");
                String scanLocation = location == null ? frozen.location() : location;
                IcebergOrphanFiles operation = new IcebergOrphanFiles(frozen, conf, scanLocation, olderThanMillis,
                        schemes, namedArguments.getValue("equal_authorities"),
                        namedArguments.getValue("prefix_mismatch_mode"), this::checkCancelled,
                        authenticator, Paths.get(Config.tmp_dir));
                Consumer<Consumer<String>> fileList = fileListSql == null ? null
                        : consumer -> queryFileList(parent, scanLocation, consumer);
                return operation.execute(fileList, namedArguments.getBoolean("prefix_listing"),
                        namedArguments.getBoolean("dry_run"), namedArguments.getBoolean("stream_results"),
                        namedArguments.getInt("max_concurrent_deletes"));
            });
        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            throw new UserException("Failed to remove orphan files: " + e.getMessage(), e);
        }
    }

    private void queryFileList(ConnectContext parent, String location, Consumer<String> consume) {
        ConnectContext child;
        try {
            child = fileListContext(parent);
        } catch (DdlException e) {
            throw new IllegalStateException("Cannot initialize file_list_view query", e);
        }
        child.setStartTime();
        // This is a fresh query, but its remaining time belongs to the outer EXECUTE statement.
        checkCancelled();
        long remainingSeconds = Math.max(1, (deadlineMillis - System.currentTimeMillis() + 999) / 1000);
        child.getSessionVariable().setQueryTimeoutS((int) Math.min(Integer.MAX_VALUE, remainingSeconds));
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(child)) {
            try {
                StmtExecutor executor = new StmtExecutor(child, fileListSql);
                fileListQuery = executor;
                checkCancelled();
                executor.executeInternalQuery(() -> {
                    checkCancelled();
                    validateFileListTypes(executor.getReturnTypes());
                }, rows -> {
                    for (ResultRow row : rows) {
                        checkCancelled();
                        String path = row.get(0);
                        String modified = row.get(1);
                        // Spark applies startsWith(location) and timestamp < cutoff before URI parsing.
                        if (path != null && modified != null && (location == null || path.startsWith(location))) {
                            try {
                                if (timestamp(modified, zone).isBefore(Instant.ofEpochMilli(olderThanMillis))) {
                                    consume.accept(path);
                                }
                            } catch (AnalysisException e) {
                                throw new IllegalArgumentException(e.getMessage(), e);
                            }
                        }
                    }
                });
            } finally {
                fileListQuery = null;
                if (child.getStatementContext() != null) {
                    child.getStatementContext().close();
                }
            }
        }
    }

    static ConnectContext fileListContext(ConnectContext parent) throws DdlException {
        ConnectContext child = new ConnectContext(null, false, parent.getSessionId());
        child.setEnv(parent.getEnv());
        child.setSessionVariable(VariableMgr.cloneSessionVariable(parent.getSessionVariable()));
        child.setCurrentUserIdentity(parent.getCurrentUserIdentity());
        child.setAuthenticatedPrincipal(parent.getAuthenticatedPrincipal());
        child.setAuthenticatedRoles(new HashSet<>(parent.getAuthenticatedRoles()));
        child.setIsTempUser(parent.getIsTempUser());
        child.setRemoteIP(parent.getRemoteIP());
        child.setConnectionId(parent.getConnectionId());
        child.setUserVars(new HashMap<>(parent.getUserVars()));
        // Seed the previous-query value through the existing lifecycle API without borrowing
        // the parent's active query ID. executeInternalQuery will assign a fresh child ID.
        child.setQueryId(parent.getLastQueryId());
        child.resetQueryId();
        child.changeDefaultCatalog(parent.getDefaultCatalog());
        child.setDatabase(parent.getDatabase());
        // Caller presentation/benchmark options must not truncate or suppress procedure input.
        // Explicit LIMIT clauses stored in the selected view remain part of that view's plan.
        VariableMgr.setVar(child.getSessionVariable(),
                new SetVar(SessionVariable.SQL_SELECT_LIMIT, new StringLiteral(Long.toString(Long.MAX_VALUE))));
        VariableMgr.setVar(child.getSessionVariable(),
                new SetVar(SessionVariable.DEFAULT_ORDER_BY_LIMIT, new StringLiteral("-1")));
        child.getSessionVariable().dryRunQuery = false;
        child.getSessionVariable().enableSqlCache = false;
        child.getSessionVariable().enableQueryCache = false;
        return child;
    }

    static void validateFileListTypes(List<Type> types) {
        if (types.size() != 2 || !types.get(0).isVarcharOrStringType()
                || !(types.get(1).isDatetime() || types.get(1).isDatetimeV2())) {
            throw new IllegalArgumentException("file_list_view requires file_path STRING/VARCHAR and "
                    + "last_modified DATETIME/DATETIMEV2 columns");
        }
    }

    @Override
    public void cancel(Status reason) {
        cancellation = reason;
        StmtExecutor query = fileListQuery;
        if (query != null) {
            query.cancel(reason);
        }
    }

    private void checkCancelled() {
        if (System.currentTimeMillis() >= deadlineMillis) {
            cancellation = new Status(TStatusCode.TIMEOUT, "remove_orphan_files exceeded the statement timeout");
        }
        if (cancellation != null || Thread.currentThread().isInterrupted()) {
            throw new IllegalStateException("remove_orphan_files cancelled: "
                    + (cancellation == null ? "interrupted" : cancellation.getErrorMsg()));
        }
    }

    @Override
    public String getDescription() {
        return "Remove Iceberg files not referenced by retained metadata";
    }
}
