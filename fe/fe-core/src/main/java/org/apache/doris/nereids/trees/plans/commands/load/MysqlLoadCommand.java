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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.NoForward;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

// LOAD command, load files into tables.
//
// syntax:
// LOAD mysqlDataDesc
// [PROPERTIES (key1=value1, )]
// [commentSpec]
//
// mysqlDataDesc:
//    DATA [ LOCAL ]
//    INFILE "<file_name>"
//    INTO TABLE "<tbl_name>"
//    [ PARTITION (<partition_name> [, ... ]) ]
//    [ COLUMNS TERMINATED BY "<column_separator>" ]
//    [ LINES TERMINATED BY "<line_delimiter>" ]
//    [ IGNORE <number> {LINES | ROWS} ]
//    [ (<col_name_or_user_var> [, ... ] ) ]
//    [ SET (col_name={<expr> | DEFAULT} [, col_name={<expr> | DEFAULT}] ...) ]
//    [ PROPERTIES ("<key>" = "<value>" [ , ... ]) ]
// commentSpec:
//    COMMENT ...

/**
 * MysqlLoadCommand
 */
public class MysqlLoadCommand extends Command implements NoForward {
    public static final String TIMEOUT_PROPERTY = "timeout";
    public static final String EXEC_MEM_LIMIT_PROPERTY = "exec_mem_limit";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    public static final String STRICT_MODE_PROPERTY = "strict_mode";
    public static final String TIMEZONE_PROPERTY = "timezone";
    public static final String ENCLOSE_PROPERTY = "enclose";
    public static final String ESCAPE_PROPERTY = "escape";
    public static final String TRIM_DOUBLE_QUOTES_PROPERTY = "trim_double_quotes";
    public static final String KEY_SKIP_LINES = "skip_lines";
    public static final String KEY_IN_PARAM_COLUMN_SEPARATOR = "column_separator";
    public static final String KEY_IN_PARAM_LINE_DELIMITER = "line_delimiter";
    public static final String KEY_IN_PARAM_COLUMNS = "columns";
    public static final String KEY_IN_PARAM_TEMP_PARTITIONS = "temporary_partitions";
    public static final String KEY_IN_PARAM_PARTITIONS = "partitions";
    public static final String KEY_CLOUD_CLUSTER = "cloud_cluster";
    private static final Logger LOG = LogManager.getLogger(MysqlLoadCommand.class);

    private static final ImmutableMap<String, Function> PROPERTIES_MAP = new ImmutableMap.Builder<String, Function>()
            .put(TIMEOUT_PROPERTY, new Function<String, Long>() {
                @Override
                public @Nullable Long apply(@Nullable String s) {
                    return Long.valueOf(s);
                }
            })
            .put(MAX_FILTER_RATIO_PROPERTY, new Function<String, Double>() {
                @Override
                public @Nullable Double apply(@Nullable String s) {
                    return Double.valueOf(s);
                }
            })
            .put(EXEC_MEM_LIMIT_PROPERTY, new Function<String, Long>() {
                @Override
                public @Nullable Long apply(@Nullable String s) {
                    return Long.valueOf(s);
                }
            })
            .put(STRICT_MODE_PROPERTY, new Function<String, Boolean>() {
                @Override
                public @Nullable Boolean apply(@Nullable String s) {
                    return Boolean.valueOf(s);
                }
            })
            .put(TIMEZONE_PROPERTY, new Function<String, String>() {
                @Override
                public @Nullable String apply(@Nullable String s) {
                    return s;
                }
            })
            .put(TRIM_DOUBLE_QUOTES_PROPERTY, new Function<String, Boolean>() {
                @Override
                public @Nullable Boolean apply(@Nullable String s) {
                    return Boolean.valueOf(s);
                }
            })
            .put(ENCLOSE_PROPERTY, new Function<String, String>() {
                @Override
                public @Nullable String apply(@Nullable String s) {
                    return s;
                }
            })
            .put(ESCAPE_PROPERTY, new Function<String, String>() {
                @Override
                public @Nullable String apply(@Nullable String s) {
                    return s;
                }
            })
            .build();

    private final MysqlDataDescription mysqlDataDescription;
    private final Map<String, String> properties;
    private final EtlJobType etlJobType = EtlJobType.LOCAL_FILE;
    private final String comment;

    /**
     * MysqlLoadCommand
     */
    public MysqlLoadCommand(MysqlDataDescription mysqlDataDescription, Map<String, String> properties, String comment) {
        super(PlanType.MYSQL_LOAD_COMMAND);
        Objects.requireNonNull(mysqlDataDescription, "mysqlDataDescription is null");
        Objects.requireNonNull(properties, "properties is null");
        Objects.requireNonNull(comment, "comment is null");

        this.mysqlDataDescription = mysqlDataDescription;
        this.properties = properties;
        this.comment = comment;
    }

    public MysqlDataDescription getMysqlDataDescription() {
        return mysqlDataDescription;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        handleMysqlLoadComand(ctx);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException, IOException {
        if (mysqlDataDescription == null) {
            throw new AnalysisException("No data file in mysql load command.");
        }

        // check data descriptions, only support one file path:
        String fullDbName = mysqlDataDescription.analyzeFullDbName(ctx);
        mysqlDataDescription.analyze(fullDbName);
        if (!mysqlDataDescription.isClientLocal()) {
            for (String path : mysqlDataDescription.getFilePaths()) {
                if (Config.mysql_load_server_secure_path.isEmpty()) {
                    throw new AnalysisException("Load local data from fe local is not enabled. If you want to use it,"
                        + " please set the `mysql_load_server_secure_path` for FE to be a right path.");
                } else {
                    File file = new File(path);
                    if (!file.getCanonicalPath().startsWith(Config.mysql_load_server_secure_path)) {
                        throw new AnalysisException("Local file should be under the secure path of FE.");
                    }
                    if (!file.exists()) {
                        throw new AnalysisException("File: " + path + " is not exists.");
                    }
                }
            }
        }

        try {
            checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    private void handleMysqlLoadComand(ConnectContext ctx) {
        try {
            LoadManager loadManager = ctx.getEnv().getLoadManager();
            if (!ctx.getCapability().supportClientLocalFile()) {
                ctx.getState().setError(ErrorCode.ERR_NOT_ALLOWED_COMMAND, "This client is not support"
                        + " to load client local file.");
                return;
            }
            String loadId = UUID.randomUUID().toString();
            LoadJobRowResult submitResult = loadManager.getMysqlLoadManager()
                    .executeMySqlLoadJob(ctx, mysqlDataDescription, loadId);
            ctx.getState().setOk(submitResult.getRecords(), submitResult.getWarnings(),
                    submitResult.toString());
        } catch (UserException e) {
            // Return message to info client what happened.
            if (LOG.isDebugEnabled()) {
                LOG.debug("DDL statement({}) process failed.", toSql(), e);
            }
            ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + toSql() + ") process failed.", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        }
    }

    /**
     * checkProperties
     */
    public static void checkProperties(Map<String, String> properties) throws DdlException {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!PROPERTIES_MAP.containsKey(entry.getKey())) {
                throw new DdlException(entry.getKey() + " is invalid property");
            }
        }

        // exec mem
        final String execMemProperty = properties.get(EXEC_MEM_LIMIT_PROPERTY);
        if (execMemProperty != null) {
            try {
                final long execMem = Long.valueOf(execMemProperty);
                if (execMem <= 0) {
                    throw new DdlException(EXEC_MEM_LIMIT_PROPERTY + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(EXEC_MEM_LIMIT_PROPERTY + " is not a number.");
            }
        }

        // timeout
        final String timeoutLimitProperty = properties.get(TIMEOUT_PROPERTY);
        if (timeoutLimitProperty != null) {
            try {
                final int timeoutLimit = Integer.valueOf(timeoutLimitProperty);
                if (timeoutLimit < 0) {
                    throw new DdlException(TIMEOUT_PROPERTY + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(TIMEOUT_PROPERTY + " is not a number.");
            }
        }

        // max filter ratio
        final String maxFilterRadioProperty = properties.get(MAX_FILTER_RATIO_PROPERTY);
        if (maxFilterRadioProperty != null) {
            try {
                double maxFilterRatio = Double.valueOf(maxFilterRadioProperty);
                if (maxFilterRatio < 0.0 || maxFilterRatio > 1.0) {
                    throw new DdlException(MAX_FILTER_RATIO_PROPERTY + " must between 0.0 and 1.0.");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(MAX_FILTER_RATIO_PROPERTY + " is not a number.");
            }
        }

        // strict mode
        final String strictModeProperty = properties.get(STRICT_MODE_PROPERTY);
        if (strictModeProperty != null) {
            if (!strictModeProperty.equalsIgnoreCase("true")
                    && !strictModeProperty.equalsIgnoreCase("false")) {
                throw new DdlException(STRICT_MODE_PROPERTY + " is not a boolean");
            }
        }

        // time zone
        final String timezone = properties.get(TIMEZONE_PROPERTY);
        if (timezone != null) {
            properties.put(TIMEZONE_PROPERTY, TimeUtils.checkTimeZoneValidAndStandardize(
                    properties.getOrDefault(TIMEZONE_PROPERTY, TimeUtils.DEFAULT_TIME_ZONE)));
        }
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOAD").append("\n");
        sb.append("(");
        sb.append(mysqlDataDescription.toSql()).append(")");
        if (!properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<>(properties, "=", true, false, true));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitMysqlLoadCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.LOAD;
    }
}
