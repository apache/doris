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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.storage.ObjectStorageProperties;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.load.NereidsDataDescription;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * load OLAP table data from external bulk file
 */
public class LoadCommand extends Command implements NeedAuditEncryption, ForwardWithSync {
    public static final String TIMEOUT_PROPERTY = "timeout";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    public static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String CLUSTER_PROPERTY = "cluster";
    public static final String STRICT_MODE = "strict_mode";
    public static final String TIMEZONE = "timezone";
    public static final String LOAD_PARALLELISM = "load_parallelism";
    public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";
    public static final String PRIORITY = "priority";
    public static final String LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";

    // deprecated, keeping this property to make LoadStmt#checkProperties() happy
    public static final String USE_NEW_LOAD_SCAN_NODE = "use_new_load_scan_node";

    // for load data from Baidu Object Store(BOS) todo wait new property support
    public static final String BOS_ENDPOINT = "bos_endpoint";
    public static final String BOS_ACCESSKEY = "bos_accesskey";
    public static final String BOS_SECRET_ACCESSKEY = "bos_secret_accesskey";

    // mini load params
    public static final String KEY_IN_PARAM_COLUMNS = "columns";
    public static final String KEY_IN_PARAM_SET = "set";
    public static final String KEY_IN_PARAM_HLL = "hll";
    public static final String KEY_IN_PARAM_COLUMN_SEPARATOR = "column_separator";
    public static final String KEY_IN_PARAM_LINE_DELIMITER = "line_delimiter";
    public static final String KEY_IN_PARAM_PARTITIONS = "partitions";
    public static final String KEY_IN_PARAM_FORMAT_TYPE = "format";

    public static final String KEY_IN_PARAM_WHERE = "where";
    public static final String KEY_IN_PARAM_MAX_FILTER_RATIO = "max_filter_ratio";
    public static final String KEY_IN_PARAM_TIMEOUT = "timeout";
    public static final String KEY_IN_PARAM_TEMP_PARTITIONS = "temporary_partitions";
    public static final String KEY_IN_PARAM_NEGATIVE = "negative";
    public static final String KEY_IN_PARAM_STRICT_MODE = "strict_mode";
    public static final String KEY_IN_PARAM_TIMEZONE = "timezone";
    public static final String KEY_IN_PARAM_EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String KEY_IN_PARAM_JSONPATHS = "jsonpaths";
    public static final String KEY_IN_PARAM_JSONROOT = "json_root";
    public static final String KEY_IN_PARAM_STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String KEY_IN_PARAM_FUZZY_PARSE = "fuzzy_parse";
    public static final String KEY_IN_PARAM_NUM_AS_STRING = "num_as_string";
    public static final String KEY_IN_PARAM_MERGE_TYPE = "merge_type";
    public static final String KEY_IN_PARAM_DELETE_CONDITION = "delete";
    public static final String KEY_IN_PARAM_FUNCTION_COLUMN = "function_column";
    public static final String KEY_IN_PARAM_SEQUENCE_COL = "sequence_col";
    public static final String KEY_IN_PARAM_BACKEND_ID = "backend_id";
    public static final String KEY_SKIP_LINES = "skip_lines";
    public static final String KEY_TRIM_DOUBLE_QUOTES = "trim_double_quotes";
    public static final String PARTIAL_COLUMNS = "partial_columns";
    public static final String PARTIAL_UPDATE_NEW_KEY_POLICY = "partial_update_new_key_behavior";
    public static final String KEY_COMMENT = "comment";
    public static final String KEY_CLOUD_CLUSTER = "cloud_cluster";
    public static final String KEY_ENCLOSE = "enclose";
    public static final String KEY_ESCAPE = "escape";
    public static final ImmutableMap<String, Function> PROPERTIES_MAP = new ImmutableMap.Builder<String, Function>()
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
            .put(EXEC_MEM_LIMIT, new Function<String, Long>() {
                @Override
                public @Nullable Long apply(@Nullable String s) {
                    return Long.valueOf(s);
                }
            })
            .put(STRICT_MODE, new Function<String, Boolean>() {
                @Override
                public @Nullable Boolean apply(@Nullable String s) {
                    return Boolean.valueOf(s);
                }
            })
            .put(PARTIAL_COLUMNS, new Function<String, Boolean>() {
                @Override
                public @Nullable Boolean apply(@Nullable String s) {
                    return Boolean.valueOf(s);
                }
            })
            .put(PARTIAL_UPDATE_NEW_KEY_POLICY, new Function<String, TPartialUpdateNewRowPolicy>() {
                @Override
                public @Nullable TPartialUpdateNewRowPolicy apply(@Nullable String s) {
                    return TPartialUpdateNewRowPolicy.valueOf(s.toUpperCase());
                }
            })
            .put(TIMEZONE, new Function<String, String>() {
                @Override
                public @Nullable String apply(@Nullable String s) {
                    return s;
                }
            })
            .put(LOAD_PARALLELISM, new Function<String, Integer>() {
                @Override
                public @Nullable Integer apply(@Nullable String s) {
                    return Integer.valueOf(s);
                }
            })
            .put(SEND_BATCH_PARALLELISM, new Function<String, Integer>() {
                @Override
                public @Nullable Integer apply(@Nullable String s) {
                    return Integer.valueOf(s);
                }
            })
            .put(CLUSTER_PROPERTY, new Function<String, String>() {
                @Override
                public @Nullable String apply(@Nullable String s) {
                    return s;
                }
            })
            .put(LOAD_TO_SINGLE_TABLET, new Function<String, Boolean>() {
                @Override
                public @Nullable Boolean apply(@Nullable String s) {
                    return Boolean.valueOf(s);
                }
            })
            .put(USE_NEW_LOAD_SCAN_NODE, new Function<String, Boolean>() {
                @Override
                public @Nullable Boolean apply(@Nullable String s) {
                    return Boolean.valueOf(s);
                }
            })
            .put(KEY_SKIP_LINES, new Function<String, Integer>() {
                @Override
                public @Nullable Integer apply(@Nullable String s) {
                    return Integer.valueOf(s);
                }
            })
            .put(KEY_TRIM_DOUBLE_QUOTES, new Function<String, Boolean>() {
                @Override
                public @Nullable Boolean apply(@Nullable String s) {
                    return Boolean.valueOf(s);
                }
            })
            .put(PRIORITY, (Function<String, LoadTask.Priority>) s -> LoadTask.Priority.valueOf(s))
            .build();
    private static final Logger LOG = LogManager.getLogger(LoadCommand.class);
    private final LabelName label;
    private final List<NereidsDataDescription> dataDescriptions;
    private final BrokerDesc brokerDesc;
    private final ResourceDesc resourceDesc;
    private final Map<String, String> properties;
    private String user;

    private EtlJobType etlJobType = EtlJobType.UNKNOWN;

    private String comment;
    private String mysqlLoadId;
    private UserIdentity userIdentity;

    /**
     * constructor of LoadCommand
     */
    public LoadCommand(LabelName label, List<NereidsDataDescription> dataDescriptions, BrokerDesc brokerDesc,
                           ResourceDesc resourceDesc, Map<String, String> properties, String comment) {
        super(PlanType.LOAD_COMMAND);
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = brokerDesc;
        this.resourceDesc = resourceDesc;
        this.properties = properties;
        this.comment = comment != null ? comment : "";
    }

    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public LabelName getLabel() {
        return label;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public ResourceDesc getResourceDesc() {
        return resourceDesc;
    }

    public List<NereidsDataDescription> getDataDescriptions() {
        return dataDescriptions;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    /**
     * check for properties
     */
    public static void checkProperties(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            return;
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!PROPERTIES_MAP.containsKey(entry.getKey())) {
                throw new DdlException(entry.getKey() + " is invalid property");
            }
        }

        // exec mem
        final String execMemProperty = properties.get(EXEC_MEM_LIMIT);
        if (execMemProperty != null) {
            try {
                final long execMem = Long.valueOf(execMemProperty);
                if (execMem <= 0) {
                    throw new DdlException(EXEC_MEM_LIMIT + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(EXEC_MEM_LIMIT + " is not a number.");
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
        final String strictModeProperty = properties.get(STRICT_MODE);
        if (strictModeProperty != null) {
            if (!strictModeProperty.equalsIgnoreCase("true")
                    && !strictModeProperty.equalsIgnoreCase("false")) {
                throw new DdlException(STRICT_MODE + " is not a boolean");
            }
        }

        // partial update
        final String partialColumnsProperty = properties.get(PARTIAL_COLUMNS);
        if (partialColumnsProperty != null) {
            if (!partialColumnsProperty.equalsIgnoreCase("true")
                    && !partialColumnsProperty.equalsIgnoreCase("false")) {
                throw new DdlException(PARTIAL_COLUMNS + " is not a boolean");
            }
        }

        // partial update new key policy
        final String partialUpdateNewKeyPolicyProperty = properties.get(PARTIAL_UPDATE_NEW_KEY_POLICY);
        if (partialUpdateNewKeyPolicyProperty != null) {
            if (!partialUpdateNewKeyPolicyProperty.equalsIgnoreCase("append")
                    && !partialUpdateNewKeyPolicyProperty.equalsIgnoreCase("error")) {
                throw new DdlException(PARTIAL_UPDATE_NEW_KEY_POLICY + " should be one of [append, error], but found "
                    + partialUpdateNewKeyPolicyProperty);
            }
        }

        // time zone
        final String timezone = properties.get(TIMEZONE);
        if (timezone != null) {
            properties.put(TIMEZONE, TimeUtils.checkTimeZoneValidAndStandardize(
                    properties.getOrDefault(TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE)));
        }

        // send batch parallelism
        final String sendBatchParallelism = properties.get(SEND_BATCH_PARALLELISM);
        if (sendBatchParallelism != null) {
            try {
                final int sendBatchParallelismValue = Integer.valueOf(sendBatchParallelism);
                if (sendBatchParallelismValue < 1) {
                    throw new DdlException(SEND_BATCH_PARALLELISM + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(SEND_BATCH_PARALLELISM + " is not a number.");
            }
        }

        // priority
        final String priority = properties.get(PRIORITY);
        if (priority != null) {
            try {
                LoadTask.Priority.valueOf(priority);
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new DdlException(PRIORITY + " must be in [LOW/NORMAL/HIGH].");
            }
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (Strings.isNullOrEmpty(label.getDbName())) {
            if (Strings.isNullOrEmpty(ctx.getDatabase())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            label.setDbName(ctx.getDatabase());
        }
        FeNameFormat.checkLabel(label.getLabelName());

        if (dataDescriptions == null || dataDescriptions.isEmpty()) {
            throw new AnalysisException("No data file in load statement.");
        }
        // check data descriptions, support 2 cases bellow:
        // case 1: multi file paths, multi data descriptions
        // case 2: one hive table, one data description
        boolean isLoadFromTable = false;
        for (NereidsDataDescription dataDescription : dataDescriptions) {
            if (brokerDesc == null && resourceDesc == null) {
                dataDescription.setIsHadoopLoad(true);
            }
            String fullDbName = dataDescription.analyzeFullDbName(label.getDbName(), ctx);
            dataDescription.analyze(fullDbName);

            if (dataDescription.isLoadFromTable()) {
                isLoadFromTable = true;
            }
            Database db = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(fullDbName);
            OlapTable table = db.getOlapTableOrAnalysisException(dataDescription.getTableName());
            if (dataDescription.getMergeType() != LoadTask.MergeType.APPEND
                    && table.getKeysType() != KeysType.UNIQUE_KEYS) {
                throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
            }
            if (dataDescription.getMergeType() != LoadTask.MergeType.APPEND && !table.hasDeleteSign()) {
                throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
            }
            if (brokerDesc != null && !brokerDesc.isMultiLoadBroker()) {
                for (int i = 0; i < dataDescription.getFilePaths().size(); i++) {
                    String location = brokerDesc.getFileLocation(dataDescription.getFilePaths().get(i));
                    dataDescription.getFilePaths().set(i, location);
                    dataDescription.getFilePaths().set(i, dataDescription.getFilePaths().get(i));
                }
            }
        }
        if (isLoadFromTable) {
            if (dataDescriptions.size() > 1) {
                throw new AnalysisException("Only support one olap table load from one external table");
            }
            if (resourceDesc == null) {
                throw new AnalysisException("Load from table should use Spark Load");
            }
        }

        if (resourceDesc != null) {
            resourceDesc.analyze();
            etlJobType = resourceDesc.getEtlJobType();
            // check resource usage privilege
            if (!Env.getCurrentEnv().getAccessManager().checkResourcePriv(ConnectContext.get(),
                    resourceDesc.getName(), PrivPredicate.USAGE)) {
                throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                    + "'@'" + ConnectContext.get().getRemoteIP()
                    + "' for resource '" + resourceDesc.getName() + "'");
            }
        } else if (brokerDesc != null) {
            etlJobType = EtlJobType.BROKER;
            checkS3Param();
        } else {
            etlJobType = EtlJobType.UNKNOWN;
        }

        try {
            checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }

        user = ctx.getQualifiedUser();
        if (ctx.getCurrentUserIdentity() != null) {
            this.setUserIdentity(ctx.getCurrentUserIdentity());
        }

        handleLoadCommand(ctx, executor);
    }

    /**
     * check for s3 param
     */
    public void checkS3Param() throws UserException {
        if (brokerDesc.getFileType() != null && brokerDesc.getFileType().equals(TFileType.FILE_S3)) {
            ObjectStorageProperties storageProperties = (ObjectStorageProperties) brokerDesc.getStorageProperties();
            String endpoint = storageProperties.getEndpoint();
            checkEndpoint(endpoint);
            checkWhiteList(endpoint);
            List<String> filePaths = new ArrayList<>();
            if (dataDescriptions != null && !dataDescriptions.isEmpty()) {
                for (NereidsDataDescription dataDescription : dataDescriptions) {
                    if (dataDescription.getFilePaths() != null) {
                        for (String filePath : dataDescription.getFilePaths()) {
                            if (filePath != null && !filePath.isEmpty()) {
                                filePaths.add(filePath);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * check endpoint
     */
    private void checkEndpoint(String endpoint) throws UserException {
        HttpURLConnection connection = null;
        try {
            String urlStr = endpoint;
            // Add default protocol if not specified
            if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
                urlStr = "http://" + endpoint;
            }
            SecurityChecker.getInstance().startSSRFChecking(urlStr);
            URL url = new URL(urlStr);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(10000);
            connection.connect();
        } catch (Exception e) {
            LOG.warn("Failed to connect endpoint={}, err={}", endpoint, e);
            String msg;
            if (e instanceof UserException) {
                msg = ((UserException) e).getDetailMessage();
            } else {
                msg = e.getMessage();
            }
            throw new UserException(InternalErrorCode.GET_REMOTE_DATA_ERROR,
                "Failed to access object storage, message=" + msg, e);
        } finally {
            if (connection != null) {
                try {
                    connection.disconnect();
                } catch (Exception e) {
                    LOG.warn("Failed to disconnect connection, endpoint={}, err={}", endpoint, e);
                }
            }
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    }

    /**
     * check WhiteList
     */
    public void checkWhiteList(String endpoint) throws UserException {
        endpoint = endpoint.replaceFirst("^http://", "");
        endpoint = endpoint.replaceFirst("^https://", "");
        List<String> whiteList = new ArrayList<>(Arrays.asList(Config.s3_load_endpoint_white_list));
        whiteList.removeIf(String::isEmpty);
        if (!whiteList.isEmpty() && !whiteList.contains(endpoint)) {
            throw new UserException("endpoint: " + endpoint
                + " is not in s3 load endpoint white list: " + String.join(",", whiteList));
        }
    }

    /**
     * this method is from StmtExecutor.handleLoadStmt()
     */
    public void handleLoadCommand(ConnectContext ctx, StmtExecutor executor) {
        try {
            // EtlJobType jobType = loadStmt.getEtlJobType();
            if (etlJobType == EtlJobType.UNKNOWN) {
                throw new DdlException("Unknown load job type");
            }
            LoadManager loadManager = ctx.getEnv().getLoadManager();
            if (etlJobType == EtlJobType.LOCAL_FILE) {
                if (!ctx.getCapability().supportClientLocalFile()) {
                    ctx.getState().setError(ErrorCode.ERR_NOT_ALLOWED_COMMAND, "This client is not support"
                            + " to load client local file.");
                    return;
                }
                String loadId = UUID.randomUUID().toString();
                mysqlLoadId = loadId;
                LoadJobRowResult submitResult = loadManager.getMysqlLoadManager()
                        .executeMySqlLoadJobFromCommand(ctx, getDataDescriptions().get(0), loadId);
                ctx.getState().setOk(submitResult.getRecords(), submitResult.getWarnings(), submitResult.toString());
            } else {
                loadManager.createLoadJobFromCommand(this, executor, ctx);
                ctx.getState().setOk();
            }
        } catch (UserException e) {
            // Return message to info client what happened.
            if (LOG.isDebugEnabled()) {
                LOG.debug("DDL statement({}) process failed.", executor.getOriginStmt().originStmt, e);
            }
            ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + executor.getOriginStmt().originStmt + ") process failed.", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLoadCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.LOAD;
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
