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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.constants.AzureProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// LOAD statement, load files into tables.
//
// syntax:
//      LOAD LABEL load_label
//          (data_desc, ...)
//          [broker_desc]
//          [resource_desc]
//      [PROPERTIES (key1=value1, )]
//
//      load_label:
//          db_name.label_name
//
//      data_desc:
//          DATA INFILE ('file_path', ...)
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [COLUMNS TERMINATED BY separator ]
//          [(col1, ...)]
//          [SET (k1=f1(xx), k2=f2(xx))]
//
//      broker_desc:
//          WITH BROKER name
//          (key2=value2, ...)
//
//      resource_desc:
//          WITH RESOURCE name
//          (key3=value3, ...)
public class LoadStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(LoadStmt.class);

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
    // temp property, just make regression test happy.
    // should remove when Config.enable_new_load_scan_node is set to true by default.
    public static final String USE_NEW_LOAD_SCAN_NODE = "use_new_load_scan_node";

    // for load data from Baidu Object Store(BOS)
    public static final String BOS_ENDPOINT = "bos_endpoint";
    public static final String BOS_ACCESSKEY = "bos_accesskey";
    public static final String BOS_SECRET_ACCESSKEY = "bos_secret_accesskey";

    // for S3 load check
    public static final List<String> PROVIDERS =
            new ArrayList<>(Arrays.asList("cos", "oss", "s3", "obs", "bos", "azure"));

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

    public static final String KEY_COMMENT = "comment";

    public static final String KEY_CLOUD_CLUSTER = "cloud_cluster";

    public static final String KEY_ENCLOSE = "enclose";

    public static final String KEY_ESCAPE = "escape";

    private final LabelName label;
    private final List<DataDescription> dataDescriptions;
    private final BrokerDesc brokerDesc;
    private final ResourceDesc resourceDesc;
    private final Map<String, String> properties;
    private String user;

    private boolean isMysqlLoad = false;

    private EtlJobType etlJobType = EtlJobType.UNKNOWN;

    private String comment;

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

    public LoadStmt(DataDescription dataDescription, Map<String, String> properties, String comment) {
        this.label = new LabelName();
        this.dataDescriptions = Lists.newArrayList(dataDescription);
        this.brokerDesc = null;
        this.resourceDesc = null;
        this.properties = properties;
        this.user = null;
        this.isMysqlLoad = true;
        if (comment != null) {
            this.comment = comment;
        } else {
            this.comment = "";
        }
    }

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions,
                    BrokerDesc brokerDesc, Map<String, String> properties, String comment) {
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = brokerDesc;
        this.resourceDesc = null;
        this.properties = properties;
        this.user = null;
        if (comment != null) {
            this.comment = comment;
        } else {
            this.comment = "";
        }
    }

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions,
                    ResourceDesc resourceDesc, Map<String, String> properties, String comment) {
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = null;
        this.resourceDesc = resourceDesc;
        this.properties = properties;
        this.user = null;
        if (comment != null) {
            this.comment = comment;
        } else {
            this.comment = "";
        }
    }

    public LabelName getLabel() {
        return label;
    }

    public List<DataDescription> getDataDescriptions() {
        return dataDescriptions;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public ResourceDesc getResourceDesc() {
        return resourceDesc;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Deprecated
    public String getUser() {
        return user;
    }

    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public static void checkProperties(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            return;
        }

        for (Entry<String, String> entry : properties.entrySet()) {
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

        // time zone
        final String timezone = properties.get(TIMEZONE);
        if (timezone != null) {
            properties.put(TIMEZONE, TimeUtils.checkTimeZoneValidAndStandardize(
                    properties.getOrDefault(LoadStmt.TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE)));
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
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (!isMysqlLoad) {
            label.analyze(analyzer);
        }
        if (dataDescriptions == null || dataDescriptions.isEmpty()) {
            throw new AnalysisException("No data file in load statement.");
        }
        // check data descriptions, support 2 cases bellow:
        // case 1: multi file paths, multi data descriptions
        // case 2: one hive table, one data description
        boolean isLoadFromTable = false;
        for (DataDescription dataDescription : dataDescriptions) {
            if (brokerDesc == null && resourceDesc == null && !isMysqlLoad) {
                dataDescription.setIsHadoopLoad(true);
            }
            String fullDbName = dataDescription.analyzeFullDbName(label.getDbName(), analyzer);
            dataDescription.analyze(fullDbName);

            if (dataDescription.isLoadFromTable()) {
                isLoadFromTable = true;
            }
            Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(fullDbName);
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
                    StorageBackend.checkPath(dataDescription.getFilePaths().get(i),
                            brokerDesc.getStorageType(), "DATA INFILE must be specified.");
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

        // mysql load only have one data desc.
        if (isMysqlLoad && !dataDescriptions.get(0).isClientLocal()) {
            for (String path : dataDescriptions.get(0).getFilePaths()) {
                if (Config.mysql_load_server_secure_path.isEmpty()) {
                    throw new AnalysisException("Load local data from fe local is not enabled. If you want to use it,"
                            + " plz set the `mysql_load_server_secure_path` for FE to be a right path.");
                } else {
                    File file = new File(path);
                    try {
                        if (!(file.getCanonicalPath().startsWith(Config.mysql_load_server_secure_path))) {
                            throw new AnalysisException("Local file should be under the secure path of FE.");
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    if (!file.exists()) {
                        throw new AnalysisException("File: " + path + " is not exists.");
                    }
                }
            }
        }

        if (resourceDesc != null) {
            resourceDesc.analyze();
            etlJobType = resourceDesc.getEtlJobType();
            // check resource usage privilege
            if (!Env.getCurrentEnv().getAccessManager().checkResourcePriv(ConnectContext.get(),
                                                                         resourceDesc.getName(),
                                                                         PrivPredicate.USAGE)) {
                throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                                                    + "'@'" + ConnectContext.get().getRemoteIP()
                                                    + "' for resource '" + resourceDesc.getName() + "'");
            }
        } else if (brokerDesc != null) {
            etlJobType = EtlJobType.BROKER;
            checkS3Param();
        } else if (isMysqlLoad) {
            etlJobType = EtlJobType.LOCAL_FILE;
        } else {
            // if cluster is null, use default hadoop cluster
            // if cluster is not null, use this hadoop cluster
            etlJobType = EtlJobType.HADOOP;
        }

        try {
            checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }

        user = ConnectContext.get().getQualifiedUser();
    }


    private String getProviderFromEndpoint() {
        Map<String, String> properties = brokerDesc.getProperties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(S3Properties.PROVIDER)) {
                return entry.getValue();
            }
        }
        return S3Properties.S3_PROVIDER;
    }

    private String getBucketFromFilePath(String filePath) throws Exception {
        String[] parts = filePath.split("\\/\\/");
        if (parts.length < 2) {
            throw new Exception("filePath is not valid");
        }
        String buckt = parts[1].split("\\/")[0];
        return buckt;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean needAuditEncryption() {
        if (brokerDesc != null || resourceDesc != null) {
            return true;
        }
        return false;
    }

    public void setEtlJobType(EtlJobType etlJobType) {
        this.etlJobType = etlJobType;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOAD LABEL ").append(label.toSql()).append("\n");
        sb.append("(");
        Joiner.on(",\n").appendTo(sb, Lists.transform(dataDescriptions, new Function<DataDescription, Object>() {
            @Override
            public Object apply(DataDescription dataDescription) {
                return dataDescription.toSql();
            }
        })).append(")");
        if (brokerDesc != null) {
            sb.append("\n").append(brokerDesc.toSql());
        }
        if (resourceDesc != null) {
            sb.append("\n").append(resourceDesc.toSql());
        }

        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public RedirectStatus getRedirectStatus() {
        if (isMysqlLoad) {
            return RedirectStatus.NO_FORWARD;
        } else {
            return RedirectStatus.FORWARD_WITH_SYNC;
        }
    }

    private void checkEndpoint(String endpoint) throws UserException {
        HttpURLConnection connection = null;
        try {
            String urlStr = "http://" + endpoint;
            SecurityChecker.getInstance().startSSRFChecking(urlStr);
            URL url = new URL(urlStr);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(10000);
            connection.connect();
        } catch (Exception e) {
            LOG.warn("Failed to connect endpoint={}", endpoint, e);
            throw new UserException("Incorrect object storage info: " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.disconnect();
                } catch (Exception e) {
                    LOG.warn("Failed to disconnect connection, endpoint={}", endpoint, e);
                }
            }
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    }

    public void checkS3Param() throws UserException {
        Map<String, String> brokerDescProperties = brokerDesc.getProperties();
        if (brokerDescProperties.containsKey(S3Properties.Env.ENDPOINT)
                && brokerDescProperties.containsKey(S3Properties.Env.ACCESS_KEY)
                && brokerDescProperties.containsKey(S3Properties.Env.SECRET_KEY)
                && brokerDescProperties.containsKey(S3Properties.Env.REGION)) {
            String endpoint = brokerDescProperties.get(S3Properties.Env.ENDPOINT);
            endpoint = endpoint.replaceFirst("^http://", "");
            endpoint = endpoint.replaceFirst("^https://", "");
            brokerDescProperties.put(S3Properties.Env.ENDPOINT, endpoint);
            checkWhiteList(endpoint);
            if (AzureProperties.checkAzureProviderPropertyExist(brokerDescProperties)) {
                return;
            }
            checkEndpoint(endpoint);
            checkAkSk();
        }
    }

    public void checkWhiteList(String endpoint) throws UserException {
        List<String> whiteList = new ArrayList<>(Arrays.asList(Config.s3_load_endpoint_white_list));
        whiteList.removeIf(String::isEmpty);
        if (!whiteList.isEmpty() && !whiteList.contains(endpoint)) {
            throw new UserException("endpoint: " + endpoint
                    + " is not in s3 load endpoint white list: " + String.join(",", whiteList));
        }
    }

    private void checkAkSk() throws UserException {
        RemoteBase remote = null;
        ObjectInfo objectInfo = null;
        try {
            Map<String, String> brokerDescProperties = brokerDesc.getProperties();
            String provider = getProviderFromEndpoint();
            for (DataDescription dataDescription : dataDescriptions) {
                for (String filePath : dataDescription.getFilePaths()) {
                    String bucket = getBucketFromFilePath(filePath);
                    objectInfo = new ObjectInfo(ObjectStoreInfoPB.Provider.valueOf(provider.toUpperCase()),
                            brokerDescProperties.get(S3Properties.Env.ACCESS_KEY),
                            brokerDescProperties.get(S3Properties.Env.SECRET_KEY),
                            bucket, brokerDescProperties.get(S3Properties.Env.ENDPOINT),
                            brokerDescProperties.get(S3Properties.Env.REGION), "");
                    remote = RemoteBase.newInstance(objectInfo);
                    // RemoteBase#headObject does not throw exception if key does not exist.
                    remote.headObject("1");
                    remote.listObjects(null);
                    remote.close();
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed check object info={}", objectInfo, e);
            String message = e.getMessage();
            if (message != null) {
                int index = message.indexOf("Error message=");
                if (index != -1) {
                    message = message.substring(index);
                }
            }
            throw new UserException(InternalErrorCode.GET_REMOTE_DATA_ERROR,
                    "Incorrect object storage info, " + message);
        } finally {
            if (remote != null) {
                remote.close();
            }
        }

    }

    @Override
    public StmtType stmtType() {
        return StmtType.LOAD;
    }
}
