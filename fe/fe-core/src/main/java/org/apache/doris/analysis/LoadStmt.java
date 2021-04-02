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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// LOAD statement, load files into tables.
//
// syntax:
//      LOAD LABEL load_label
//          (data_desc, ...)
//          [broker_desc]
//          [BY cluster]
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
    public static final String TIMEOUT_PROPERTY = "timeout";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    public static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String CLUSTER_PROPERTY = "cluster";
    public static final String STRICT_MODE = "strict_mode";
    public static final String TIMEZONE = "timezone";
    public static final String LOAD_PARALLELISM = "load_parallelism";

    // for load data from Baidu Object Store(BOS)
    public static final String BOS_ENDPOINT = "bos_endpoint";
    public static final String BOS_ACCESSKEY = "bos_accesskey";
    public static final String BOS_SECRET_ACCESSKEY = "bos_secret_accesskey";

    // mini load params
    public static final String KEY_IN_PARAM_COLUMNS = "columns";
    public static final String KEY_IN_PARAM_SET= "set";
    public static final String KEY_IN_PARAM_HLL= "hll";
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
    public static final String KEY_IN_PARAM_JSONPATHS  = "jsonpaths";
    public static final String KEY_IN_PARAM_JSONROOT  = "json_root";
    public static final String KEY_IN_PARAM_STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String KEY_IN_PARAM_FUZZY_PARSE = "fuzzy_parse";
    public static final String KEY_IN_PARAM_MERGE_TYPE = "merge_type";
    public static final String KEY_IN_PARAM_DELETE_CONDITION = "delete";
    public static final String KEY_IN_PARAM_FUNCTION_COLUMN = "function_column";
    public static final String KEY_IN_PARAM_SEQUENCE_COL = "sequence_col";
    public static final String KEY_IN_PARAM_BACKEND_ID = "backend_id";

    private final LabelName label;
    private final List<DataDescription> dataDescriptions;
    private final BrokerDesc brokerDesc;
    private final String cluster;
    private final ResourceDesc resourceDesc;
    private final Map<String, String> properties;
    private String user;

    private EtlJobType etlJobType = EtlJobType.UNKNOWN;

    public final static ImmutableMap<String, Function> PROPERTIES_MAP = new ImmutableMap.Builder<String, Function>()
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
            .put(CLUSTER_PROPERTY, new Function<String, String>() {
                @Override
                public @Nullable String apply(@Nullable String s) {
                    return s;
                }
            })
            .build();

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions,
                    BrokerDesc brokerDesc, String cluster, Map<String, String> properties) {
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = brokerDesc;
        this.cluster = cluster;
        this.resourceDesc = null;
        this.properties = properties;
        this.user = null;
    }

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions,
                    ResourceDesc resourceDesc, Map<String, String> properties) {
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = null;
        this.cluster = null;
        this.resourceDesc = resourceDesc;
        this.properties = properties;
        this.user = null;
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

    public String getCluster() {
        return cluster;
    }

    public ResourceDesc getResourceDesc() {
        return resourceDesc;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

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

        // time zone
        final String timezone = properties.get(TIMEZONE);
        if (timezone != null) {
            properties.put(TIMEZONE, TimeUtils.checkTimeZoneValidAndStandardize(
                    properties.getOrDefault(LoadStmt.TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE)));
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        label.analyze(analyzer);
        if (dataDescriptions == null || dataDescriptions.isEmpty()) {
            throw new AnalysisException("No data file in load statement.");
        }
        // check data descriptions, support 2 cases bellow:
        // case 1: multi file paths, multi data descriptions
        // case 2: one hive table, one data description
        boolean isLoadFromTable = false;
        for (DataDescription dataDescription : dataDescriptions) {
            if (brokerDesc == null && resourceDesc == null) {
                dataDescription.setIsHadoopLoad(true);
            }
            dataDescription.analyze(label.getDbName());

            if (dataDescription.isLoadFromTable()) {
                isLoadFromTable = true;
            }
            Database db = Catalog.getCurrentCatalog().getDb(label.getDbName());
            if (db == null) {
                throw new AnalysisException("database: " + label.getDbName() + "not  found.");
            }
            Table table = db.getTable(dataDescription.getTableName());
            if (dataDescription.getMergeType() != LoadTask.MergeType.APPEND &&
                    (!(table instanceof OlapTable) || ((OlapTable) table).getKeysType() != KeysType.UNIQUE_KEYS)) {
                throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
            }
            if (dataDescription.getMergeType() != LoadTask.MergeType.APPEND
                    && !((table instanceof OlapTable) && ((OlapTable) table).hasDeleteSign()) ) {
                throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
            }
            if (brokerDesc != null && !brokerDesc.isMultiLoadBroker()) {
                for (int i = 0; i < dataDescription.getFilePaths().size(); i++) {
                    dataDescription.getFilePaths().set(i,
                        brokerDesc.convertPathToS3(dataDescription.getFilePaths().get(i)));
                    dataDescription.getFilePaths().set(i,
                        ExportStmt.checkPath(dataDescription.getFilePaths().get(i), brokerDesc.getStorageType()));
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
            // TODO(wyb): spark-load
            if (!Config.enable_spark_load) {
                throw new AnalysisException("Spark Load is coming soon");
            }
            // check resource usage privilege
            if (!Catalog.getCurrentCatalog().getAuth().checkResourcePriv(ConnectContext.get(),
                                                                         resourceDesc.getName(),
                                                                         PrivPredicate.USAGE)) {
                throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                                                    + "'@'" + ConnectContext.get().getRemoteIP()
                                                    + "' for resource '" + resourceDesc.getName() + "'");
            }
        } else if (brokerDesc != null) {
            etlJobType = EtlJobType.BROKER;
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
        if (cluster != null) {
            sb.append("\nBY '");
            sb.append(cluster);
            sb.append("'");
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
}
