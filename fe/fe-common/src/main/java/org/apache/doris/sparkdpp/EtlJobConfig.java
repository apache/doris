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

package org.apache.doris.sparkdpp;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** jobconfig.json file format
{
    "tables": {
        10014: {
            "indexes": [{
                "indexId": 10014,
                "columns": [{
                    "columnName": "k1",
                    "columnType": "SMALLINT",
                    "isKey": true,
                    "isAllowNull": true,
                    "aggregationType": "NONE"
                }, {
                    "columnName": "k2",
                    "columnType": "VARCHAR",
                    "stringLength": 20,
                    "isKey": true,
                    "isAllowNull": true,
                    "aggregationType": "NONE"
                }, {
                    "columnName": "v",
                    "columnType": "BIGINT",
                    "isKey": false,
                    "isAllowNull": false,
                    "aggregationType": "NONE"
                }],
                "schemaHash": 1294206574,
                "indexType": "DUPLICATE",
                "isBaseIndex": true
            }, {
                "indexId": 10017,
                "columns": [{
                    "columnName": "k1",
                    "columnType": "SMALLINT",
                    "isKey": true,
                    "isAllowNull": true,
                    "aggregationType": "NONE"
                }, {
                    "columnName": "v",
                    "columnType": "BIGINT",
                    "isKey": false,
                    "isAllowNull": false,
                    "aggregationType": "BITMAP_UNION",
                    "defineExpr": "to_bitmap(v)"
                }],
                "schemaHash": 1294206575,
                "indexType": "AGGREGATE",
                "isBaseIndex": false
            }],
            "partitionInfo": {
                "partitionType": "RANGE",
                "partitionColumnRefs": ["k1"],
                "distributionColumnRefs": ["k2"],
                 "partitions": [{
                    "partitionId": 10020,
                    "startKeys": [-100],
                    "endKeys": [10],
                    "isMaxPartition": false,
                    "bucketNum": 3
                }, {
                    "partitionId": 10021,
                    "startKeys": [10],
                    "endKeys": [100],
                    "isMaxPartition": false,
                    "bucketNum": 3
                }]
            },
            "fileGroups": [{
                "partitions": [10020],
                "filePaths": ["hdfs://hdfs_host:port/user/palo/test/file"],
                "fileFieldNames": ["tmp_k1", "k2"],
                "valueSeparator": ",",
                "lineDelimiter": "\n",
                "columnMappings": {
                    "k1": {
                        "functionName": "strftime",
                        "args": ["%Y-%m-%d %H:%M:%S", "tmp_k1"]
                    }
                },
                "where": "k2 > 10",
                "isNegative": false,
                "hiveDbTableName": "hive_db.table",
                "hiveTableProperties": {
                    "hive.metastore.uris": "thrift://host:port"
                }
            }]
        }
    },
    "outputPath": "hdfs://hdfs_host:port/user/output/10003/label1/1582599203397",
    "outputFilePattern": "V1.label1.%d.%d.%d.%d.%d.parquet",
    "label": "label0",
    "properties": {
        "strictMode": false,
        "timezone": "Asia/Shanghai"
    },
    "version": "V1"
}
 */
public class EtlJobConfig implements Serializable {
    // global dict
    public static final String GLOBAL_DICT_TABLE_NAME = "doris_global_dict_table_%d";
    public static final String DISTINCT_KEY_TABLE_NAME = "doris_distinct_key_table_%d_%s";
    public static final String DORIS_INTERMEDIATE_HIVE_TABLE_NAME = "doris_intermediate_hive_table_%d_%s";

    // hdfsEtlPath/jobs/dbId/loadLabel/PendingTaskSignature
    private static final String ETL_OUTPUT_PATH_FORMAT = "%s/jobs/%d/%s/%d";
    private static final String ETL_OUTPUT_FILE_NAME_DESC_V1
            = "version.label.tableId.partitionId.indexId.bucket.schemaHash.parquet";
    // tableId.partitionId.indexId.bucket.schemaHash
    public static final String TABLET_META_FORMAT = "%d.%d.%d.%d.%d";
    public static final String ETL_OUTPUT_FILE_FORMAT = "parquet";

    // dpp result
    public static final String DPP_RESULT_NAME = "dpp_result.json";

    @SerializedName(value = "tables")
    public Map<Long, EtlTable> tables;
    @SerializedName(value = "outputPath")
    public String outputPath;
    @SerializedName(value = "outputFilePattern")
    public String outputFilePattern;
    @SerializedName(value = "label")
    public String label;
    @SerializedName(value = "properties")
    public EtlJobProperty properties;
    @SerializedName(value = "configVersion")
    public ConfigVersion configVersion;

    public EtlJobConfig(Map<Long, EtlTable> tables, String outputFilePattern, String label, EtlJobProperty properties) {
        this.tables = tables;
        // set outputPath when submit etl job
        this.outputPath = null;
        this.outputFilePattern = outputFilePattern;
        this.label = label;
        this.properties = properties;
        this.configVersion = ConfigVersion.V1;
    }

    @Override
    public String toString() {
        return "EtlJobConfig{"
                + "tables=" + tables
                + ", outputPath='" + outputPath + '\''
                + ", outputFilePattern='" + outputFilePattern + '\''
                + ", label='" + label + '\''
                + ", properties=" + properties
                + ", version=" + configVersion
                + '}';
    }

    public String getOutputPath() {
        return outputPath;
    }

    public static String getOutputPath(String hdfsEtlPath, long dbId, String loadLabel, long taskSignature) {
        return String.format(ETL_OUTPUT_PATH_FORMAT, hdfsEtlPath, dbId, loadLabel, taskSignature);
    }

    public static String getOutputFilePattern(String loadLabel, FilePatternVersion filePatternVersion) {
        return String.format("%s.%s.%s.%s", filePatternVersion.name(), loadLabel,
                TABLET_META_FORMAT, ETL_OUTPUT_FILE_FORMAT);
    }

    public static String getDppResultFilePath(String outputPath) {
        return outputPath + "/" + DPP_RESULT_NAME;
    }

    public static String getTabletMetaStr(String filePath) throws Exception {
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String[] fileNameArr = fileName.split("\\.");
        // check file version
        switch (FilePatternVersion.valueOf(fileNameArr[0])) {
            case V1:
                // version.label.tableId.partitionId.indexId.bucket.schemaHash.parquet
                if (fileNameArr.length != ETL_OUTPUT_FILE_NAME_DESC_V1.split("\\.").length) {
                    throw new Exception("etl output file name error, format: " + ETL_OUTPUT_FILE_NAME_DESC_V1
                                                + ", name: " + fileName);
                }
                long tableId = Long.parseLong(fileNameArr[2]);
                long partitionId = Long.parseLong(fileNameArr[3]);
                long indexId = Long.parseLong(fileNameArr[4]);
                int bucket = Integer.parseInt(fileNameArr[5]);
                int schemaHash = Integer.parseInt(fileNameArr[6]);
                // tableId.partitionId.indexId.bucket.schemaHash
                return String.format(TABLET_META_FORMAT, tableId, partitionId, indexId, bucket, schemaHash);
            default:
                throw new Exception("etl output file version error. version: " + fileNameArr[0]);
        }
    }

    public String configToJson() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.addDeserializationExclusionStrategy(new HiddenAnnotationExclusionStrategy());
        Gson gson = gsonBuilder.create();
        return gson.toJson(this);
    }

    public static EtlJobConfig configFromJson(String jsonConfig) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        return gson.fromJson(jsonConfig, EtlJobConfig.class);
    }

    public static class EtlJobProperty implements Serializable {
        @SerializedName(value = "strictMode")
        public boolean strictMode;
        @SerializedName(value = "timezone")
        public String timezone;

        @Override
        public String toString() {
            return "EtlJobProperty{"
                    + "strictMode=" + strictMode
                    + ", timezone='" + timezone + '\''
                    + '}';
        }
    }

    public enum ConfigVersion {
        V1
    }

    public enum FilePatternVersion {
        V1
    }

    public enum SourceType {
        FILE,
        HIVE
    }

    public static class EtlTable implements Serializable {
        @SerializedName(value = "indexes")
        public List<EtlIndex> indexes;
        @SerializedName(value = "partitionInfo")
        public EtlPartitionInfo partitionInfo;
        @SerializedName(value = "fileGroups")
        public List<EtlFileGroup> fileGroups;

        public EtlTable(List<EtlIndex> etlIndexes, EtlPartitionInfo etlPartitionInfo) {
            this.indexes = etlIndexes;
            this.partitionInfo = etlPartitionInfo;
            this.fileGroups = Lists.newArrayList();
        }

        public void addFileGroup(EtlFileGroup etlFileGroup) {
            fileGroups.add(etlFileGroup);
        }

        @Override
        public String toString() {
            return "EtlTable{"
                    + "indexes=" + indexes
                    + ", partitionInfo=" + partitionInfo
                    + ", fileGroups=" + fileGroups
                    + '}';
        }
    }

    public static class EtlColumn implements Serializable {
        @SerializedName(value = "columnName")
        public String columnName;
        @SerializedName(value = "columnType")
        public String columnType;
        @SerializedName(value = "isAllowNull")
        public boolean isAllowNull;
        @SerializedName(value = "isKey")
        public boolean isKey;
        @SerializedName(value = "aggregationType")
        public String aggregationType;
        @SerializedName(value = "defaultValue")
        public String defaultValue;
        @SerializedName(value = "stringLength")
        public int stringLength;
        @SerializedName(value = "precision")
        public int precision;
        @SerializedName(value = "scale")
        public int scale;
        @SerializedName(value = "defineExpr")
        public String defineExpr;

        // for unit test
        public EtlColumn() { }

        public EtlColumn(String columnName, String columnType, boolean isAllowNull, boolean isKey,
                         String aggregationType, String defaultValue, int stringLength, int precision, int scale) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.isAllowNull = isAllowNull;
            this.isKey = isKey;
            this.aggregationType = aggregationType;
            this.defaultValue = defaultValue;
            this.stringLength = stringLength;
            this.precision = precision;
            this.scale = scale;
            this.defineExpr = null;
        }

        @Override
        public String toString() {
            return "EtlColumn{"
                    + "columnName='" + columnName + '\''
                    + ", columnType='" + columnType + '\''
                    + ", isAllowNull=" + isAllowNull
                    + ", isKey=" + isKey
                    + ", aggregationType='" + aggregationType + '\''
                    + ", defaultValue='" + defaultValue + '\''
                    + ", stringLength=" + stringLength
                    + ", precision=" + precision
                    + ", scale=" + scale
                    + ", defineExpr='" + defineExpr + '\''
                    + '}';
        }
    }

    public static class EtlIndexComparator implements Comparator<EtlIndex> {
        @Override
        public int compare(EtlIndex a, EtlIndex b) {
            int diff = a.columns.size() - b.columns.size();
            if (diff == 0) {
                return 0;
            } else if (diff > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public static class EtlIndex implements Serializable {
        @SerializedName(value = "indexId")
        public long indexId;
        @SerializedName(value = "columns")
        public List<EtlColumn> columns;
        @SerializedName(value = "schemaHash")
        public int schemaHash;
        @SerializedName(value = "indexType")
        public String indexType;
        @SerializedName(value = "isBaseIndex")
        public boolean isBaseIndex;

        public EtlIndex(long indexId, List<EtlColumn> etlColumns, int schemaHash,
                        String indexType, boolean isBaseIndex) {
            this.indexId = indexId;
            this.columns =  etlColumns;
            this.schemaHash = schemaHash;
            this.indexType = indexType;
            this.isBaseIndex = isBaseIndex;
        }

        public EtlColumn getColumn(String name) {
            for (EtlColumn column : columns) {
                if (column.columnName.equals(name)) {
                    return column;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "EtlIndex{"
                    + "indexId=" + indexId
                    + ", columns=" + columns
                    + ", schemaHash=" + schemaHash
                    + ", indexType='" + indexType + '\''
                    + ", isBaseIndex=" + isBaseIndex
                    + '}';
        }
    }

    public static class EtlPartitionInfo implements Serializable {
        @SerializedName(value = "partitionType")
        public String partitionType;
        @SerializedName(value = "partitionColumnRefs")
        public List<String> partitionColumnRefs;
        @SerializedName(value = "distributionColumnRefs")
        public List<String> distributionColumnRefs;
        @SerializedName(value = "partitions")
        public List<EtlPartition> partitions;

        public EtlPartitionInfo(String partitionType, List<String> partitionColumnRefs,
                                List<String> distributionColumnRefs, List<EtlPartition> etlPartitions) {
            this.partitionType = partitionType;
            this.partitionColumnRefs = partitionColumnRefs;
            this.distributionColumnRefs = distributionColumnRefs;
            this.partitions = etlPartitions;
        }

        @Override
        public String toString() {
            return "EtlPartitionInfo{"
                    + "partitionType='" + partitionType + '\''
                    + ", partitionColumnRefs=" + partitionColumnRefs
                    + ", distributionColumnRefs=" + distributionColumnRefs
                    + ", partitions=" + partitions
                    + '}';
        }
    }

    public static class EtlPartition implements Serializable {
        @SerializedName(value = "partitionId")
        public long partitionId;
        @SerializedName(value = "startKeys")
        public List<Object> startKeys;
        @SerializedName(value = "endKeys")
        public List<Object> endKeys;
        @SerializedName(value = "isMaxPartition")
        public boolean isMaxPartition;
        @SerializedName(value = "bucketNum")
        public int bucketNum;

        public EtlPartition(long partitionId, List<Object> startKeys, List<Object> endKeys,
                            boolean isMaxPartition, int bucketNum) {
            this.partitionId = partitionId;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.isMaxPartition = isMaxPartition;
            this.bucketNum = bucketNum;
        }

        @Override
        public String toString() {
            return "EtlPartition{"
                    + "partitionId=" + partitionId
                    + ", startKeys=" + startKeys
                    + ", endKeys=" + endKeys
                    + ", isMaxPartition=" + isMaxPartition
                    + ", bucketNum=" + bucketNum
                    + '}';
        }
    }

    public static class EtlFileGroup implements Serializable {
        @SerializedName(value = "sourceType")
        public SourceType sourceType = SourceType.FILE;
        @SerializedName(value = "filePaths")
        public List<String> filePaths;
        @SerializedName(value = "fileFieldNames")
        public List<String> fileFieldNames;
        @SerializedName(value = "columnsFromPath")
        public List<String> columnsFromPath;
        @SerializedName(value = "columnSeparator")
        public String columnSeparator;
        @SerializedName(value = "lineDelimiter")
        public String lineDelimiter;
        @SerializedName(value = "isNegative")
        public boolean isNegative;
        @SerializedName(value = "fileFormat")
        public String fileFormat;
        @SerializedName(value = "columnMappings")
        public Map<String, EtlColumnMapping> columnMappings;
        @SerializedName(value = "where")
        public String where;
        @SerializedName(value = "partitions")
        public List<Long> partitions;
        @SerializedName(value = "hiveDbTableName")
        public String hiveDbTableName;
        @SerializedName(value = "hiveTableProperties")
        public Map<String, String> hiveTableProperties;

        // hive db table used in dpp, not serialized
        // set with hiveDbTableName (no bitmap column) or IntermediateHiveTable (created by global dict builder)
        // in spark etl job
        public String dppHiveDbTableName;

        // for data infile path
        public EtlFileGroup(SourceType sourceType, List<String> filePaths, List<String> fileFieldNames,
                            List<String> columnsFromPath, String columnSeparator, String lineDelimiter,
                            boolean isNegative, String fileFormat, Map<String, EtlColumnMapping> columnMappings,
                            String where, List<Long> partitions) {
            this.sourceType = sourceType;
            this.filePaths = filePaths;
            this.fileFieldNames = fileFieldNames;
            this.columnsFromPath = columnsFromPath;
            this.columnSeparator = Strings.isNullOrEmpty(columnSeparator) ? "\t" : columnSeparator;
            this.lineDelimiter = lineDelimiter;
            this.isNegative = isNegative;
            this.fileFormat = fileFormat;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        // for data from table
        public EtlFileGroup(SourceType sourceType, String hiveDbTableName, Map<String, String> hiveTableProperties,
                            boolean isNegative, Map<String, EtlColumnMapping> columnMappings,
                            String where, List<Long> partitions) {
            this.sourceType = sourceType;
            this.hiveDbTableName = hiveDbTableName;
            this.hiveTableProperties = hiveTableProperties;
            this.isNegative = isNegative;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        @Override
        public String toString() {
            return "EtlFileGroup{"
                    + "sourceType=" + sourceType
                    + ", filePaths=" + filePaths
                    + ", fileFieldNames=" + fileFieldNames
                    + ", columnsFromPath=" + columnsFromPath
                    + ", columnSeparator='" + columnSeparator + '\''
                    + ", lineDelimiter='" + lineDelimiter + '\''
                    + ", isNegative=" + isNegative
                    + ", fileFormat='" + fileFormat + '\''
                    + ", columnMappings=" + columnMappings
                    + ", where='" + where + '\''
                    + ", partitions=" + partitions
                    + ", hiveDbTableName='" + hiveDbTableName + '\''
                    + ", hiveTableProperties=" + hiveTableProperties
                    + '}';
        }
    }

    /**
     * FunctionCallExpr = functionName(args)
     * For compatibility with old designed functions used in Hadoop MapReduce etl
     *
     * expr is more general, like k1 + 1, not just FunctionCall
     */
    public static class EtlColumnMapping implements Serializable {
        @SerializedName(value = "functionName")
        public String functionName;
        @SerializedName(value = "args")
        public List<String> args;
        @SerializedName(value = "expr")
        public String expr;

        private static Map<String, String> functionMap = new ImmutableMap.Builder<String, String>()
                .put("md5sum", "md5").build();

        public EtlColumnMapping(String functionName, List<String> args) {
            this.functionName = functionName;
            this.args = args;
        }

        public EtlColumnMapping(String expr) {
            this.expr = expr;
        }

        public String toDescription() {
            StringBuilder sb = new StringBuilder();
            if (functionName == null) {
                sb.append(expr);
            } else {
                if (functionMap.containsKey(functionName)) {
                    sb.append(functionMap.get(functionName));
                } else {
                    sb.append(functionName);
                }
                sb.append("(");
                if (args != null) {
                    for (String arg : args) {
                        sb.append(arg);
                        sb.append(",");
                    }
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return "EtlColumnMapping{"
                    + "functionName='" + functionName + '\''
                    + ", args=" + args
                    + ", expr=" + expr
                    + '}';
        }
    }

    public static class HiddenAnnotationExclusionStrategy implements ExclusionStrategy {
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(SerializedName.class) == null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }
}
