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

package org.apache.doris.load.loadv2.etl;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/** jobconfig.json file format
 * {
 * 	"tables": {
 * 		10014: {
 * 			"indexes": [{
 * 			    "index_id": 10014,
 * 				"columns": [{
 * 				    "column_name": "k1",
 * 				    "column_type": "SMALLINT",
 * 				    "is_key": true,
 * 				    "is_allow_null": true,
 * 				    "aggregation_type": "NONE"
 *                }, {
 * 					"column_name": "k2",
 * 				    "column_type": "VARCHAR",
 * 				    "string_length": 20,
 * 					"is_key": true,
 * 				    "is_allow_null": true,
 * 					"aggregation_type": "NONE"
 *                }, {
 * 					"column_name": "v",
 * 				    "column_type": "BIGINT",
 * 					"is_key": false,
 * 				    "is_allow_null": false,
 * 					"aggregation_type": "NONE"
 *              }],
 * 				"schema_hash": 1294206574,
 * 			    "index_type": "DUPLICATE",
 * 			    "is_base_index": true
 *            }, {
 * 			    "index_id": 10017,
 * 				"columns": [{
 * 				    "column_name": "k1",
 * 				    "column_type": "SMALLINT",
 * 					"is_key": true,
 * 				    "is_allow_null": true,
 * 					"aggregation_type": "NONE"
 *                }, {
 * 					"column_name": "v",
 * 				    "column_type": "BIGINT",
 * 					"is_key": false,
 * 				    "is_allow_null": false,
 * 					"aggregation_type": "SUM"
 *              }],
 * 				"schema_hash": 1294206575,
 * 			    "index_type": "AGGREGATE",
 * 			    "is_base_index": false
 *          }],
 * 			"partition_info": {
 * 				"partition_type": "RANGE",
 * 				"partition_column_refs": ["k1"],
 *              "distribution_column_refs": ["k2"],
 * 				"partitions": [{
 * 				    "partition_id": 10020,
 * 					"start_keys": [-100],
 * 					"end_keys": [10],
 * 					"is_max_partition": false,
 * 					"bucket_num": 3
 *                }, {
 *                  "partition_id": 10021,
 *                  "start_keys": [10],
 *                  "end_keys": [100],
 *                  "is_max_partition": false,
 *  				"bucket_num": 3
 *              }]
 *          },
 * 			"file_groups": [{
 * 		        "partitions": [10020],
 * 				"file_paths": ["hdfs://hdfs_host:port/user/palo/test/file"],
 * 				"file_field_names": ["tmp_k1", "k2"],
 * 				"value_separator": ",",
 * 			    "line_delimiter": "\n",
 * 				"column_mappings": {
 * 					"k1": {
 * 						"function_name": "strftime",
 * 						"args": ["%Y-%m-%d %H:%M:%S", "tmp_k1"]
 *                   }
 *              },
 * 				"where": "k2 > 10",
 * 				"is_negative": false,
 * 				"hive_table_name": "hive_db.table"
 *          }]
 *      }
 *  },
 * 	"output_path": "hdfs://hdfs_host:port/user/output/10003/label1/1582599203397",
 * 	"output_file_pattern": "label1.%d.%d.%d.%d.%d",
 * 	"label": "label0"
 * }
 */
public class EtlJobConfig {
    // global dict
    public static final String GLOBAL_DICT_TABLE_NAME = "doris_global_dict_table_%d";
    public static final String DISTINCT_KEY_TABLE_NAME = "doris_distinct_key_table_%d_%s";
    public static final String DORIS_INTERMEDIATE_HIVE_TABLE_NAME = "doris_intermediate_hive_table_%d_%s";

    // hdfs://host:port/outputPath/dbId/loadLabel/PendingTaskSignature
    private static final String ETL_OUTPUT_PATH_FORMAT = "%s%s/%d/%s/%d";
    private static final String ETL_OUTPUT_FILE_NAME_DESC = "label.tableId.partitionId.indexId.bucket.schemaHash";
    public static final String ETL_OUTPUT_FILE_NAME_NO_LABEL_FORMAT = "%d.%d.%d.%d.%d";

    // config
    public static final String JOB_CONFIG_FILE_NAME = "jobconfig.json";

    Map<Long, EtlTable> tables;
    String outputPath;
    String outputFilePattern;
    String label;
    // private EtlErrorHubInfo hubInfo;

    public EtlJobConfig(Map<Long, EtlTable> tables, String outputFilePattern, String label) {
        this.tables = tables;
        // set outputPath when submit etl job
        this.outputPath = null;
        this.outputFilePattern = outputFilePattern;
        this.label = label;
    }

    @Override
    public String toString() {
        return "EtlJobConfig{" +
                "tables=" + tables +
                ", outputPath='" + outputPath + '\'' +
                ", outputFilePattern='" + outputFilePattern + '\'' +
                ", label='" + label + '\'' +
                '}';
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public static String getOutputPath(String hdfsDefaultName, String outputPath, long dbId,
                                       String loadLabel, long taskSignature) {
        return String.format(ETL_OUTPUT_PATH_FORMAT, hdfsDefaultName, outputPath, dbId, loadLabel, taskSignature);
    }

    public static String getTablePartitionIndexBucketSchemaStr(String filePath) throws Exception {
        // label.tableId.partitionId.indexId.bucket.schemaHash
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String[] fileNameArr = fileName.split("\\.");
        if (fileNameArr.length != ETL_OUTPUT_FILE_NAME_DESC.split("\\.").length) {
            throw new Exception("etl output file name error, format: " + ETL_OUTPUT_FILE_NAME_DESC
                                        + ", name: " + fileName);
        }

        // tableId.partitionId.indexId.bucket.schemaHash
        return fileName.substring(fileName.indexOf(".") + 1);
    }

    public static class EtlTable {
        List<EtlIndex> indexes;
        EtlPartitionInfo partitionInfo;
        List<EtlFileGroup> fileGroups;

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
            return "EtlTable{" +
                    "indexes=" + indexes +
                    ", partitionInfo=" + partitionInfo +
                    ", fileGroups=" + fileGroups +
                    '}';
        }
    }

    public static class EtlColumn {
        String columnName;
        String columnType;
        boolean isAllowNull;
        boolean isKey;
        String aggregationType;
        String defaultValue;
        int stringLength;
        int precision;
        int scale;

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
        }

        @Override
        public String toString() {
            return "EtlColumn{" +
                    "columnName='" + columnName + '\'' +
                    ", columnType='" + columnType + '\'' +
                    ", isAllowNull=" + isAllowNull +
                    ", isKey=" + isKey +
                    ", aggregationType='" + aggregationType + '\'' +
                    ", defaultValue='" + defaultValue + '\'' +
                    ", stringLength=" + stringLength +
                    ", precision=" + precision +
                    ", scale=" + scale +
                    '}';
        }
    }

    public static class EtlIndex {
        long indexId;
        List<EtlColumn> columns;
        int schemaHash;
        String indexType;
        boolean isBaseIndex;

        public EtlIndex(long indexId, List<EtlColumn> etlColumns, int schemaHash,
                        String indexType, boolean isBaseIndex) {
            this.indexId = indexId;
            this.columns =  etlColumns;
            this.schemaHash = schemaHash;
            this.indexType = indexType;
            this.isBaseIndex = isBaseIndex;
        }

        @Override
        public String toString() {
            return "EtlIndex{" +
                    "indexId=" + indexId +
                    ", columns=" + columns +
                    ", schemaHash=" + schemaHash +
                    ", indexType='" + indexType + '\'' +
                    ", isBaseIndex=" + isBaseIndex +
                    '}';
        }
    }

    public static class EtlPartitionInfo {
        String partitionType;
        List<String> partitionColumnRefs;
        List<String> distributionColumnRefs;
        List<EtlPartition> partitions;

        public EtlPartitionInfo(String partitionType, List<String> partitionColumnRefs,
                                List<String> distributionColumnRefs, List<EtlPartition> etlPartitions) {
            this.partitionType = partitionType;
            this.partitionColumnRefs = partitionColumnRefs;
            this.distributionColumnRefs = distributionColumnRefs;
            this.partitions = etlPartitions;
        }

        @Override
        public String toString() {
            return "EtlPartitionInfo{" +
                    "partitionType='" + partitionType + '\'' +
                    ", partitionColumnRefs=" + partitionColumnRefs +
                    ", distributionColumnRefs=" + distributionColumnRefs +
                    ", partitions=" + partitions +
                    '}';
        }
    }

    public static class EtlPartition {
        long partitionId;
        List<Object> startKeys;
        List<Object> endKeys;
        boolean isMaxPartition;
        int bucketNum;

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
            return "EtlPartition{" +
                    "partitionId=" + partitionId +
                    ", startKeys=" + startKeys +
                    ", endKeys=" + endKeys +
                    ", isMaxPartition=" + isMaxPartition +
                    ", bucketNum=" + bucketNum +
                    '}';
        }
    }

    public static class EtlFileGroup {
        List<String> filePaths;
        List<String> fileFieldNames;
        List<String> columnsFromPath;
        String columnSeparator;
        String lineDelimiter;
        boolean isNegative;
        String fileFormat;
        Map<String, EtlColumnMapping> columnMappings;
        String where;
        List<Long> partitions;

        String hiveTableName;

        public EtlFileGroup(List<String> filePaths, List<String> fileFieldNames, List<String> columnsFromPath,
                            String columnSeparator, String lineDelimiter, boolean isNegative, String fileFormat,
                            Map<String, EtlColumnMapping> columnMappings, String where, List<Long> partitions) {
            this.filePaths = filePaths;
            this.fileFieldNames = fileFieldNames;
            this.columnsFromPath = columnsFromPath;
            this.columnSeparator = columnSeparator;
            this.lineDelimiter = lineDelimiter;
            this.isNegative = isNegative;
            this.fileFormat = fileFormat;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        public void setHiveTableName(String hiveTableName) {
            this.hiveTableName = hiveTableName;
        }

        @Override
        public String toString() {
            return "EtlFileGroup{" +
                    "filePaths=" + filePaths +
                    ", fileFieldNames=" + fileFieldNames +
                    ", columnsFromPath=" + columnsFromPath +
                    ", columnSeparator='" + columnSeparator + '\'' +
                    ", lineDelimiter='" + lineDelimiter + '\'' +
                    ", isNegative=" + isNegative +
                    ", fileFormat='" + fileFormat + '\'' +
                    ", columnMappings=" + columnMappings +
                    ", where='" + where + '\'' +
                    ", partitions=" + partitions +
                    ", hiveTableName='" + hiveTableName + '\'' +
                    '}';
        }
    }

    public static class EtlColumnMapping {
        String functionName;
        List<String> args;

        public EtlColumnMapping(String functionName, List<String> args) {
            this.functionName = functionName;
            this.args = args;
        }

        @Override
        public String toString() {
            return "EtlColumnMapping{" +
                    "functionName='" + functionName + '\'' +
                    ", args=" + args +
                    '}';
        }
    }
}
