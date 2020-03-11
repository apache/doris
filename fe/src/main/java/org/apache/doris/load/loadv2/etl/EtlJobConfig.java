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
 * 			"columns": {
 * 				"k1": {
 * 					"default_value": "\\N",
 * 					"column_type": "DATETIME",
 * 					"is_allow_null": true
 *              },
 * 				"k2": {
 * 					"default_value": "0",
 * 					"column_type": "SMALLINT",
 * 					"is_allow_null": true
 *              },
 * 				"v": {
 * 					"default_value": "0",
 * 					"column_type": "BIGINT",
 * 					"is_allow_null": false
 *              }
 *          },
 * 			"indexes": {
 * 				10014: {
 * 					"column_refs": [{
 * 						"name": "k1",
 * 						"is_key": true,
 * 						"aggregation_type": "NONE"
 *                    }, {
 * 						"name": "k2",
 * 						"is_key": true,
 * 						"aggregation_type": "NONE"
 *                    }, {
 * 						"name": "v",
 * 						"is_key": false,
 * 						"aggregation_type": "NONE"
 *                    }],
 *                  "distribution_column_refs": ["k1"],
 * 					"schema_hash": 1294206574
 *              },
 * 				10017: {
 * 					"column_refs": [{
 * 						"name": "k1",
 * 						"is_key": true,
 * 						"aggregation_type": "NONE"
 *                    }, {
 * 						"name": "v",
 * 						"is_key": false,
 * 						"aggregation_type": "SUM"
 *                    }],
 *                  "distribution_column_refs": ["k1"],
 * 					"schema_hash": 1294206575
 *                }
 *           },
 * 			"partition_info": {
 * 				"partition_type": "RANGE",
 * 				"partition_column_refs": ["k2"],
 * 				"partitions": {
 * 					10020: {
 * 						"start_keys": [-100],
 * 						"end_keys": [10],
 * 						"is_max_partition": false,
 * 						"bucket_num": 3
 *                  }
 *              }
 *          },
 * 			"file_groups": [{
 * 		        "partitions": [10020],
 * 				"file_paths": ["hdfs://hdfs_host:port/user/palo/test/file"],
 * 				"file_field_names": ["tmp_k1", "k2"],
 * 				"value_separator": ",",
 * 			    "line_delimiter": "\n"
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
 * 	"output_file_pattern": "label1.%(table_id)d.%(partition_id)d.%(index_id)d.%(bucket)d.%(schema_hash)d"
 * }
 */
public class EtlJobConfig {
    public static final String GLOBAL_DICT_TABLE_NAME = "doris_global_dict_table_%d";
    public static final String DISTINCT_KEY_TABLE_NAME = "doris_distinct_key_table_%d_%s";
    public static final String DORIS_INTERMEDIATE_HIVE_TABLE_NAME = "doris_intermediate_hive_table_%d_%s";

    Map<Long, EtlTable> tables;
    String outputPath;
    String outputFilePattern;
    // private EtlErrorHubInfo hubInfo;

    public EtlJobConfig(Map<Long, EtlTable> tables, String outputFilePattern) {
        this.tables = tables;
        // set outputPath when submit etl job
        this.outputPath = null;
        this.outputFilePattern = outputFilePattern;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public static class EtlTable {
        Map<String, EtlColumn> columns;
        Map<Long, EtlIndex> indexes;
        EtlPartitionInfo partitionInfo;
        List<EtlFileGroup> fileGroups;

        public EtlTable(Map<String, EtlColumn> nameToEtlColumn, Map<Long, EtlIndex> idToEtlIndex,
                        EtlPartitionInfo etlPartitionInfo) {
            this.columns = nameToEtlColumn;
            this.indexes = idToEtlIndex;
            this.partitionInfo = etlPartitionInfo;
            this.fileGroups = Lists.newArrayList();
        }

        public void addFileGroup(EtlFileGroup etlFileGroup) {
            fileGroups.add(etlFileGroup);
        }
    }

    public static class EtlColumn {
        String name;
        String columnType;
        boolean isAllowNull;
        boolean isKey;
        String aggregationType;
        String defaultValue;
        int stringLength;
        int precision;
        int scale;

        public EtlColumn(String name, String columnType, boolean isAllowNull, boolean isKey, String aggregationType,
                         String defaultValue, int stringLength, int precision, int scale) {
            this.name = name;
            this.columnType = columnType;
            this.isAllowNull = isAllowNull;
            this.isKey = isKey;
            this.aggregationType = aggregationType;
            this.defaultValue = defaultValue;
            this.stringLength = stringLength;
            this.precision = precision;
            this.scale = scale;
        }
    }

    public static class EtlIndex {
        List<EtlColumn> columnRefs;
        List<String> distributionColumnRefs;
        int schemaHash;

        public EtlIndex(List<EtlColumn> columnMaps, List<String> distributionColumnRefs, int schemaHash) {
            this.columnRefs = columnMaps;
            this.distributionColumnRefs = distributionColumnRefs;
            this.schemaHash = schemaHash;
        }
    }

    public static class EtlPartitionInfo {
        String partitionType;
        List<String> partitionColumnRefs;
        Map<Long, EtlPartition> partitions;

        public EtlPartitionInfo(String partitionType, List<String> partitionColumnRefs,
                                Map<Long, EtlPartition> idToEtlPartition) {
            this.partitionType = partitionType;
            this.partitionColumnRefs = partitionColumnRefs;
            this.partitions = idToEtlPartition;
        }
    }

    public static class EtlPartition {
        List<Object> startKeys;
        List<Object> endKeys;
        boolean isMaxPartition;
        int bucketNum;

        public EtlPartition(List<Object> startKeys, List<Object> endKeys, boolean isMaxPartition, int bucketNum) {
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.isMaxPartition = isMaxPartition;
            this.bucketNum = bucketNum;
        }
    }

    public static class EtlFileGroup {
        List<String> filePaths;
        List<String> fileFieldNames;
        List<String> columnsFromPath;
        String valueSeparator;
        String lineDelimiter;
        boolean isNegative;
        String fileFormat;
        Map<String, EtlColumnMapping> columnMappings;
        String where;
        List<Long> partitions;

        String hiveTableName;

        public EtlFileGroup(List<String> filePaths, List<String> fileFieldNames, List<String> columnsFromPath,
                            String valueSeparator, String lineDelimiter, boolean isNegative, String fileFormat,
                            Map<String, EtlColumnMapping> columnMappings, String where, List<Long> partitions) {
            this.filePaths = filePaths;
            this.fileFieldNames = fileFieldNames;
            this.columnsFromPath = columnsFromPath;
            this.valueSeparator = valueSeparator;
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
    }

    public static class EtlColumnMapping {
        String functionName;
        List<String> args;

        public EtlColumnMapping(String functionName, List<String> args) {
            this.functionName = functionName;
            this.args = args;
        }
    }
}
