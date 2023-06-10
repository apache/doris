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

package org.apache.doris.hudi;

import org.apache.doris.jni.vec.ColumnType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * The hudi scan param
 */
public class HudiScanParam {
    public static String HADOOP_FS_PREFIX = "hadoop_fs.";

    private final int fetchSize;
    private final String basePath;
    private final String dataFilePath;
    private final long dataFileLength;
    private final String[] deltaFilePaths;
    private final String hudiColumnNames;
    private final String[] hudiColumnTypes;
    private final String[] requiredFields;
    private int[] requiredColumnIds;
    private ColumnType[] requiredTypes;
    private final String[] nestedFields;
    private final String instantTime;
    private final String serde;
    private final String inputFormat;
    private final ObjectInspector[] fieldInspectors;
    private final StructField[] structFields;
    private final Map<String, String> hadoopConf;

    public HudiScanParam(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.basePath = params.get("base_path");
        this.dataFilePath = params.get("data_file_path");
        this.dataFileLength = Long.parseLong(params.get("data_file_length"));
        String deltaFilePaths = params.get("delta_file_paths");

        if (StringUtils.isEmpty(deltaFilePaths)) {
            this.deltaFilePaths = new String[0];
        } else {
            this.deltaFilePaths = deltaFilePaths.split(",");
        }

        this.hudiColumnNames = params.get("hudi_column_names");
        this.hudiColumnTypes = params.get("hudi_column_types").split("#");
        this.requiredFields = params.get("required_fields").split(",");
        this.nestedFields = params.getOrDefault("nested_fields", "").split(",");
        this.instantTime = params.get("instant_time");
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];

        hadoopConf = new HashMap<>();
        for (Map.Entry<String, String> kv : params.entrySet()) {
            if (kv.getKey().startsWith(HADOOP_FS_PREFIX)) {
                hadoopConf.put(kv.getKey().substring(HADOOP_FS_PREFIX.length()), kv.getValue());
            }
        }

        parseRequiredColumns();
    }

    private void parseRequiredColumns() {
        String[] hiveColumnNames = this.hudiColumnNames.split(",");
        Map<String, Integer> hiveColumnNameToIndex = new HashMap<>();
        Map<String, String> hiveColumnNameToType = new HashMap<>();
        for (int i = 0; i < hiveColumnNames.length; i++) {
            hiveColumnNameToIndex.put(hiveColumnNames[i], i);
            hiveColumnNameToType.put(hiveColumnNames[i], this.hudiColumnTypes[i]);
        }
        requiredTypes = new ColumnType[requiredFields.length];
        requiredColumnIds = new int[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            requiredColumnIds[i] = hiveColumnNameToIndex.get(requiredFields[i]);
            String type = hiveColumnNameToType.get(requiredFields[i]);
            requiredTypes[i] = ColumnType.parseType(requiredFields[i], type);
        }
    }

    public Properties createProperties() {
        Properties properties = new Properties();

        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
                Arrays.stream(requiredColumnIds).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, String.join(",", requiredFields));
        properties.setProperty(HudiScanUtils.COLUMNS, hudiColumnNames);
        // recover INT64 based timestamp mark to hive type, timestamp(3)/timestamp(6) => timestamp
        properties.setProperty(HudiScanUtils.COLUMNS_TYPES,
                Arrays.stream(hudiColumnTypes).map(type -> type.startsWith("timestamp(") ? "timestamp" : type).collect(
                        Collectors.joining(",")));
        properties.setProperty(serdeConstants.SERIALIZATION_LIB, serde);

        for (Map.Entry<String, String> kv : hadoopConf.entrySet()) {
            properties.setProperty(kv.getKey(), kv.getValue());
        }

        return properties;
    }


    public int getFetchSize() {
        return fetchSize;
    }

    public String getBasePath() {
        return basePath;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    public long getDataFileLength() {
        return dataFileLength;
    }

    public String[] getDeltaFilePaths() {
        return deltaFilePaths;
    }

    public String getHudiColumnNames() {
        return hudiColumnNames;
    }

    public String[] getHudiColumnTypes() {
        return hudiColumnTypes;
    }

    public String[] getRequiredFields() {
        return requiredFields;
    }

    public int[] getRequiredColumnIds() {
        return requiredColumnIds;
    }

    public ColumnType[] getRequiredTypes() {
        return requiredTypes;
    }

    public String[] getNestedFields() {
        return nestedFields;
    }

    public String getInstantTime() {
        return instantTime;
    }

    public String getSerde() {
        return serde;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public ObjectInspector[] getFieldInspectors() {
        return fieldInspectors;
    }

    public StructField[] getStructFields() {
        return structFields;
    }

}
