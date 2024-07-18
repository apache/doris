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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.info.SimpleTableInfo;

import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;
import java.util.Map;

@Data
public class HivePartition {
    public static final String LAST_MODIFY_TIME_KEY = "transient_lastDdlTime";
    public static final String FILE_NUM_KEY = "numFiles";

    private SimpleTableInfo tableInfo;
    private String inputFormat;
    private String path;
    private List<String> partitionValues;
    private boolean isDummyPartition;
    private Map<String, String> parameters;
    private String outputFormat;
    private String serde;
    private List<FieldSchema> columns;

    // If you want to read the data under a partition, you can use this constructor
    public HivePartition(SimpleTableInfo tableInfo, boolean isDummyPartition,
            String inputFormat, String path, List<String> partitionValues, Map<String, String> parameters) {
        this.tableInfo = tableInfo;
        this.isDummyPartition = isDummyPartition;
        // eg: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
        this.inputFormat = inputFormat;
        // eg: hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=beijing
        this.path = path;
        // eg: cn, beijing
        this.partitionValues = partitionValues;
        this.parameters = parameters;
    }

    public HivePartition(String database, String tableName, boolean isDummyPartition,
            String inputFormat, String path, List<String> partitionValues, Map<String, String> parameters) {
        this(new SimpleTableInfo(database, tableName), isDummyPartition, inputFormat, path, partitionValues,
                parameters);
    }

    // If you want to update hms with partition, then you can use this constructor,
    // as updating hms requires some additional information, such as outputFormat and so on
    public HivePartition(SimpleTableInfo tableInfo, boolean isDummyPartition,
            String inputFormat, String path, List<String> partitionValues, Map<String, String> parameters,
            String outputFormat, String serde, List<FieldSchema> columns) {
        this(tableInfo, isDummyPartition, inputFormat, path, partitionValues, parameters);
        this.outputFormat = outputFormat;
        this.serde = serde;
        this.columns = columns;
    }

    public String getDbName() {
        return tableInfo.getDbName();
    }

    public String getTblName() {
        return tableInfo.getTbName();
    }

    // return partition name like: nation=cn/city=beijing
    public String getPartitionName(List<Column> partColumns) {
        Preconditions.checkState(partColumns.size() == partitionValues.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partColumns.size(); ++i) {
            if (i != 0) {
                sb.append("/");
            }
            sb.append(partColumns.get(i).getName()).append("=").append(partitionValues.get(i));
        }
        return sb.toString();
    }

    public boolean isDummyPartition() {
        return this.isDummyPartition;
    }

    public long getLastModifiedTime() {
        if (parameters == null || !parameters.containsKey(LAST_MODIFY_TIME_KEY)) {
            return 0L;
        }
        return Long.parseLong(parameters.get(LAST_MODIFY_TIME_KEY)) * 1000;
    }

    /**
     * If there are no files, it proves that there is no data under the partition, we return 0
     *
     * @return
     */
    public long getLastModifiedTimeIgnoreInit() {
        if (getFileNum() == 0) {
            return 0L;
        }
        return getLastModifiedTime();
    }

    public long getFileNum() {
        if (parameters == null || !parameters.containsKey(FILE_NUM_KEY)) {
            return 0L;
        }
        return Long.parseLong(parameters.get(FILE_NUM_KEY));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HivePartition{");
        sb.append("tableInfo=").append(tableInfo);
        sb.append(", inputFormat='").append(inputFormat).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", partitionValues=").append(partitionValues);
        sb.append(", isDummyPartition=").append(isDummyPartition);
        sb.append(", parameters=").append(parameters);
        sb.append(", outputFormat='").append(outputFormat).append('\'');
        sb.append(", serde='").append(serde).append('\'');
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }
}
