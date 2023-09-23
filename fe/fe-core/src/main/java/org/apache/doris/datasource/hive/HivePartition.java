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

import com.google.common.base.Preconditions;
import lombok.Data;

import java.util.List;

@Data
public class HivePartition {
    private String dbName;
    private String tblName;
    private String inputFormat;
    private String path;
    private List<String> partitionValues;
    private boolean isDummyPartition;

    public HivePartition(String dbName, String tblName, boolean isDummyPartition,
                         String inputFormat, String path, List<String> partitionValues) {
        this.dbName = dbName;
        this.tblName = tblName;
        this.isDummyPartition = isDummyPartition;
        // eg: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
        this.inputFormat = inputFormat;
        // eg: hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=beijing
        this.path = path;
        // eg: cn, beijing
        this.partitionValues = partitionValues;
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

    @Override
    public String toString() {
        return "HivePartition{"
                + "dbName='" + dbName + '\''
                + ", tblName='" + tblName + '\''
                + ", isDummyPartition='" + isDummyPartition + '\''
                + ", inputFormat='" + inputFormat + '\''
                + ", path='" + path + '\''
                + ", partitionValues=" + partitionValues + '}';
    }
}
