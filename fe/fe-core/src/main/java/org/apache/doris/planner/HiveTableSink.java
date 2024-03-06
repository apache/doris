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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataSink.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.THiveBucket;
import org.apache.doris.thrift.THiveCompressionType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartition;
import org.apache.doris.thrift.THiveTableSink;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.ArrayList;
import java.util.List;

public class HiveTableSink extends DataSink {

    private HMSExternalTable targetTable;
    protected TDataSink tDataSink;

    public HiveTableSink(HMSExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "HIVE TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        // TODO: explain partitions
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    public void init(List<Column> cols, List<Long> partitionIds) throws AnalysisException {
        THiveTableSink tSink = new THiveTableSink();
        tSink.setDbName(targetTable.getDbName());
        tSink.setTableName(targetTable.getName());
        List<String> dataCols = new ArrayList<>();
        for (Column col : cols) {
            dataCols.add(col.getName());
        }
        tSink.setDataColumnNames(dataCols);
        List<String> partitionCols = new ArrayList<>();
        List<THivePartition> partitions = new ArrayList<>();
        for (Long partitionId : partitionIds) {
            String partName = targetTable.getPartitionName(partitionId);
            if (StringUtils.isNotEmpty(partName)) {
                partitionCols.add(partName);
                THivePartition hivePartition = new THivePartition();
                // TODO: use partition format type itself.
                hivePartition.setFileFormat(getFileFormatType());
                hivePartition.setValues(new ArrayList<>(targetTable.getPartitionNames()));
                // TODO: set partition location: hivePartition.setLocation();
                partitions.add(hivePartition);
            }
        }
        tSink.setPartitionColumnNames(partitionCols);
        tSink.setPartitions(partitions);

        List<THiveBucket> buckets  = new ArrayList<>();
        // TODO: set buckets
        tSink.setBuckets(buckets);
        tSink.setFileFormat(getFileFormatType());

        tSink.setCompressionType(THiveCompressionType.SNAPPY);

        StorageDescriptor sd = targetTable.getRemoteTable().getSd();
        THiveLocationParams locationParams = new THiveLocationParams();
        locationParams.setWritePath(sd.getLocation());
        locationParams.setTargetPath(sd.getLocation());
        tSink.setLocation(locationParams);

        tDataSink = new TDataSink(getDataSinkType());
        tDataSink.setHiveTableSink(tSink);
    }

    private TFileFormatType getFileFormatType() {
        // TODO: use simple format here
        TFileFormatType fileFormatType;
        if (targetTable.getRemoteTable().getSd().getInputFormat().toLowerCase().contains("orc")) {
            fileFormatType = TFileFormatType.FORMAT_ORC;
        } else {
            fileFormatType = TFileFormatType.FORMAT_PARQUET;
        }
        return fileFormatType;
    }

    protected TDataSinkType getDataSinkType() {
        return TDataSinkType.HIVE_TABLE_SINK;
    }

    public void complete(Analyzer analyzer) {

    }

    private void toTDataSink() {

    }
}
