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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.HiveInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveBucket;
import org.apache.doris.thrift.THiveColumn;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartition;
import org.apache.doris.thrift.THiveTableSink;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HiveTableSink extends BaseExternalTableDataSink {

    private final HMSExternalTable targetTable;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public HiveTableSink(HMSExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
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
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {
        THiveTableSink tSink = new THiveTableSink();
        tSink.setDbName(targetTable.getDbName());
        tSink.setTableName(targetTable.getName());
        Set<String> partNames = new HashSet<>(targetTable.getPartitionColumnNames());
        List<Column> allColumns = targetTable.getColumns();
        Set<String> colNames = allColumns.stream().map(Column::getName).collect(Collectors.toSet());
        colNames.removeAll(partNames);
        List<THiveColumn> targetColumns = new ArrayList<>();
        for (Column col : allColumns) {
            if (partNames.contains(col.getName())) {
                THiveColumn tHiveColumn = new THiveColumn();
                tHiveColumn.setName(col.getName());
                tHiveColumn.setColumnType(THiveColumnType.PARTITION_KEY);
                targetColumns.add(tHiveColumn);
            } else if (colNames.contains(col.getName())) {
                THiveColumn tHiveColumn = new THiveColumn();
                tHiveColumn.setName(col.getName());
                tHiveColumn.setColumnType(THiveColumnType.REGULAR);
                targetColumns.add(tHiveColumn);
            }
        }
        tSink.setColumns(targetColumns);

        setPartitionValues(tSink);

        StorageDescriptor sd = targetTable.getRemoteTable().getSd();
        THiveBucket bucketInfo = new THiveBucket();
        bucketInfo.setBucketedBy(sd.getBucketCols());
        bucketInfo.setBucketCount(sd.getNumBuckets());
        tSink.setBucketInfo(bucketInfo);

        TFileFormatType formatType = getTFileFormatType(sd.getInputFormat());
        tSink.setFileFormat(formatType);
        setCompressType(tSink, formatType);

        THiveLocationParams locationParams = new THiveLocationParams();
        LocationPath locationPath = new LocationPath(sd.getLocation(), targetTable.getHadoopProperties());
        String location = locationPath.toString();
        String storageLocation = locationPath.toStorageLocation().toString();
        TFileType fileType = locationPath.getTFileTypeForBE();
        if (fileType == TFileType.FILE_S3) {
            locationParams.setWritePath(storageLocation);
            locationParams.setOriginalWritePath(location);
            locationParams.setTargetPath(location);
            if (insertCtx.isPresent()) {
                HiveInsertCommandContext context = (HiveInsertCommandContext) insertCtx.get();
                tSink.setOverwrite(context.isOverwrite());
                context.setWritePath(storageLocation);
            }
        } else {
            String writeTempPath = createTempPath(location);
            locationParams.setWritePath(writeTempPath);
            locationParams.setOriginalWritePath(writeTempPath);
            locationParams.setTargetPath(location);
            if (insertCtx.isPresent()) {
                HiveInsertCommandContext context = (HiveInsertCommandContext) insertCtx.get();
                tSink.setOverwrite(context.isOverwrite());
                context.setWritePath(writeTempPath);
            }
        }
        locationParams.setFileType(fileType);
        tSink.setLocation(locationParams);

        tSink.setHadoopConfig(targetTable.getHadoopProperties());

        tDataSink = new TDataSink(getDataSinkType());
        tDataSink.setHiveTableSink(tSink);
    }

    private String createTempPath(String location) {
        String user = ConnectContext.get().getUserIdentity().getUser();
        return LocationPath.getTempWritePath(location, "/tmp/.doris_staging/" + user);
    }

    private void setCompressType(THiveTableSink tSink, TFileFormatType formatType) {
        String compressType;
        switch (formatType) {
            case FORMAT_ORC:
                compressType = targetTable.getRemoteTable().getParameters().get("orc.compress");
                break;
            case FORMAT_PARQUET:
                compressType = targetTable.getRemoteTable().getParameters().get("parquet.compression");
                break;
            default:
                compressType = "uncompressed";
                break;
        }
        tSink.setCompressionType(getTFileCompressType(compressType));
    }

    private void setPartitionValues(THiveTableSink tSink) throws AnalysisException {
        List<THivePartition> partitions = new ArrayList<>();
        List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions =
                ((HMSExternalCatalog) targetTable.getCatalog())
                        .getClient().listPartitions(targetTable.getDbName(), targetTable.getName());
        for (org.apache.hadoop.hive.metastore.api.Partition partition : hivePartitions) {
            THivePartition hivePartition = new THivePartition();
            StorageDescriptor sd = partition.getSd();
            hivePartition.setFileFormat(getTFileFormatType(sd.getInputFormat()));

            hivePartition.setValues(partition.getValues());
            THiveLocationParams locationParams = new THiveLocationParams();
            String location = sd.getLocation();
            // pass the same of write path and target path to partition
            locationParams.setWritePath(location);
            locationParams.setTargetPath(location);
            locationParams.setFileType(LocationPath.getTFileTypeForBE(location));
            hivePartition.setLocation(locationParams);
            partitions.add(hivePartition);
        }
        tSink.setPartitions(partitions);
    }

    protected TDataSinkType getDataSinkType() {
        return TDataSinkType.HIVE_TABLE_SINK;
    }
}
