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

package org.apache.doris.planner;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveColumn;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.TPaimonTableSink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PaimonTableSink extends BaseExternalTableDataSink {

    private final PaimonExternalTable targetTable;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public PaimonTableSink(PaimonExternalTable targetTable) {
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
        strBuilder.append(prefix).append("PAIMON TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("Table: ")
                .append(targetTable.getDbName()).append(".").append(targetTable.getName()).append("\n");
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {
        TPaimonTableSink tSink = new TPaimonTableSink();

        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        // Get paimon table metadata
        Table paimonTable = targetTable.getPaimonTable(Optional.empty());

        // file format - get from table options, default to parquet
        String fileFormat = paimonTable.options().getOrDefault(
                CoreOptions.FILE_FORMAT.key(), "parquet");
        TFileFormatType formatType = getTFileFormatType(fileFormat.toUpperCase());
        tSink.setFileFormat(formatType);

        // compression type
        TFileCompressType compressType = formatType == TFileFormatType.FORMAT_PARQUET
                ? TFileCompressType.SNAPPYBLOCK : TFileCompressType.ZLIB;
        tSink.setCompressionType(compressType);

        // output path - use paimon table location
        String location = ((FileStoreTable) paimonTable).location().toString();
        tSink.setOutputPath(location);

        // file type (S3, HDFS, etc.)
        TFileType fileType = LocationPath.getTFileTypeForBE(location);
        tSink.setFileType(fileType);
        if (fileType == TFileType.FILE_BROKER) {
            tSink.setBrokerAddresses(getBrokerAddresses(targetTable.getCatalog().bindBrokerName()));
        }

        // hadoop config from catalog properties
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) targetTable.getCatalog();
        Map<String, String> props = new HashMap<>(catalog.getPaimonOptionsMap());
        tSink.setHadoopConfig(props);

        // overwrite flag
        if (insertCtx.isPresent()) {
            PaimonInsertCommandContext context = (PaimonInsertCommandContext) insertCtx.get();
            tSink.setOverwrite(context.isOverwrite());
        }

        // partition column names
        List<String> partitionKeys = paimonTable.partitionKeys();
        if (!partitionKeys.isEmpty()) {
            tSink.setPartitionColumns(partitionKeys);
        }

        // column descriptors (PARTITION_KEY vs REGULAR) - used by BE to identify partition columns
        Set<String> partitionKeySet = new HashSet<>(partitionKeys);
        List<Column> allColumns = targetTable.getColumns();
        List<THiveColumn> columns = new ArrayList<>();
        for (Column col : allColumns) {
            THiveColumn tCol = new THiveColumn();
            tCol.setName(col.getName());
            tCol.setColumnType(partitionKeySet.contains(col.getName())
                    ? THiveColumnType.PARTITION_KEY : THiveColumnType.REGULAR);
            columns.add(tCol);
        }
        tSink.setColumns(columns);

        tDataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        tDataSink.setPaimonTableSink(tSink);
    }
}
