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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteSink;

import org.apache.iceberg.Table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Planner sink for Iceberg DELETE operations.
 * Generates TIcebergDeleteSink for BE to write delete files.
 */
public class IcebergDeleteSink extends BaseExternalTableDataSink {

    private final IcebergExternalTable targetTable;
    private final DeleteCommandContext deleteContext;

    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_PARQUET);
            add(TFileFormatType.FORMAT_ORC);
        }};

    // Store PropertiesMap, including vended credentials or static credentials
    private Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;

    public IcebergDeleteSink(IcebergExternalTable targetTable, DeleteCommandContext deleteContext) {
        super();
        if (targetTable.isView()) {
            throw new UnsupportedOperationException("DELETE from iceberg view is not supported");
        }
        this.targetTable = targetTable;
        this.deleteContext = deleteContext;

        IcebergExternalCatalog catalog = (IcebergExternalCatalog) targetTable.getCatalog();
        storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                catalog.getCatalogProperty().getMetastoreProperties(),
                catalog.getCatalogProperty().getStoragePropertiesMap(),
                targetTable.getIcebergTable());
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("ICEBERG DELETE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("  DeleteType: ")
                .append(deleteContext.getDeleteFileType()).append("\n");
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {

        TIcebergDeleteSink tSink = new TIcebergDeleteSink();

        Table icebergTable = targetTable.getIcebergTable();

        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        // Set delete type (POSITION_DELETES only)
        tSink.setDeleteType(deleteContext.toTFileContent());

        // File format and compression
        tSink.setFileFormat(getTFileFormatType(IcebergUtils.getFileFormat(icebergTable).name()));
        tSink.setCompressType(getTFileCompressType(IcebergUtils.getFileCompress(icebergTable)));

        // Hadoop config
        Map<String, String> props = new HashMap<>();
        for (StorageProperties storageProperties : storagePropertiesMap.values()) {
            props.putAll(storageProperties.getBackendConfigProperties());
        }
        tSink.setHadoopConfig(props);

        // Location for delete files (typically under metadata/)
        String tableLocation = IcebergUtils.dataLocation(icebergTable);
        LocationPath locationPath = LocationPath.of(tableLocation, storagePropertiesMap);
        tSink.setOutputPath(locationPath.toStorageLocation().toString());
        tSink.setTableLocation(tableLocation);

        TFileType fileType = locationPath.getTFileTypeForBE();
        tSink.setFileType(fileType);
        if (fileType.equals(TFileType.FILE_BROKER)) {
            tSink.setBrokerAddresses(getBrokerAddresses(targetTable.getCatalog().bindBrokerName()));
        }

        // Partition information
        if (icebergTable.spec().isPartitioned()) {
            tSink.setPartitionSpecId(icebergTable.spec().specId());
            // Partition data JSON will be set by BE based on actual data
        }

        tDataSink = new TDataSink(TDataSinkType.ICEBERG_DELETE_SINK);
        tDataSink.setIcebergDeleteSink(tSink);
    }
}
