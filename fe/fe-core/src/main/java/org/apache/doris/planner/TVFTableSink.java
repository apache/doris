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
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.OrcFileFormatProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TTVFTableSink;
import org.apache.doris.thrift.TTVFWriterType;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TVFTableSink is used for INSERT INTO tvf_name(properties) SELECT ...
 * It writes query results to files via TVF (local/s3/hdfs).
 *
 * Property parsing reuses the same StorageProperties and FileFormatProperties
 * infrastructure as the read-side TVF (SELECT * FROM s3/hdfs/local(...)).
 */
public class TVFTableSink extends DataSink {
    private final PlanNodeId exchNodeId;
    private final String tvfName;
    private final Map<String, String> properties;
    private final List<Column> cols;
    private TDataSink tDataSink;

    public TVFTableSink(PlanNodeId exchNodeId, String tvfName, Map<String, String> properties, List<Column> cols) {
        this.exchNodeId = exchNodeId;
        this.tvfName = tvfName;
        this.properties = properties;
        this.cols = cols;
    }

    public void bindDataSink() throws AnalysisException {
        TTVFTableSink tSink = new TTVFTableSink();
        tSink.setTvfName(tvfName);

        // --- 1. Parse file format properties (reuse read-side FileFormatProperties) ---
        // Make a mutable copy; FileFormatProperties.analyzeFileFormatProperties removes consumed keys.
        Map<String, String> propsCopy = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        propsCopy.putAll(properties);

        String formatStr = propsCopy.getOrDefault("format", "csv").toLowerCase();
        propsCopy.remove("format");

        // Also consume "compression_type" as alias for "compress_type" (write-side convention)
        if (propsCopy.containsKey("compression_type") && !propsCopy.containsKey("compress_type")) {
            propsCopy.put("compress_type", propsCopy.remove("compression_type"));
        }

        FileFormatProperties fileFormatProps = FileFormatProperties.createFileFormatProperties(formatStr);
        fileFormatProps.analyzeFileFormatProperties(propsCopy, true);

        TFileFormatType formatType = fileFormatProps.getFileFormatType();
        if (!Util.isCsvFormat(formatType) && formatType != TFileFormatType.FORMAT_PARQUET
                && formatType != TFileFormatType.FORMAT_ORC) {
            throw new AnalysisException("Unsupported format: " + formatType.name());
        }
        tSink.setFileFormat(formatType);

        // Set file type based on TVF name
        TFileType fileType = getFileType(tvfName);
        tSink.setFileType(fileType);

        // --- 2. Parse storage/connection properties (reuse read-side StorageProperties) ---
        Map<String, String> backendConnectProps;
        if (tvfName.equals("local")) {
            // Local TVF: pass properties as-is (same as LocalProperties.getBackendConfigProperties)
            backendConnectProps = new java.util.HashMap<>(propsCopy);
        } else {
            // S3/HDFS: use StorageProperties to normalize connection property keys
            // (e.g. "s3.endpoint" -> "AWS_ENDPOINT", "hadoop.username" -> hadoop config)
            try {
                StorageProperties storageProps = StorageProperties.createPrimary(propsCopy);
                backendConnectProps = storageProps.getBackendConfigProperties();
            } catch (Exception e) {
                throw new AnalysisException("Failed to parse storage properties: " + e.getMessage(), e);
            }
        }

        String filePath = properties.get("file_path");
        tSink.setFilePath(filePath);

        // Set normalized properties for BE
        tSink.setProperties(backendConnectProps);

        // Set columns
        List<TColumn> tColumns = new ArrayList<>();
        for (Column col : cols) {
            tColumns.add(col.toThrift());
        }
        tSink.setColumns(tColumns);

        // --- 3. Set format-specific sink options ---
        if (fileFormatProps instanceof CsvFileFormatProperties) {
            CsvFileFormatProperties csvProps = (CsvFileFormatProperties) fileFormatProps;
            csvProps.checkSupportedCompressionType(true);
            tSink.setColumnSeparator(csvProps.getColumnSeparator());
            tSink.setLineDelimiter(csvProps.getLineDelimiter());
            tSink.setCompressionType(csvProps.getCompressionType());
        } else if (fileFormatProps instanceof OrcFileFormatProperties) {
            tSink.setCompressionType(((OrcFileFormatProperties) fileFormatProps).getOrcCompressionType());
        }
        // Parquet compression is handled by BE via parquet writer options

        // --- 4. Set sink-specific options ---
        // Max file size
        String maxFileSizeStr = properties.get("max_file_size");
        if (maxFileSizeStr != null) {
            tSink.setMaxFileSizeBytes(Long.parseLong(maxFileSizeStr));
        }

        // Delete existing files is handled by FE (InsertIntoTVFCommand), always tell BE not to delete
        tSink.setDeleteExistingFiles(false);

        // Backend id for local TVF
        String backendIdStr = properties.get("backend_id");
        if (backendIdStr != null) {
            tSink.setBackendId(Long.parseLong(backendIdStr));
        }

        // Set hadoop config for hdfs/s3 (BE uses this for file writer creation)
        if (!tvfName.equals("local")) {
            tSink.setHadoopConfig(backendConnectProps);
        }

        // Set writer_type: JNI if writer_class is specified, otherwise NATIVE
        String writerClass = properties.get("writer_class");
        if (writerClass != null) {
            tSink.setWriterType(TTVFWriterType.JNI);
            tSink.setWriterClass(writerClass);
        } else {
            tSink.setWriterType(TTVFWriterType.NATIVE);
        }

        tDataSink = new TDataSink(TDataSinkType.TVF_TABLE_SINK);
        tDataSink.setTvfTableSink(tSink);
    }

    private TFileType getFileType(String tvfName) throws AnalysisException {
        switch (tvfName.toLowerCase()) {
            case "local":
                return TFileType.FILE_LOCAL;
            case "s3":
                return TFileType.FILE_S3;
            case "hdfs":
                return TFileType.FILE_HDFS;
            default:
                throw new AnalysisException("Unsupported TVF type: " + tvfName);
        }
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("TVF TABLE SINK\n");
        strBuilder.append(prefix).append("  tvfName: ").append(tvfName).append("\n");
        strBuilder.append(prefix).append("  filePath: ").append(properties.get("file_path")).append("\n");
        strBuilder.append(prefix).append("  format: ").append(properties.getOrDefault("format", "csv")).append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        return tDataSink;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }
}
