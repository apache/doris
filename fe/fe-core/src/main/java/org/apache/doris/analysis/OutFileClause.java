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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// For syntax select * from tbl INTO OUTFILE xxxx
public class OutFileClause {
    private static final Logger LOG = LogManager.getLogger(OutFileClause.class);

    public static final List<String> RESULT_COL_NAMES = Lists.newArrayList();
    public static final List<PrimitiveType> RESULT_COL_TYPES = Lists.newArrayList();
    public static final List<String> PARQUET_REPETITION_TYPES = Lists.newArrayList();
    public static final List<String> PARQUET_DATA_TYPES = Lists.newArrayList();

    static {
        RESULT_COL_NAMES.add("FileNumber");
        RESULT_COL_NAMES.add("TotalRows");
        RESULT_COL_NAMES.add("FileSize");
        RESULT_COL_NAMES.add("URL");

        RESULT_COL_TYPES.add(PrimitiveType.INT);
        RESULT_COL_TYPES.add(PrimitiveType.BIGINT);
        RESULT_COL_TYPES.add(PrimitiveType.BIGINT);
        RESULT_COL_TYPES.add(PrimitiveType.VARCHAR);

        PARQUET_REPETITION_TYPES.add("required");
        PARQUET_REPETITION_TYPES.add("repeated");
        PARQUET_REPETITION_TYPES.add("optional");

        PARQUET_DATA_TYPES.add("boolean");
        PARQUET_DATA_TYPES.add("int32");
        PARQUET_DATA_TYPES.add("int64");
        PARQUET_DATA_TYPES.add("int96");
        PARQUET_DATA_TYPES.add("byte_array");
        PARQUET_DATA_TYPES.add("float");
        PARQUET_DATA_TYPES.add("double");
        PARQUET_DATA_TYPES.add("fixed_len_byte_array");
    }

    public static final String LOCAL_FILE_PREFIX = "file:///";
    private static final String BROKER_PROP_PREFIX = "broker.";
    private static final String PROP_BROKER_NAME = "broker.name";
    private static final String PROP_COLUMN_SEPARATOR = "column_separator";
    private static final String PROP_LINE_DELIMITER = "line_delimiter";
    private static final String PROP_MAX_FILE_SIZE = "max_file_size";
    private static final String PROP_SUCCESS_FILE_NAME = "success_file_name";
    private static final String PARQUET_PROP_PREFIX = "parquet.";
    private static final String SCHEMA = "schema";

    private static final long DEFAULT_MAX_FILE_SIZE_BYTES = 1 * 1024 * 1024 * 1024; // 1GB
    private static final long MIN_FILE_SIZE_BYTES = 5 * 1024 * 1024L; // 5MB
    private static final long MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024L; // 2GB


    private String filePath;
    private String format;
    private Map<String, String> properties;

    // set following members after analyzing
    private String columnSeparator = "\t";
    private String lineDelimiter = "\n";
    private TFileFormatType fileFormatType;
    private long maxFileSizeBytes = DEFAULT_MAX_FILE_SIZE_BYTES;
    private BrokerDesc brokerDesc = null;
    // True if result is written to local disk.
    // If set to true, the brokerDesc must be null.
    private boolean isLocalOutput = false;
    private String successFileName = "";
    private List<List<String>> schema = new ArrayList<>();
    private Map<String, String> fileProperties = new HashMap<>();

    public OutFileClause(String filePath, String format, Map<String, String> properties) {
        this.filePath = filePath;
        this.format = Strings.isNullOrEmpty(format) ? "csv" : format.toLowerCase();
        this.properties = properties;
    }

    public OutFileClause(OutFileClause other) {
        this.filePath = other.filePath;
        this.format = other.format;
        this.properties = other.properties == null ? null : Maps.newHashMap(other.properties);
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public TFileFormatType getFileFormatType() {
        return fileFormatType;
    }

    public long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        analyzeFilePath();

        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }
        switch (this.format) {
            case "csv":
                fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            case "parquet":
                fileFormatType = TFileFormatType.FORMAT_PARQUET;
                break;
            default:
                throw new AnalysisException("format:" + this.format + " is not supported.");
        }

        analyzeProperties();

        if (brokerDesc != null && isLocalOutput) {
            throw new AnalysisException("No need to specify BROKER properties in OUTFILE clause for local file output");
        } else if (brokerDesc == null && !isLocalOutput) {
            throw new AnalysisException("Must specify BROKER properties in OUTFILE clause");
        }
    }

    public void analyze(Analyzer analyzer, SelectStmt stmt) throws AnalysisException {
        analyze(analyzer);
        List<SelectListItem> items = stmt.getSelectList().getItems();
        for (SelectListItem item:items) {
            if (item.getExpr().getType() == Type.LARGEINT && isParquetFormat()) {
                throw new AnalysisException("currently parquet do not support largeint type");
            }
        }
    }

    private void analyzeFilePath() throws AnalysisException {
        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }

        if (filePath.startsWith(LOCAL_FILE_PREFIX)) {
            if (!Config.enable_outfile_to_local) {
                throw new AnalysisException("Exporting results to local disk is not allowed.");
            }
            isLocalOutput = true;
            filePath = filePath.substring(LOCAL_FILE_PREFIX.length() - 1); // leave last '/'
        } else {
            isLocalOutput = false;
        }
    }

    private void analyzeProperties() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        Set<String> processedPropKeys = Sets.newHashSet();
        getBrokerProperties(processedPropKeys);

        if (properties.containsKey(PROP_COLUMN_SEPARATOR)) {
            if (!isCsvFormat()) {
                throw new AnalysisException(PROP_COLUMN_SEPARATOR + " is only for CSV format");
            }
            columnSeparator = properties.get(PROP_COLUMN_SEPARATOR);
            processedPropKeys.add(PROP_COLUMN_SEPARATOR);
        }

        if (properties.containsKey(PROP_LINE_DELIMITER)) {
            if (!isCsvFormat()) {
                throw new AnalysisException(PROP_LINE_DELIMITER + " is only for CSV format");
            }
            lineDelimiter = properties.get(PROP_LINE_DELIMITER);
            processedPropKeys.add(PROP_LINE_DELIMITER);
        }

        if (properties.containsKey(PROP_MAX_FILE_SIZE)) {
            maxFileSizeBytes = ParseUtil.analyzeDataVolumn(properties.get(PROP_MAX_FILE_SIZE));
            if (maxFileSizeBytes > MAX_FILE_SIZE_BYTES || maxFileSizeBytes < MIN_FILE_SIZE_BYTES) {
                throw new AnalysisException("max file size should between 5MB and 2GB. Given: " + maxFileSizeBytes);
            }
            processedPropKeys.add(PROP_MAX_FILE_SIZE);
        }

        if (properties.containsKey(PROP_SUCCESS_FILE_NAME)) {
            successFileName = properties.get(PROP_SUCCESS_FILE_NAME);
            FeNameFormat.checkCommonName("file name", successFileName);
            processedPropKeys.add(PROP_SUCCESS_FILE_NAME);
        }

        if (this.fileFormatType == TFileFormatType.FORMAT_PARQUET) {
            getParquetProperties(processedPropKeys);
        }

        if (processedPropKeys.size() != properties.size()) {
            LOG.debug("{} vs {}", processedPropKeys, properties);
            throw new AnalysisException("Unknown properties: " + properties.keySet().stream()
                    .filter(k -> !processedPropKeys.contains(k)).collect(Collectors.toList()));
        }

    }

    private void getBrokerProperties(Set<String> processedPropKeys) {
        if (!properties.containsKey(PROP_BROKER_NAME)) {
            return;
        }
        String brokerName = properties.get(PROP_BROKER_NAME);
        processedPropKeys.add(PROP_BROKER_NAME);

        Map<String, String> brokerProps = Maps.newHashMap();
        Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(BROKER_PROP_PREFIX) && !entry.getKey().equals(PROP_BROKER_NAME)) {
                brokerProps.put(entry.getKey().substring(BROKER_PROP_PREFIX.length()), entry.getValue());
                processedPropKeys.add(entry.getKey());
            }
        }

        brokerDesc = new BrokerDesc(brokerName, brokerProps);
    }

    /**
     * example:
     * SELECT citycode FROM table1 INTO OUTFILE "file:///root/doris/"
     * FORMAT AS PARQUET PROPERTIES ("schema"="required,int32,siteid;", "parquet.compression"="snappy");
     *
     * schema: it defined the schema of parquet file, it consists of 3 field: competition type, data type, column name
     * multiple columns is split by `;`
     *
     * prefix with 'parquet.' defines the properties of parquet file,
     * currently only supports: compression, disable_dictionary, version
     */
    private void getParquetProperties(Set<String> processedPropKeys) throws AnalysisException {
        String schema = properties.get(SCHEMA);
        if (schema == null || schema.length() <= 0) {
            throw new AnalysisException("schema is required for parquet file");
        }
        schema = schema.replace(" ","");
        schema = schema.toLowerCase();
        String[] schemas = schema.split(";");
        for (String item:schemas) {
            String[] properties = item.split(",");
            if (properties.length != 3) {
                throw new AnalysisException("must only contains repetition type/column type/column name");
            }
            if (!PARQUET_REPETITION_TYPES.contains(properties[0])) {
                throw new AnalysisException("unknown repetition type");
            }
            if (!properties[0].equalsIgnoreCase("required")) {
                throw new AnalysisException("currently only support required type");
            }
            if (!PARQUET_DATA_TYPES.contains(properties[1])) {
                throw new AnalysisException("data type is not supported:"+properties[1]);
            }
            List<String> column = new ArrayList<>();
            column.addAll(Arrays.asList(properties));
            this.schema.add(column);
        }
        processedPropKeys.add(SCHEMA);
        Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(PARQUET_PROP_PREFIX)) {
                processedPropKeys.add(entry.getKey());
                fileProperties.put(entry.getKey().substring(PARQUET_PROP_PREFIX.length()), entry.getValue());
            }
        }
    }

    private boolean isCsvFormat() {
        return fileFormatType == TFileFormatType.FORMAT_CSV_BZ2
                || fileFormatType == TFileFormatType.FORMAT_CSV_DEFLATE
                || fileFormatType == TFileFormatType.FORMAT_CSV_GZ
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZ4FRAME
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZO
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZOP
                || fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN;
    }

    private boolean isParquetFormat() {
        return fileFormatType == TFileFormatType.FORMAT_PARQUET;
    }

    @Override
    public OutFileClause clone() {
        return new OutFileClause(this);
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(" INTO OUTFILE '").append(filePath).append(" FORMAT AS ").append(format);
        if (properties != null && !properties.isEmpty()) {
            sb.append(" PROPERTIES(");
            sb.append(new PrintableMap<>(properties, " = ", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

    public TResultFileSinkOptions toSinkOptions() {
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions(filePath, fileFormatType);
        if (isCsvFormat()) {
            sinkOptions.setColumnSeparator(columnSeparator);
            sinkOptions.setLineDelimiter(lineDelimiter);
        }
        sinkOptions.setMaxFileSizeBytes(maxFileSizeBytes);
        if (brokerDesc != null) {
            sinkOptions.setBrokerProperties(brokerDesc.getProperties());
            // broker_addresses of sinkOptions will be set in Coordinator.
            // Because we need to choose the nearest broker with the result sink node.
        }
        if (!Strings.isNullOrEmpty(successFileName)) {
            sinkOptions.setSuccessFileName(successFileName);
        }
        if (isParquetFormat()) {
            sinkOptions.setSchema(this.schema);
            sinkOptions.setFileProperties(this.fileProperties);
        }
        return sinkOptions;
    }
}


