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

import org.apache.doris.backup.HDFSStorage;
import org.apache.doris.backup.S3Storage;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.collections.map.CaseInsensitiveMap;
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
    private static final String S3_FILE_PREFIX = "S3://";
    private static final String HDFS_FILE_PREFIX = "hdfs://";
    private static final String HADOOP_FS_PROP_PREFIX = "dfs.";
    private static final String HADOOP_PROP_PREFIX = "hadoop.";
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

    private boolean isAnalyzed = false;

    public OutFileClause(String filePath, String format, Map<String, String> properties) {
        this.filePath = filePath;
        this.format = Strings.isNullOrEmpty(format) ? "csv" : format.toLowerCase();
        this.properties = properties;
        this.isAnalyzed = false;
    }

    public OutFileClause(OutFileClause other) {
        this.filePath = other.filePath;
        this.format = other.format;
        this.properties = other.properties == null ? null : Maps.newHashMap(other.properties);
        this.isAnalyzed = other.isAnalyzed;
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

    public List<List<String>> getSchema() {
        return schema;
    }

    public void analyze(Analyzer analyzer, List<Expr> resultExprs) throws UserException {
        if (isAnalyzed) {
            // If the query stmt is rewritten, the whole stmt will be analyzed again.
            // But some of fields in this OutfileClause has been changed,
            // such as `filePath`'s schema header has been removed.
            // So OutfileClause does not support to be analyzed again.
            return;
        }
        analyzeFilePath();

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
        isAnalyzed = true;

        if (isParquetFormat()) {
            analyzeForParquetFormat(resultExprs);
        }
    }

    private void analyzeForParquetFormat(List<Expr> resultExprs) throws AnalysisException {
        if (this.schema.isEmpty()) {
            genParquetSchema(resultExprs);
        }

        // check schema number
        if (resultExprs.size() != this.schema.size()) {
            throw new AnalysisException("Parquet schema number does not equal to select item number");
        }

        // check type
        for (int i = 0; i < this.schema.size(); ++i) {
            String type = this.schema.get(i).get(1);
            Type resultType = resultExprs.get(i).getType();
            switch (resultType.getPrimitiveType()) {
                case BOOLEAN:
                    if (!type.equals("boolean")) {
                        throw new AnalysisException("project field type is BOOLEAN, should use boolean, but the type of column "
                                + i + " is " + type);
                    }
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                    if (!type.equals("int32")) {
                        throw new AnalysisException("project field type is TINYINT/SMALLINT/INT, should use int32, "
                                + "but the definition type of column " + i + " is " + type);
                    }
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    if (!type.equals("int64")) {
                        throw new AnalysisException("project field type is BIGINT/DATE/DATETIME, should use int64, " +
                                "but the definition type of column " + i + " is " + type);
                    }
                    break;
                case FLOAT:
                    if (!type.equals("float")) {
                        throw new AnalysisException("project field type is FLOAT, should use float, but the definition type of column "
                                + i + " is " + type);
                    }
                    break;
                case DOUBLE:
                    if (!type.equals("double")) {
                        throw new AnalysisException("project field type is DOUBLE, should use double, but the definition type of column "
                                + i + " is " + type);
                    }
                    break;
                case CHAR:
                case VARCHAR:
                case STRING:
                case DECIMALV2:
                    if (!type.equals("byte_array")) {
                        throw new AnalysisException("project field type is CHAR/VARCHAR/STRING/DECIMAL, should use byte_array, " +
                                "but the definition type of column " + i + " is " + type);
                    }
                    break;
                case HLL:
                case BITMAP:
                    if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isReturnObjectDataAsBinary()) {
                        if (!type.equals("byte_array")) {
                            throw new AnalysisException("project field type is HLL/BITMAP, should use byte_array, " +
                                    "but the definition type of column " + i + " is " + type);
                        }
                    } else {
                        throw new AnalysisException("Parquet format does not support column type: " + resultType.getPrimitiveType());
                    }
                    break;
                default:
                    throw new AnalysisException("Parquet format does not support column type: " + resultType.getPrimitiveType());
            }
        }
    }

    private void genParquetSchema(List<Expr> resultExprs) throws AnalysisException {
        Preconditions.checkState(this.schema.isEmpty());
        for (int i = 0; i < resultExprs.size(); ++i) {
            Expr expr = resultExprs.get(i);
            List<String> column = new ArrayList<>();
            column.add("required");
            switch (expr.getType().getPrimitiveType()) {
                case BOOLEAN:
                    column.add("boolean");
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                    column.add("int32");
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    column.add("int64");
                    break;
                case FLOAT:
                    column.add("float");
                    break;
                case DOUBLE:
                    column.add("double");
                    break;
                case CHAR:
                case VARCHAR:
                case STRING:
                case DECIMALV2:
                    column.add("byte_array");
                    break;
                case HLL:
                case BITMAP:
                    if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isReturnObjectDataAsBinary()) {
                        column.add("byte_array");
                    }
                default:
                    throw new AnalysisException("currently parquet do not support column type: " + expr.getType().getPrimitiveType());
            }
            column.add("col" + i);
            this.schema.add(column);
        }
    }

    private void analyzeFilePath() throws AnalysisException {
        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }

        if (filePath.startsWith(LOCAL_FILE_PREFIX)) {
            if (!Config.enable_outfile_to_local) {
                throw new AnalysisException("Exporting results to local disk is not allowed." 
                    + " To enable this feature, you need to add `enable_outfile_to_local=true` in fe.conf and restart FE");
            }
            isLocalOutput = true;
            filePath = filePath.substring(LOCAL_FILE_PREFIX.length() - 1); // leave last '/'
        } else {
            isLocalOutput = false;
        }

        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }
    }

    private void analyzeProperties() throws UserException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        Set<String> processedPropKeys = Sets.newHashSet();
        analyzeBrokerDesc(processedPropKeys);

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

    /**
     * The following two situations will generate the corresponding @brokerDesc:
     * 1. broker: with broker name
     * 2. s3: with s3 pattern path, without broker name
     */
    private void analyzeBrokerDesc(Set<String> processedPropKeys) throws UserException {
        String brokerName = properties.get(PROP_BROKER_NAME);
        StorageBackend.StorageType storageType;
        if (properties.containsKey(PROP_BROKER_NAME)) {
            processedPropKeys.add(PROP_BROKER_NAME);
            storageType = StorageBackend.StorageType.BROKER;
        } else if (filePath.toUpperCase().startsWith(S3_FILE_PREFIX)) {
            brokerName = StorageBackend.StorageType.S3.name();
            storageType = StorageBackend.StorageType.S3;
        } else if (filePath.toUpperCase().startsWith(HDFS_FILE_PREFIX.toUpperCase())) {
            brokerName = StorageBackend.StorageType.HDFS.name();
            storageType = StorageBackend.StorageType.HDFS;
            filePath = filePath.substring(HDFS_FILE_PREFIX.length() - 1);
        } else {
            return;
        }

        Map<String, String> brokerProps = Maps.newHashMap();
        Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(BROKER_PROP_PREFIX) && !entry.getKey().equals(PROP_BROKER_NAME)) {
                brokerProps.put(entry.getKey().substring(BROKER_PROP_PREFIX.length()), entry.getValue());
                processedPropKeys.add(entry.getKey());
            } else if (entry.getKey().toUpperCase().startsWith(S3Storage.S3_PROPERTIES_PREFIX)) {
                brokerProps.put(entry.getKey(), entry.getValue());
                processedPropKeys.add(entry.getKey());
            } else if (entry.getKey().contains(BrokerUtil.HADOOP_FS_NAME)
                && storageType == StorageBackend.StorageType.HDFS) {
                brokerProps.put(entry.getKey(), entry.getValue());
                processedPropKeys.add(entry.getKey());
            } else if ((entry.getKey().startsWith(HADOOP_FS_PROP_PREFIX) || entry.getKey().startsWith(HADOOP_PROP_PREFIX))
                && storageType == StorageBackend.StorageType.HDFS) {
                brokerProps.put(entry.getKey(), entry.getValue());
                processedPropKeys.add(entry.getKey());
            }
        }
        if (storageType == StorageBackend.StorageType.S3) {
            S3Storage.checkS3(new CaseInsensitiveMap(brokerProps));
        } else if (storageType == StorageBackend.StorageType.HDFS) {
            HDFSStorage.checkHDFS(new CaseInsensitiveMap(brokerProps));
        }

        brokerDesc = new BrokerDesc(brokerName, storageType, brokerProps);
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
        // save all parquet prefix property
        Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(PARQUET_PROP_PREFIX)) {
                processedPropKeys.add(entry.getKey());
                fileProperties.put(entry.getKey().substring(PARQUET_PROP_PREFIX.length()), entry.getValue());
            }
        }

        // check schema. if schema is not set, Doris will gen schema by select items
        String schema = properties.get(SCHEMA);
        if (schema == null) {
            return;
        }
        if (schema.isEmpty()) {
            throw new AnalysisException("Parquet schema property should not be empty");
        }
        schema = schema.replace(" ", "");
        schema = schema.toLowerCase();
        String[] schemas = schema.split(";");
        for (String item : schemas) {
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


