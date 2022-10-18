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

import org.apache.doris.backup.HdfsStorage;
import org.apache.doris.backup.S3Storage;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TParquetCompressionType;
import org.apache.doris.thrift.TParquetDataType;
import org.apache.doris.thrift.TParquetRepetitionType;
import org.apache.doris.thrift.TParquetSchema;
import org.apache.doris.thrift.TParquetVersion;
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
    public static final Map<String, TParquetRepetitionType> PARQUET_REPETITION_TYPE_MAP = Maps.newHashMap();
    public static final Map<String, TParquetDataType> PARQUET_DATA_TYPE_MAP = Maps.newHashMap();
    public static final Map<String, TParquetCompressionType> PARQUET_COMPRESSION_TYPE_MAP = Maps.newHashMap();
    public static final Map<String, TParquetVersion> PARQUET_VERSION_MAP = Maps.newHashMap();
    public static final Set<String> ORC_DATA_TYPE = Sets.newHashSet();

    static {
        RESULT_COL_NAMES.add("FileNumber");
        RESULT_COL_NAMES.add("TotalRows");
        RESULT_COL_NAMES.add("FileSize");
        RESULT_COL_NAMES.add("URL");

        RESULT_COL_TYPES.add(PrimitiveType.INT);
        RESULT_COL_TYPES.add(PrimitiveType.BIGINT);
        RESULT_COL_TYPES.add(PrimitiveType.BIGINT);
        RESULT_COL_TYPES.add(PrimitiveType.VARCHAR);

        PARQUET_REPETITION_TYPE_MAP.put("required", TParquetRepetitionType.REQUIRED);
        PARQUET_REPETITION_TYPE_MAP.put("repeated", TParquetRepetitionType.REPEATED);
        PARQUET_REPETITION_TYPE_MAP.put("optional", TParquetRepetitionType.OPTIONAL);

        PARQUET_DATA_TYPE_MAP.put("boolean", TParquetDataType.BOOLEAN);
        PARQUET_DATA_TYPE_MAP.put("int32", TParquetDataType.INT32);
        PARQUET_DATA_TYPE_MAP.put("int64", TParquetDataType.INT64);
        PARQUET_DATA_TYPE_MAP.put("int96", TParquetDataType.INT96);
        PARQUET_DATA_TYPE_MAP.put("byte_array", TParquetDataType.BYTE_ARRAY);
        PARQUET_DATA_TYPE_MAP.put("float", TParquetDataType.FLOAT);
        PARQUET_DATA_TYPE_MAP.put("double", TParquetDataType.DOUBLE);
        PARQUET_DATA_TYPE_MAP.put("fixed_len_byte_array", TParquetDataType.FIXED_LEN_BYTE_ARRAY);

        PARQUET_COMPRESSION_TYPE_MAP.put("snappy", TParquetCompressionType.SNAPPY);
        PARQUET_COMPRESSION_TYPE_MAP.put("gzip", TParquetCompressionType.GZIP);
        PARQUET_COMPRESSION_TYPE_MAP.put("brotli", TParquetCompressionType.BROTLI);
        PARQUET_COMPRESSION_TYPE_MAP.put("zstd", TParquetCompressionType.ZSTD);
        PARQUET_COMPRESSION_TYPE_MAP.put("lz4", TParquetCompressionType.LZ4);
        PARQUET_COMPRESSION_TYPE_MAP.put("lzo", TParquetCompressionType.LZO);
        PARQUET_COMPRESSION_TYPE_MAP.put("bz2", TParquetCompressionType.BZ2);
        PARQUET_COMPRESSION_TYPE_MAP.put("default", TParquetCompressionType.UNCOMPRESSED);

        PARQUET_VERSION_MAP.put("v1", TParquetVersion.PARQUET_1_0);
        PARQUET_VERSION_MAP.put("latest", TParquetVersion.PARQUET_2_LATEST);

        ORC_DATA_TYPE.add("bigint");
        ORC_DATA_TYPE.add("boolean");
        ORC_DATA_TYPE.add("double");
        ORC_DATA_TYPE.add("float");
        ORC_DATA_TYPE.add("int");
        ORC_DATA_TYPE.add("smallint");
        ORC_DATA_TYPE.add("string");
        ORC_DATA_TYPE.add("tinyint");
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
    private static final String ORC_PROP_PREFIX = "orc.";
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

    private List<TParquetSchema> parquetSchemas = new ArrayList<>();

    private List<Pair<String, String>> orcSchemas = new ArrayList<>();

    private boolean isAnalyzed = false;
    private String headerType = "";

    private static final String PARQUET_COMPRESSION = "compression";
    private TParquetCompressionType parquetCompressionType = TParquetCompressionType.UNCOMPRESSED;
    private static final String PARQUET_DISABLE_DICTIONARY = "disable_dictionary";
    private boolean parquetDisableDictionary = false;
    private static final String PARQUET_VERSION = "version";
    private static TParquetVersion parquetVersion = TParquetVersion.PARQUET_1_0;

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

    public String getHeaderType() {
        return headerType;
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

    public List<TParquetSchema> getParquetSchemas() {
        return parquetSchemas;
    }

    public void analyze(Analyzer analyzer, List<Expr> resultExprs, List<String> colLabels) throws UserException {
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
            case "orc":
                fileFormatType = TFileFormatType.FORMAT_ORC;
                break;
            case "csv_with_names":
                headerType = FeConstants.csv_with_names;
                fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            case "csv_with_names_and_types":
                headerType = FeConstants.csv_with_names_and_types;
                fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
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
            analyzeForParquetFormat(resultExprs, colLabels);
        } else if (isOrcFormat()) {
            analyzeForOrcFormat(resultExprs, colLabels);
        }
    }

    private void genOrcSchema(List<Expr> resultExprs, List<String> colLabels) throws AnalysisException {
        Preconditions.checkState(this.parquetSchemas.isEmpty());
        for (int i = 0; i < resultExprs.size(); ++i) {
            Expr expr = resultExprs.get(i);
            String type = "";
            switch (expr.getType().getPrimitiveType()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case STRING:
                    type = expr.getType().getPrimitiveType().toString().toLowerCase();
                    break;
                case HLL:
                case BITMAP:
                    if (!(ConnectContext.get() != null && ConnectContext.get()
                            .getSessionVariable().isReturnObjectDataAsBinary())) {
                        break;
                    }
                    type = "string";
                    break;
                case DATE:
                case DATETIME:
                case DATETIMEV2:
                case DATEV2:
                case CHAR:
                case VARCHAR:
                    type = "string";
                    break;
                case DECIMALV2:
                    if (!expr.getType().isWildcardDecimal()) {
                        type = String.format("decimal(%d, %d)", ScalarType.MAX_DECIMAL128_PRECISION,
                                ((ScalarType) expr.getType()).decimalScale());
                    } else {
                        throw new AnalysisException("currently ORC writer do not support WildcardDecimal!");
                    }
                    break;
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    if (!expr.getType().isWildcardDecimal()) {
                        type = String.format("decimal(%d, %d)", ((ScalarType) expr.getType()).getPrecision(),
                                ((ScalarType) expr.getType()).decimalScale());
                    } else {
                        throw new AnalysisException("currently ORC writer do not support WildcardDecimal!");
                    }
                    break;
                default:
                    throw new AnalysisException("currently orc do not support column type: "
                            + expr.getType().getPrimitiveType());
            }
            orcSchemas.add(Pair.of(colLabels.get(i), type));
        }
    }

    private String serializeOrcSchema() {
        StringBuilder sb = new StringBuilder();
        sb.append("struct<");
        this.orcSchemas.forEach(pair -> sb.append(pair.first + ":" + pair.second + ","));
        if (!this.orcSchemas.isEmpty()) {
            return sb.substring(0, sb.length() - 1) + ">";
        } else {
            return sb.toString() + ">";
        }
    }

    private void analyzeForOrcFormat(List<Expr> resultExprs, List<String> colLabels) throws AnalysisException {
        if (this.orcSchemas.isEmpty()) {
            genOrcSchema(resultExprs, colLabels);
        }
        // check schema number
        if (resultExprs.size() != this.orcSchemas.size()) {
            throw new AnalysisException("Orc schema number does not equal to select item number");
        }
        // check type
        for (int i = 0; i < this.orcSchemas.size(); ++i) {
            Pair<String, String> schema = this.orcSchemas.get(i);
            Type resultType = resultExprs.get(i).getType();
            switch (resultType.getPrimitiveType()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case STRING:
                    if (!schema.second.equals(resultType.getPrimitiveType().toString().toLowerCase())) {
                        throw new AnalysisException("project field type is " + resultType.getPrimitiveType().toString()
                                + ", should use "  + resultType.getPrimitiveType().toString() +  ","
                                + " but the type of column " + i + " is " + schema.second);
                    }
                    break;
                case DATE:
                case DATETIME:
                case DATETIMEV2:
                case DATEV2:
                case CHAR:
                case VARCHAR:
                    if (!schema.second.equals("string")) {
                        throw new AnalysisException("project field type is " + resultType.getPrimitiveType().toString()
                                + ", should use string, but the definition type of column " + i + " is "
                                + schema.second);
                    }
                    break;
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMALV2:
                    if (!schema.second.startsWith("decimal")) {
                        throw new AnalysisException("project field type is " + resultType.getPrimitiveType().toString()
                                + ", should use string, but the definition type of column " + i + " is "
                                + schema.second);
                    }
                    break;
                case HLL:
                case BITMAP:
                    if (ConnectContext.get() != null && ConnectContext.get()
                            .getSessionVariable().isReturnObjectDataAsBinary()) {
                        if (!schema.second.equals("string")) {
                            throw new AnalysisException("project field type is HLL/BITMAP, should use string, "
                                    + "but the definition type of column " + i + " is " + schema.second);
                        }
                    } else {
                        throw new AnalysisException("Orc format does not support column type: "
                                + resultType.getPrimitiveType());
                    }
                    break;
                default:
                    throw new AnalysisException("Orc format does not support column type: "
                            + resultType.getPrimitiveType());
            }
        }
    }

    private void analyzeForParquetFormat(List<Expr> resultExprs, List<String> colLabels) throws AnalysisException {
        if (this.parquetSchemas.isEmpty()) {
            genParquetSchema(resultExprs, colLabels);
        }

        // check schema number
        if (resultExprs.size() != this.parquetSchemas.size()) {
            throw new AnalysisException("Parquet schema number does not equal to select item number");
        }

        // check type
        for (int i = 0; i < this.parquetSchemas.size(); ++i) {
            TParquetDataType type = this.parquetSchemas.get(i).schema_data_type;
            Type resultType = resultExprs.get(i).getType();
            switch (resultType.getPrimitiveType()) {
                case BOOLEAN:
                    if (!PARQUET_DATA_TYPE_MAP.get("boolean").equals(type)) {
                        throw new AnalysisException("project field type is BOOLEAN, should use boolean,"
                                + " but the type of column " + i + " is " + type);
                    }
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                    if (!PARQUET_DATA_TYPE_MAP.get("int32").equals(type)) {
                        throw new AnalysisException("project field type is TINYINT/SMALLINT/INT,"
                                + "should use int32, " + "but the definition type of column " + i + " is " + type);
                    }
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    if (!PARQUET_DATA_TYPE_MAP.get("int64").equals(type)) {
                        throw new AnalysisException("project field type is BIGINT/DATE/DATETIME,"
                                + "should use int64, but the definition type of column " + i + " is " + type);
                    }
                    break;
                case FLOAT:
                    if (!PARQUET_DATA_TYPE_MAP.get("float").equals(type)) {
                        throw new AnalysisException("project field type is FLOAT, should use float,"
                                + " but the definition type of column " + i + " is " + type);
                    }
                    break;
                case DOUBLE:
                    if (!PARQUET_DATA_TYPE_MAP.get("double").equals(type)) {
                        throw new AnalysisException("project field type is DOUBLE, should use double,"
                                + " but the definition type of column " + i + " is " + type);
                    }
                    break;
                case CHAR:
                case VARCHAR:
                case STRING:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMALV2:
                case DATETIMEV2:
                case DATEV2:
                    if (!PARQUET_DATA_TYPE_MAP.get("byte_array").equals(type)) {
                        throw new AnalysisException("project field type is CHAR/VARCHAR/STRING/DECIMAL/DATEV2"
                                + "/DATETIMEV2, should use byte_array, but the definition type of column "
                                + i + " is " + type);
                    }
                    break;
                case HLL:
                case BITMAP:
                    if (ConnectContext.get() != null && ConnectContext.get()
                            .getSessionVariable().isReturnObjectDataAsBinary()) {
                        if (!PARQUET_DATA_TYPE_MAP.get("byte_array").equals(type)) {
                            throw new AnalysisException("project field type is HLL/BITMAP, should use byte_array, "
                                    + "but the definition type of column " + i + " is " + type);
                        }
                    } else {
                        throw new AnalysisException("Parquet format does not support column type: "
                                + resultType.getPrimitiveType());
                    }
                    break;
                default:
                    throw new AnalysisException("Parquet format does not support column type: "
                            + resultType.getPrimitiveType());
            }
        }
    }

    private void genParquetSchema(List<Expr> resultExprs, List<String> colLabels) throws AnalysisException {
        Preconditions.checkState(this.parquetSchemas.isEmpty());
        for (int i = 0; i < resultExprs.size(); ++i) {
            Expr expr = resultExprs.get(i);
            TParquetSchema parquetSchema = new TParquetSchema();
            if (resultExprs.get(i).isNullable()) {
                parquetSchema.schema_repetition_type = PARQUET_REPETITION_TYPE_MAP.get("optional");
            } else {
                parquetSchema.schema_repetition_type = PARQUET_REPETITION_TYPE_MAP.get("required");
            }
            switch (expr.getType().getPrimitiveType()) {
                case BOOLEAN:
                    parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get("boolean");
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                    parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get("int32");
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get("int64");
                    break;
                case FLOAT:
                    parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get("float");
                    break;
                case DOUBLE:
                    parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get("double");
                    break;
                case CHAR:
                case VARCHAR:
                case STRING:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DATETIMEV2:
                case DATEV2:
                    parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get("byte_array");
                    break;
                case HLL:
                case BITMAP:
                    if (ConnectContext.get() != null && ConnectContext.get()
                            .getSessionVariable().isReturnObjectDataAsBinary()) {
                        parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get("byte_array");
                    }
                    break;
                default:
                    throw new AnalysisException("currently parquet do not support column type: "
                            + expr.getType().getPrimitiveType());
            }
            parquetSchema.schema_column_name = colLabels.get(i);
            parquetSchemas.add(parquetSchema);
        }
    }

    private void analyzeFilePath() throws AnalysisException {
        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }

        if (filePath.startsWith(LOCAL_FILE_PREFIX)) {
            if (!Config.enable_outfile_to_local) {
                throw new AnalysisException("Exporting results to local disk is not allowed."
                        + " To enable this feature, you need to add `enable_outfile_to_local=true`"
                        + " in fe.conf and restart FE");
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
            if (!Util.isCsvFormat(fileFormatType)) {
                throw new AnalysisException(PROP_COLUMN_SEPARATOR + " is only for CSV format");
            }
            columnSeparator = Separator.convertSeparator(properties.get(PROP_COLUMN_SEPARATOR));
            processedPropKeys.add(PROP_COLUMN_SEPARATOR);
        }

        if (properties.containsKey(PROP_LINE_DELIMITER)) {
            if (!Util.isCsvFormat(fileFormatType)) {
                throw new AnalysisException(PROP_LINE_DELIMITER + " is only for CSV format");
            }
            lineDelimiter = Separator.convertSeparator(properties.get(PROP_LINE_DELIMITER));
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

        if (this.fileFormatType == TFileFormatType.FORMAT_ORC) {
            getOrcProperties(processedPropKeys);
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
            } else if ((entry.getKey().startsWith(HADOOP_FS_PROP_PREFIX)
                    || entry.getKey().startsWith(HADOOP_PROP_PREFIX))
                    && storageType == StorageBackend.StorageType.HDFS) {
                brokerProps.put(entry.getKey(), entry.getValue());
                processedPropKeys.add(entry.getKey());
            }
        }
        if (storageType == StorageBackend.StorageType.S3) {
            S3Storage.checkS3(new CaseInsensitiveMap(brokerProps));
        } else if (storageType == StorageBackend.StorageType.HDFS) {
            HdfsStorage.checkHDFS(new CaseInsensitiveMap(brokerProps));
        }

        brokerDesc = new BrokerDesc(brokerName, storageType, brokerProps);
    }

    void setParquetCompressionType(String propertyValue) {
        if (PARQUET_COMPRESSION_TYPE_MAP.containsKey(propertyValue)) {
            this.parquetCompressionType = PARQUET_COMPRESSION_TYPE_MAP.get(propertyValue);
        } else {
            LOG.warn("not set parquet compression type or is invalid, set default to UNCOMPRESSED type.");
        }
    }

    void setParquetVersion(String propertyValue) {
        if (PARQUET_VERSION_MAP.containsKey(propertyValue)) {
            this.parquetVersion = PARQUET_VERSION_MAP.get(propertyValue);
        } else {
            LOG.warn("not set parquet version type or is invalid, set default to PARQUET_1.0 version.");
        }
    }

    /**
     * example:
     * SELECT citycode FROM table1 INTO OUTFILE "file:///root/doris/"
     * FORMAT AS PARQUET PROPERTIES ("schema"="required,int32,siteid;", "parquet.compression"="snappy");
     * <p>
     * schema: it defined the schema of parquet file, it consists of 3 field: competition type, data type, column name
     * multiple columns is split by `;`
     * <p>
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
                if (entry.getKey().substring(PARQUET_PROP_PREFIX.length()).equals(PARQUET_COMPRESSION)) {
                    setParquetCompressionType(entry.getValue());
                } else if (entry.getKey().substring(PARQUET_PROP_PREFIX.length()).equals(PARQUET_DISABLE_DICTIONARY)) {
                    this.parquetDisableDictionary = Boolean.valueOf(entry.getValue());
                } else if (entry.getKey().substring(PARQUET_PROP_PREFIX.length()).equals(PARQUET_VERSION)) {
                    setParquetVersion(entry.getValue());
                }
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
            if (!PARQUET_REPETITION_TYPE_MAP.containsKey(properties[0])) {
                throw new AnalysisException("unknown repetition type");
            }
            if (!properties[0].equalsIgnoreCase("required")) {
                throw new AnalysisException("currently only support required type");
            }
            if (!PARQUET_DATA_TYPE_MAP.containsKey(properties[1])) {
                throw new AnalysisException("data type is not supported:" + properties[1]);
            }
            TParquetSchema parquetSchema = new TParquetSchema();
            parquetSchema.schema_repetition_type = PARQUET_REPETITION_TYPE_MAP.get(properties[0]);
            parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get(properties[1]);
            parquetSchema.schema_column_name = properties[2];
            parquetSchemas.add(parquetSchema);
        }
        processedPropKeys.add(SCHEMA);
    }

    private void getOrcProperties(Set<String> processedPropKeys) throws AnalysisException {
        // check schema. if schema is not set, Doris will gen schema by select items
        String schema = properties.get(SCHEMA);
        if (schema == null) {
            return;
        }
        if (schema.isEmpty()) {
            throw new AnalysisException("Orc schema property should not be empty");
        }
        schema = schema.replace(" ", "");
        schema = schema.toLowerCase();
        String[] schemas = schema.split(";");
        for (String item : schemas) {
            String[] properties = item.split(",");
            if (properties.length != 2) {
                throw new AnalysisException("must only contains type and column name");
            }
            if (!ORC_DATA_TYPE.contains(properties[1]) && !properties[1].startsWith("decimal")) {
                throw new AnalysisException("data type is not supported:" + properties[1]);
            } else if (!ORC_DATA_TYPE.contains(properties[1]) && properties[1].startsWith("decimal")) {
                String errorMsg = "Format of decimal type must be decimal(%d,%d)";
                String precisionAndScale = properties[1].substring(0, "decimal".length()).trim();
                if (!precisionAndScale.startsWith("(") || !precisionAndScale.endsWith(")")) {
                    throw new AnalysisException(errorMsg);
                }
                String[] str = precisionAndScale.substring(1, precisionAndScale.length() - 1).split(",");
                if (str.length != 2) {
                    throw new AnalysisException(errorMsg);
                }
            }
            orcSchemas.add(Pair.of(properties[0], properties[1]));
        }
        processedPropKeys.add(SCHEMA);
    }

    private boolean isParquetFormat() {
        return fileFormatType == TFileFormatType.FORMAT_PARQUET;
    }

    private boolean isOrcFormat() {
        return fileFormatType == TFileFormatType.FORMAT_ORC;
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

    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append(" INTO OUTFILE '").append(" ? ").append(" FORMAT AS ").append(" ? ");
        if (properties != null && !properties.isEmpty()) {
            sb.append(" PROPERTIES(").append(" ? ").append(")");
        }
        return sb.toString();
    }

    public TResultFileSinkOptions toSinkOptions() {
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions(filePath, fileFormatType);
        if (Util.isCsvFormat(fileFormatType)) {
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
            sinkOptions.setParquetCompressionType(parquetCompressionType);
            sinkOptions.setParquetDisableDictionary(parquetDisableDictionary);
            sinkOptions.setParquetVersion(parquetVersion);
            sinkOptions.setParquetSchemas(parquetSchemas);
        }
        if (isOrcFormat()) {
            sinkOptions.setOrcSchema(serializeOrcSchema());
        }
        return sinkOptions;
    }
}


