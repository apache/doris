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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.HdfsPropertiesUtils;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TParquetDataType;
import org.apache.doris.thrift.TParquetRepetitionType;
import org.apache.doris.thrift.TParquetSchema;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// For syntax select * from tbl INTO OUTFILE xxxx
public class OutFileClause {
    private static final Logger LOG = LogManager.getLogger(OutFileClause.class);

    public static final List<String> RESULT_COL_NAMES = Lists.newArrayList();
    public static final List<Type> RESULT_COL_TYPES = Lists.newArrayList();
    public static final Map<String, TParquetRepetitionType> PARQUET_REPETITION_TYPE_MAP = Maps.newHashMap();
    public static final Map<String, TParquetDataType> PARQUET_DATA_TYPE_MAP = Maps.newHashMap();
    public static final Set<String> ORC_DATA_TYPE = Sets.newHashSet();
    public static final String FILE_NUMBER = "FileNumber";
    public static final String TOTAL_ROWS = "TotalRows";
    public static final String FILE_SIZE = "FileSize";
    public static final String URL = "URL";
    public static final String WRITE_TIME_SEC = "WriteTimeSec";
    public static final String WRITE_SPEED_KB = "WriteSpeedKB";

    static {
        RESULT_COL_NAMES.add(FILE_NUMBER);
        RESULT_COL_NAMES.add(TOTAL_ROWS);
        RESULT_COL_NAMES.add(FILE_SIZE);
        RESULT_COL_NAMES.add(URL);
        RESULT_COL_NAMES.add(WRITE_TIME_SEC);
        RESULT_COL_NAMES.add(WRITE_SPEED_KB);

        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.INT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.VARCHAR));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.VARCHAR));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.VARCHAR));

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
    private static final String HDFS_FILE_PREFIX = "hdfs://";
    private static final String BROKER_PROP_PREFIX = "broker.";
    public static final String PROP_BROKER_NAME = "broker.name";
    public static final String PROP_COLUMN_SEPARATOR = "column_separator";
    public static final String PROP_LINE_DELIMITER = "line_delimiter";
    public static final String PROP_MAX_FILE_SIZE = "max_file_size";
    private static final String PROP_SUCCESS_FILE_NAME = "success_file_name";
    public static final String PROP_DELETE_EXISTING_FILES = "delete_existing_files";
    public static final String PROP_FILE_SUFFIX = "file_suffix";
    public static final String PROP_WITH_BOM = "with_bom";

    private static final String SCHEMA = "schema";

    private static final long DEFAULT_MAX_FILE_SIZE_BYTES = 1 * 1024 * 1024 * 1024; // 1GB
    private static final long MIN_FILE_SIZE_BYTES = 5 * 1024 * 1024L; // 5MB

    private String filePath;
    private Map<String, String> properties;

    private long maxFileSizeBytes = DEFAULT_MAX_FILE_SIZE_BYTES;
    private boolean deleteExistingFiles = false;
    private String fileSuffix = "";
    private boolean withBom = false;
    private BrokerDesc brokerDesc = null;
    // True if result is written to local disk.
    // If set to true, the brokerDesc must be null.
    private boolean isLocalOutput = false;
    private String successFileName = "";

    private List<TParquetSchema> parquetSchemas = new ArrayList<>();

    private List<Pair<String, String>> orcSchemas = new ArrayList<>();

    private boolean isAnalyzed = false;

    private FileFormatProperties fileFormatProperties;

    public OutFileClause(String filePath, String format, Map<String, String> properties) {
        this.filePath = filePath;
        this.properties = properties;
        this.isAnalyzed = false;
        if (Strings.isNullOrEmpty(format)) {
            fileFormatProperties = FileFormatProperties.createFileFormatProperties("csv");
        } else {
            fileFormatProperties = FileFormatProperties.createFileFormatProperties(format.toLowerCase());
        }
    }

    public OutFileClause(OutFileClause other) {
        this.filePath = other.filePath;
        this.fileFormatProperties = other.fileFormatProperties;
        this.properties = other.properties == null ? null : Maps.newHashMap(other.properties);
        this.isAnalyzed = other.isAnalyzed;
    }

    public String getColumnSeparator() {
        return ((CsvFileFormatProperties) fileFormatProperties).getColumnSeparator();
    }

    public String getLineDelimiter() {
        return ((CsvFileFormatProperties) fileFormatProperties).getLineDelimiter();
    }

    public String getHeaderType() {
        return ((CsvFileFormatProperties) fileFormatProperties).getHeaderType();
    }

    public TFileFormatType getFileFormatType() {
        return fileFormatProperties.getFileFormatType();
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

        analyzeProperties();

        if (brokerDesc != null && isLocalOutput) {
            throw new AnalysisException("No need to specify BROKER properties in OUTFILE clause for local file output");
        } else if (brokerDesc == null && !isLocalOutput) {
            throw new AnalysisException("Please specify BROKER properties or check your local file path.");
        }
        isAnalyzed = true;

        if (isParquetFormat()) {
            analyzeForParquetFormat(resultExprs, colLabels);
        } else if (isOrcFormat()) {
            analyzeForOrcFormat(resultExprs, colLabels);
        }
    }

    private void genOrcSchema(List<Expr> resultExprs, List<String> colLabels) throws AnalysisException {
        Preconditions.checkState(this.orcSchemas.isEmpty());
        for (int i = 0; i < resultExprs.size(); ++i) {
            Expr expr = resultExprs.get(i);
            String type = dorisTypeToOrcTypeMap(expr.getType());
            orcSchemas.add(Pair.of(colLabels.get(i), type));
        }
    }

    private String dorisTypeToOrcTypeMap(Type dorisType) throws AnalysisException {
        String orcType = "";
        switch (dorisType.getPrimitiveType()) {
            case NULL_TYPE:
                orcType = "tinyint";
                break;
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case STRING:
                orcType = dorisType.getPrimitiveType().toString().toLowerCase();
                break;
            case HLL:
            case BITMAP:
            case QUANTILE_STATE:
                orcType = "binary";
                break;
            case DATEV2:
                orcType = "date";
                break;
            case DATETIMEV2:
                orcType = "timestamp";
                break;
            case CHAR:
                orcType = "char(" + dorisType.getLength() + ")";
                break;
            case VARCHAR:
                orcType = "varchar(" + dorisType.getLength() + ")";
                break;
            case IPV4:
                orcType = "int";
                break;
            case LARGEINT:
            case DATE:
            case DATETIME:
            case IPV6:
            case VARIANT:
            case JSONB:
                orcType = "string";
                break;
            case DECIMALV2:
                if (!dorisType.isWildcardDecimal()) {
                    orcType = String.format("decimal(%d, 9)", ScalarType.MAX_DECIMAL128_PRECISION);
                } else {
                    throw new AnalysisException("currently ORC writer do not support WildcardDecimal!");
                }
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                if (!dorisType.isWildcardDecimal()) {
                    orcType = String.format("decimal(%d, %d)", ((ScalarType) dorisType).getPrecision(),
                            ((ScalarType) dorisType).decimalScale());
                } else {
                    throw new AnalysisException("currently ORC outfile do not support WildcardDecimal!");
                }
                break;
            case STRUCT: {
                StructType structType = (StructType) dorisType;

                StringBuilder sb = new StringBuilder();
                sb.append("struct<");
                for (int i = 0; i < structType.getFields().size(); ++i) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    StructField field = structType.getFields().get(i);
                    sb.append(field.getName())
                            .append(":")
                            .append(dorisTypeToOrcTypeMap(field.getType()));
                }
                sb.append(">");
                orcType = sb.toString();
                break;
            }
            case MAP: {
                MapType mapType = (MapType) dorisType;
                StringBuilder sb = new StringBuilder();
                sb.append("map<")
                        .append(dorisTypeToOrcTypeMap(mapType.getKeyType()))
                        .append(",")
                        .append(dorisTypeToOrcTypeMap(mapType.getValueType()));
                sb.append(">");
                orcType = sb.toString();
                break;
            }
            case ARRAY: {
                StringBuilder sb = new StringBuilder();
                ArrayType arrayType = (ArrayType) dorisType;
                sb.append("array<")
                        .append(dorisTypeToOrcTypeMap(arrayType.getItemType()))
                        .append(">");
                orcType = sb.toString();
                break;
            }
            default:
                throw new AnalysisException("currently orc do not support column type: "
                        + dorisType.getPrimitiveType());
        }
        return orcType;
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
                case NULL_TYPE:
                    checkOrcType(schema.second, "tinyint", true, resultType.getPrimitiveType().toString());
                    break;
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case STRING:
                    checkOrcType(schema.second, resultType.getPrimitiveType().toString().toLowerCase(), true,
                            resultType.getPrimitiveType().toString());
                    break;
                case DATEV2:
                    checkOrcType(schema.second, "date", true, resultType.getPrimitiveType().toString());
                    break;
                case DATETIMEV2:
                    checkOrcType(schema.second, "timestamp", true, resultType.getPrimitiveType().toString());
                    break;
                case CHAR:
                    checkOrcType(schema.second, "char", false, resultType.getPrimitiveType().toString());
                    break;
                case VARCHAR:
                    checkOrcType(schema.second, "varchar", false, resultType.getPrimitiveType().toString());
                    break;
                case IPV4:
                    checkOrcType(schema.second, "int", false, resultType.getPrimitiveType().toString());
                    break;
                case LARGEINT:
                case DATE:
                case DATETIME:
                case IPV6:
                case VARIANT:
                case JSONB:
                    checkOrcType(schema.second, "string", true, resultType.getPrimitiveType().toString());
                    break;
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMALV2:
                    checkOrcType(schema.second, "decimal", false, resultType.getPrimitiveType().toString());
                    break;
                case HLL:
                case BITMAP:
                case QUANTILE_STATE:
                    checkOrcType(schema.second, "binary", true, resultType.getPrimitiveType().toString());
                    break;
                case STRUCT:
                    checkOrcType(schema.second, "struct", false, resultType.getPrimitiveType().toString());
                    break;
                case MAP:
                    checkOrcType(schema.second, "map", false, resultType.getPrimitiveType().toString());
                    break;
                case ARRAY:
                    checkOrcType(schema.second, "array", false, resultType.getPrimitiveType().toString());
                    break;
                default:
                    throw new AnalysisException("Orc format does not support column type: "
                            + resultType.getPrimitiveType());
            }
        }
    }

    private void checkOrcType(String orcType, String expectType, boolean isEqual, String dorisType)
            throws AnalysisException {
        if (isEqual) {
            if (orcType.equals(expectType)) {
                return;
            }
        } else {
            if (orcType.startsWith(expectType)) {
                return;
            }
        }
        throw new AnalysisException("project field type is " + dorisType
                + ", should use " + expectType + ", but the definition type is " + orcType);
    }


    private void analyzeForParquetFormat(List<Expr> resultExprs, List<String> colLabels) throws AnalysisException {
        if (this.parquetSchemas.isEmpty()) {
            genParquetColumnName(resultExprs, colLabels);
        }
        // check schema number
        if (resultExprs.size() != this.parquetSchemas.size()) {
            throw new AnalysisException("Parquet schema number does not equal to select item number");
        }
    }

    private void genParquetColumnName(List<Expr> resultExprs, List<String> colLabels) throws AnalysisException {
        for (int i = 0; i < resultExprs.size(); ++i) {
            TParquetSchema parquetSchema = new TParquetSchema();
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
        if (properties != null) {
            String namePrefix = properties.containsKey(PROP_BROKER_NAME)
                    ? BROKER_PROP_PREFIX + HdfsResource.DSF_NAMESERVICES : HdfsResource.DSF_NAMESERVICES;
            String dfsNameServices = properties.getOrDefault(namePrefix, "");
            if (!Strings.isNullOrEmpty(dfsNameServices) && !filePath.contains(dfsNameServices)) {
                filePath = filePath.replace(HDFS_FILE_PREFIX, HDFS_FILE_PREFIX + dfsNameServices);
            }
        }
        // delete repeated '/'
        try {
            filePath = new URI(filePath).normalize().toString();
        } catch (URISyntaxException e) {
            throw new AnalysisException("Can not normalize the URI, error: " + e.getMessage());
        }
        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }
    }

    private void analyzeProperties() throws UserException {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        // Copy the properties, because we will remove the key from properties.
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(properties);

        analyzeBrokerDesc(copiedProps);

        fileFormatProperties.analyzeFileFormatProperties(copiedProps, true);
        // check if compression type for csv is supported
        if (fileFormatProperties instanceof CsvFileFormatProperties) {
            CsvFileFormatProperties csvFileFormatProperties = (CsvFileFormatProperties) fileFormatProperties;
            csvFileFormatProperties.checkSupportedCompressionType(true);
        }

        if (copiedProps.containsKey(PROP_MAX_FILE_SIZE)) {
            maxFileSizeBytes = ParseUtil.analyzeDataVolume(copiedProps.get(PROP_MAX_FILE_SIZE));
            if (maxFileSizeBytes < MIN_FILE_SIZE_BYTES) {
                throw new AnalysisException("max file size should larger than 5MB. Given: " + maxFileSizeBytes);
            }
            copiedProps.remove(PROP_MAX_FILE_SIZE);
        }

        if (copiedProps.containsKey(PROP_DELETE_EXISTING_FILES)) {
            deleteExistingFiles = Boolean.parseBoolean(copiedProps.get(PROP_DELETE_EXISTING_FILES))
                    & Config.enable_delete_existing_files;
            copiedProps.remove(PROP_DELETE_EXISTING_FILES);
        }

        if (copiedProps.containsKey(PROP_FILE_SUFFIX)) {
            fileSuffix = copiedProps.get(PROP_FILE_SUFFIX);
            copiedProps.remove(PROP_FILE_SUFFIX);
        }

        if (copiedProps.containsKey(PROP_WITH_BOM)) {
            withBom = Boolean.valueOf(copiedProps.get(PROP_WITH_BOM)).booleanValue();
            copiedProps.remove(PROP_WITH_BOM);
        }

        if (copiedProps.containsKey(PROP_SUCCESS_FILE_NAME)) {
            successFileName = copiedProps.get(PROP_SUCCESS_FILE_NAME);
            FeNameFormat.checkOutfileSuccessFileName("file name", successFileName);
            copiedProps.remove(PROP_SUCCESS_FILE_NAME);
        }

        if (fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_PARQUET) {
            getParquetProperties(copiedProps);
        }

        if (fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_ORC) {
            getOrcProperties(copiedProps);
        }
    }

    /**
     * The following two situations will generate the corresponding @brokerDesc:
     * 1. broker: with broker name
     * 2. s3: with s3 pattern path, without broker name
     */
    private void analyzeBrokerDesc(Map<String, String> copiedProps) throws UserException {
        /**
         * If the output is intended to be written to the local file system, skip BrokerDesc analysis.
         * This is because Broker properties are not required when writing files locally,
         * and the upper layer logic ensures that brokerDesc must be null in this case.
         */
        if (isLocalOutput) {
            return;
        }
        String brokerName = copiedProps.get(PROP_BROKER_NAME);
        brokerDesc = new BrokerDesc(brokerName, copiedProps);
        /*
         * Note on HDFS export behavior and URI handling:
         *
         * 1. Currently, URI extraction from user input supports case-insensitive key matching
         *    (e.g., "URI", "Uri", "uRI", etc.), to tolerate non-standard input from users.
         *
         * 2. In OUTFILE scenarios, if FE  fails to pass 'fs.defaultFS' to the BE ,
         *    it may lead to data export failure *without* triggering an actual error (appears as success),
         *    which is misleading and can cause silent data loss or inconsistencies.
         *
         * 3. As a temporary safeguard, the following logic forcibly extracts the default FS from the provided
         *    file path and injects it into the broker descriptor config:
         *
         *      if (brokerDesc.getStorageType() == HDFS) {
         *          extract default FS from file path
         *          and put into BE config as 'fs.defaultFS'
         *      }
         *
         * 4. Long-term solution: We should define and enforce a consistent parameter specification
         *    across all user-facing entry points (including FE input validation, broker desc normalization, etc.),
         *    to prevent missing critical configs like 'fs.defaultFS'.
         *
         * 5. Suggested improvements:
         *    - Normalize all parameter keys (e.g., to lowercase)
         *    - Centralize HDFS URI parsing logic
         *    - Add validation in FE to reject incomplete or malformed configs
         */
        if (null != brokerDesc.getStorageType() && brokerDesc.getStorageType()
                .equals(StorageBackend.StorageType.HDFS)) {
            String defaultFs = HdfsPropertiesUtils.extractDefaultFsFromPath(filePath);
            brokerDesc.getBackendConfigProperties().put(HdfsProperties.HDFS_DEFAULT_FS_NAME, defaultFs);
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
    private void getParquetProperties(Map<String, String> copiedProps) throws AnalysisException {
        // check schema. if schema is not set, Doris will gen schema by select items
        // Note: These codes are useless and outdated.
        String schema = copiedProps.get(SCHEMA);
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
            String[] fields = item.split(",");
            if (fields.length != 3) {
                throw new AnalysisException("must only contains repetition type/column type/column name");
            }
            if (!PARQUET_REPETITION_TYPE_MAP.containsKey(fields[0])) {
                throw new AnalysisException("unknown repetition type");
            }
            if (!fields[0].equalsIgnoreCase("required")) {
                throw new AnalysisException("currently only support required type");
            }
            if (!PARQUET_DATA_TYPE_MAP.containsKey(fields[1])) {
                throw new AnalysisException("data type is not supported:" + fields[1]);
            }
            TParquetSchema parquetSchema = new TParquetSchema();
            parquetSchema.schema_repetition_type = PARQUET_REPETITION_TYPE_MAP.get(fields[0]);
            parquetSchema.schema_data_type = PARQUET_DATA_TYPE_MAP.get(fields[1]);
            parquetSchema.schema_column_name = fields[2];
            parquetSchemas.add(parquetSchema);
        }
        copiedProps.remove(SCHEMA);
    }

    private void getOrcProperties(Map<String, String> copiedProps) throws AnalysisException {
        // check schema. if schema is not set, Doris will gen schema by select items
        String schema = copiedProps.get(SCHEMA);
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
            String[] fields = item.split(",");
            if (fields.length != 2) {
                throw new AnalysisException("must only contains type and column name");
            }
            if (!ORC_DATA_TYPE.contains(fields[1]) && !fields[1].startsWith("decimal")) {
                throw new AnalysisException("data type is not supported:" + fields[1]);
            } else if (!ORC_DATA_TYPE.contains(fields[1]) && fields[1].startsWith("decimal")) {
                String errorMsg = "Format of decimal type must be decimal(%d,%d)";
                String precisionAndScale = fields[1].substring(0, "decimal".length()).trim();
                if (!precisionAndScale.startsWith("(") || !precisionAndScale.endsWith(")")) {
                    throw new AnalysisException(errorMsg);
                }
                String[] str = precisionAndScale.substring(1, precisionAndScale.length() - 1).split(",");
                if (str.length != 2) {
                    throw new AnalysisException(errorMsg);
                }
            }
            orcSchemas.add(Pair.of(fields[0], fields[1]));
        }
        copiedProps.remove(SCHEMA);
    }

    private boolean isParquetFormat() {
        return fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_PARQUET;
    }

    private boolean isOrcFormat() {
        return fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_ORC;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getSuccessFileName() {
        return successFileName;
    }

    @Override
    public OutFileClause clone() {
        return new OutFileClause(this);
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(" INTO OUTFILE '").append(filePath).append(" FORMAT AS ")
                .append(fileFormatProperties.getFormatName());
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
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions(filePath,
                fileFormatProperties.getFileFormatType());
        fileFormatProperties.fullTResultFileSinkOptions(sinkOptions);

        sinkOptions.setMaxFileSizeBytes(maxFileSizeBytes);
        sinkOptions.setDeleteExistingFiles(deleteExistingFiles);
        sinkOptions.setFileSuffix(fileSuffix);
        sinkOptions.setWithBom(withBom);

        if (brokerDesc != null) {
            sinkOptions.setBrokerProperties(brokerDesc.getBackendConfigProperties());
            // broker_addresses of sinkOptions will be set in Coordinator.
            // Because we need to choose the nearest broker with the result sink node.
        }
        if (!Strings.isNullOrEmpty(successFileName)) {
            sinkOptions.setSuccessFileName(successFileName);
        }
        if (isParquetFormat()) {
            sinkOptions.setParquetSchemas(parquetSchemas);
        }
        if (isOrcFormat()) {
            sinkOptions.setOrcSchema(serializeOrcSchema());
        }
        return sinkOptions;
    }
}
