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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StreamLoadStmt implements LoadTaskInfo {

    public static class Property {
        public static final String COLUMNS = "columns";
        public static final String EXEC_MEM_LIMIT = "exec_mem_limit";
        public static final String MERGE_TYPE = "merge_type";
        public static final String TIMEOUT = "timeout";
        public static final String TIMEZONE = "timezone";
        public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";
        public static final String WHERE = "where";
        public static final String COLUMN_SEPARATOR = "column_separator";
        public static final String LINE_DELIMITER = "line_delimiter";
        public static final String PARTITIONS = "partitions";
        public static final String TEMP_PARTITIONS = "temporary_partitions";
        public static final String NEGATIVE = "negative";
        public static final String STRICT_MODE = "strict_mode";
        public static final String JSONPATHS = "jsonpaths";
        public static final String JSONROOT = "json_root";
        public static final String STRIP_OUTER_ARRAY = "strip_outer_array";
        public static final String NUM_AS_STRING = "num_as_string";
        public static final String FUZZY_PARSE = "fuzzy_parse";
        public static final String READ_JSON_BY_LINE = "read_json_by_line";
        public static final String DELETE_CONDITION = "delete";
        public static final String MAX_FILTER_RATIO = "max_filter_ratio";
        public static final String FUNCTION_COLUMN = "function_column";
        public static final String SEQUENCE_COL = "sequence_col";
        public static final String LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";
        public static final String HIDDEN_COLUMNS = "hidden_columns";
        public static final String TRIM_DOUBLE_QUOTES = "trim_double_quotes";
        public static final String SKIP_LINES = "skip_lines";
        public static final String ENABLE_PROFILE = "enable_profile";
        public static final String PARTIAL_COLUMNS = "partial_columns";
    }

    private static Map<String, String> properties;

    private static final Logger LOG = LogManager.getLogger(StreamLoadStmt.class);

    private TUniqueId id;
    private long txnId;
    private TFileType fileType;
    private TFileFormatType formatType;
    private TFileCompressType compressType = TFileCompressType.UNKNOWN;
    private boolean stripOuterArray;
    private boolean numAsString;
    private String jsonPaths;
    private String jsonRoot;
    private boolean fuzzyParse;
    private boolean readJsonByLine;

    // optional
    private ImportColumnDescs columnExprDescs = new ImportColumnDescs();
    private Expr whereExpr;
    private Separator columnSeparator;
    private Separator lineDelimiter;
    private PartitionNames partitions;
    private String path;
    private long fileSize = 0;
    private boolean negative;
    private boolean strictMode = false; // default is false
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int timeout = Config.stream_load_default_timeout_second;
    private long execMemLimit = 2 * 1024 * 1024 * 1024L; // default is 2GB
    private LoadTask.MergeType mergeType = LoadTask.MergeType.APPEND; // default is all data is load no delete
    private Expr deleteCondition;
    private String sequenceCol;
    private int sendBatchParallelism = 1;
    private double maxFilterRatio = 0.0;
    private boolean loadToSingleTablet = false;
    private String headerType = "";
    private List<String> hiddenColumns;
    private boolean trimDoubleQuotes = false;
    private boolean isPartialUpdate = false;

    private int skipLines = 0;
    private boolean enableProfile = false;

    public StreamLoadStmt(TUniqueId id, long txnId, TFileType fileType, TFileFormatType formatType,
            TFileCompressType compressType) {
        this.id = id;
        this.txnId = txnId;
        this.fileType = fileType;
        this.formatType = formatType;
        this.compressType = compressType;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
        this.numAsString = false;
        this.fuzzyParse = false;
        this.readJsonByLine = false;
    }

    public TUniqueId getId() {
        return id;
    }

    public long getTxnId() {
        return txnId;
    }

    public TFileType getFileType() {
        return fileType;
    }

    public TFileFormatType getFormatType() {
        return formatType;
    }

    public TFileCompressType getCompressType() {
        return compressType;
    }

    public ImportColumnDescs getColumnExprDescs() {
        return columnExprDescs;
    }

    public Expr getPrecedingFilter() {
        return null;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public Separator getColumnSeparator() {
        return columnSeparator;
    }

    public String getHeaderType() {
        return headerType;
    }

    public Separator getLineDelimiter() {
        return lineDelimiter;
    }

    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    public boolean isLoadToSingleTablet() {
        return loadToSingleTablet;
    }

    public PartitionNames getPartitions() {
        return partitions;
    }

    public String getPath() {
        return path;
    }

    public long getFileSize() {
        return fileSize;
    }

    public boolean getNegative() {
        return negative;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public String getTimezone() {
        return timezone;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public boolean isNumAsString() {
        return numAsString;
    }

    public boolean isReadJsonByLine() {
        return readJsonByLine;
    }

    public boolean isFuzzyParse() {
        return fuzzyParse;
    }

    public void setFuzzyParse(boolean fuzzyParse) {
        this.fuzzyParse = fuzzyParse;
    }

    public void setStripOuterArray(boolean stripOuterArray) {
        this.stripOuterArray = stripOuterArray;
    }

    public void setNumAsString(boolean numAsString) {
        this.numAsString = numAsString;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public void setJsonPath(String jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public void setJsonRoot(String jsonRoot) {
        this.jsonRoot = jsonRoot;
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    public Expr getDeleteCondition() {
        return deleteCondition;
    }

    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceCol);
    }

    public String getSequenceCol() {
        return sequenceCol;
    }

    public List<String> getHiddenColumns() {
        return hiddenColumns;
    }

    public boolean getTrimDoubleQuotes() {
        return trimDoubleQuotes;
    }

    public int getSkipLines() {
        return skipLines;
    }

    public boolean getEnableProfile() {
        return enableProfile;
    }

    public boolean isPartialUpdate() {
        return isPartialUpdate;
    }

    public static StreamLoadStmt fromTStreamLoadPutRequest(TStreamLoadPutRequest request) throws UserException {
        StreamLoadStmt streamLoadStmt = new StreamLoadStmt(request.getLoadId(), request.getTxnId(),
                request.getFileType(), request.getFormatType(),
                request.getCompressType());
        StreamLoadStmt.properties = request.getProperties();
        streamLoadStmt.setOptionalFromTSLPutRequest(request);
        if (request.isSetFileSize()) {
            streamLoadStmt.fileSize = request.getFileSize();
        }
        return streamLoadStmt;
    }

    private void setOptionalFromTSLPutRequest(TStreamLoadPutRequest request) throws UserException {
        if (Strings.isNullOrEmpty(properties.get(Property.COLUMNS))) {
            setColumnToColumnExpr(properties.get(Property.COLUMNS));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.WHERE))) {
            whereExpr = parseWhereExpr(properties.get(Property.WHERE));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.COLUMN_SEPARATOR))) {
            setColumnSeparator(properties.get(Property.COLUMN_SEPARATOR));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.LINE_DELIMITER))) {
            setLineDelimiter(properties.get(Property.LINE_DELIMITER));
        }
        if (request.isSetHeaderType()) {
            headerType = request.getHeaderType();
        }
        if (Strings.isNullOrEmpty(properties.get(Property.PARTITIONS))) {
            String[] partNames = properties.get(Property.PARTITIONS).trim().split("\\s*,\\s*");
            if (properties.get(Property.TEMP_PARTITIONS) != null) {
                partitions = new PartitionNames(true, Lists.newArrayList(partNames));
            } else {
                partitions = new PartitionNames(false, Lists.newArrayList(partNames));
            }
        }
        switch (request.getFileType()) {
            case FILE_STREAM:
                // fall through to case FILE_LOCAL
            case FILE_LOCAL:
                path = request.getPath();
                break;
            default:
                throw new UserException("unsupported file type, type=" + request.getFileType());
        }
        if (Strings.isNullOrEmpty(properties.get(Property.NEGATIVE))) {
            negative = Boolean.parseBoolean(properties.get(Property.NEGATIVE));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.TIMEOUT))) {
            timeout = Integer.parseInt(properties.get(Property.TIMEOUT));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.STRICT_MODE))) {
            strictMode = Boolean.parseBoolean(properties.get(Property.STRICT_MODE));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.TIMEZONE))) {
            timezone = TimeUtils.checkTimeZoneValidAndStandardize(properties.get(Property.TIMEZONE));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.EXEC_MEM_LIMIT))) {
            execMemLimit = Long.parseLong(properties.get(Property.EXEC_MEM_LIMIT));
        }
        if (request.getFormatType() == TFileFormatType.FORMAT_JSON) {
            if (properties.get(Property.JSONPATHS) != null) {
                jsonPaths = properties.get(Property.JSONPATHS);
            }
            if (properties.get(Property.JSONROOT) != null) {
                jsonRoot = properties.get(Property.JSONROOT);
            }
            stripOuterArray = Boolean.parseBoolean(properties.get(Property.STRIP_OUTER_ARRAY));
            numAsString = Boolean.parseBoolean(properties.get(Property.NUM_AS_STRING));
            fuzzyParse = Boolean.parseBoolean(properties.get(Property.FUZZY_PARSE));
            readJsonByLine = Boolean.parseBoolean(properties.get(Property.READ_JSON_BY_LINE));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.MERGE_TYPE))) {
            try {
                mergeType = LoadTask.MergeType.valueOf(properties.get(Property.MERGE_TYPE).toString());
            } catch (IllegalArgumentException e) {
                throw new UserException("unknown merge type " + properties.get(Property.MERGE_TYPE).toString());
            }
        }
        if (Strings.isNullOrEmpty(properties.get(Property.DELETE_CONDITION))) {
            deleteCondition = parseWhereExpr(properties.get(Property.DELETE_CONDITION));
        }
        if (negative && mergeType != LoadTask.MergeType.APPEND) {
            throw new AnalysisException("Negative is only used when merge type is APPEND.");
        }
        if (Strings.isNullOrEmpty(properties.get(Property.FUNCTION_COLUMN + "." + Property.SEQUENCE_COL))) {
            sequenceCol = properties.get(Property.FUNCTION_COLUMN + "." + Property.SEQUENCE_COL);
        }
        if (Strings.isNullOrEmpty(properties.get(Property.SEND_BATCH_PARALLELISM))) {
            sendBatchParallelism = Integer.parseInt(properties.get(Property.SEND_BATCH_PARALLELISM));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.MAX_FILTER_RATIO))) {
            maxFilterRatio = Double.parseDouble(properties.get(Property.MAX_FILTER_RATIO));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.LOAD_TO_SINGLE_TABLET))) {
            loadToSingleTablet = Boolean.parseBoolean(properties.get(Property.LOAD_TO_SINGLE_TABLET));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.HIDDEN_COLUMNS))) {
            hiddenColumns = Arrays.asList(properties.get(Property.HIDDEN_COLUMNS).replaceAll("\\s+", "").split(","));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.TRIM_DOUBLE_QUOTES))) {
            trimDoubleQuotes = Boolean.parseBoolean(properties.get(Property.TRIM_DOUBLE_QUOTES));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.SKIP_LINES))) {
            skipLines = Integer.parseInt(properties.get(Property.SKIP_LINES));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.ENABLE_PROFILE))) {
            enableProfile = Boolean.parseBoolean(properties.get(Property.ENABLE_PROFILE));
        }
        if (Strings.isNullOrEmpty(properties.get(Property.PARTIAL_COLUMNS))) {
            isPartialUpdate = Boolean.parseBoolean(properties.get(Property.PARTIAL_COLUMNS));
        }
    }

    // used for stream load
    private void setColumnToColumnExpr(String columns) throws UserException {
        String columnsSQL = new String("COLUMNS (" + columns + ")");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(columnsSQL)));
        ImportColumnsStmt columnsStmt;
        try {
            columnsStmt = (ImportColumnsStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            LOG.warn("error happens when parsing columns, sql={}", columnsSQL, e);
            throw new AnalysisException("failed to parsing columns' header, maybe contain unsupported character");
        } catch (AnalysisException e) {
            LOG.warn("analyze columns' statement failed, sql={}, error={}",
                    columnsSQL, parser.getErrorMsg(columnsSQL), e);
            String errorMessage = parser.getErrorMsg(columnsSQL);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (Exception e) {
            LOG.warn("failed to parse columns header, sql={}", columnsSQL, e);
            throw new UserException("parse columns header failed", e);
        }

        if (columnsStmt.getColumns() != null && !columnsStmt.getColumns().isEmpty()) {
            columnExprDescs.descs = columnsStmt.getColumns();
        }
    }

    private Expr parseWhereExpr(String whereString) throws UserException {
        String whereSQL = new String("WHERE " + whereString);
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(whereSQL)));
        ImportWhereStmt whereStmt;
        try {
            whereStmt = (ImportWhereStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            LOG.warn("error happens when parsing where header, sql={}", whereSQL, e);
            throw new AnalysisException("failed to parsing where header, maybe contain unsupported character");
        } catch (AnalysisException e) {
            LOG.warn("analyze where statement failed, sql={}, error={}",
                    whereSQL, parser.getErrorMsg(whereSQL), e);
            String errorMessage = parser.getErrorMsg(whereSQL);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (Exception e) {
            LOG.warn("failed to parse where header, sql={}", whereSQL, e);
            throw new UserException("parse columns header failed", e);
        }
        return whereStmt.getExpr();
    }

    private void setColumnSeparator(String oriSeparator) throws AnalysisException {
        columnSeparator = new Separator(oriSeparator);
        columnSeparator.analyze();
    }

    private void setLineDelimiter(String oriLineDelimiter) throws AnalysisException {
        lineDelimiter = new Separator(oriLineDelimiter);
        lineDelimiter.analyze();
    }

    public long getMemLimit() {
        return execMemLimit;
    }

    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }
}
