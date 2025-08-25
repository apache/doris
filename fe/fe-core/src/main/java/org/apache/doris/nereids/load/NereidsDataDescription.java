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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

// used to describe data info which is needed to import.
//
//      data_desc:
//          DATA INFILE ('file_path', ...)
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [COLUMNS TERMINATED BY separator]
//          [FORMAT AS format]
//          [(tmp_col1, tmp_col2, col3, ...)]
//          [COLUMNS FROM PATH AS (col1, ...)]
//          [SET (k1=f1(xx), k2=f2(xxx))]
//          [where_clause]
//
//          DATA FROM TABLE external_hive_tbl_name
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [SET (k1=f1(xx), k2=f2(xxx))]
//          [where_clause]

/**
 * The transform of columns should be added after the keyword named COLUMNS.
 * The transform after the keyword named SET is the old ways which only supports the hadoop function.
 * It old way of transform will be removed gradually.
 */
public class NereidsDataDescription {
    // function isn't built-in function, hll_hash is not built-in function in hadoop load.
    private static final List<String> HADOOP_SUPPORT_FUNCTION_NAMES = Arrays.asList(
            "strftime",
            "time_format",
            "alignment_timestamp",
            "default_value",
            "md5sum",
            "replace_value",
            "now",
            FunctionSet.HLL_HASH,
            "substitute");

    private static final String DEFAULT_READ_JSON_BY_LINE = "true";

    private final String tableName;

    private String dbName;
    private final PartitionNames partitionNames;
    private final List<String> filePaths;
    private boolean clientLocal = false;
    private final boolean isNegative;
    // column names in the path
    private final List<String> columnsFromPath;
    // save column mapping in SET(xxx = xxx) clause
    private final List<Expression> columnMappingList;
    private final Expression precedingFilterExpr;
    private final Expression whereExpr;
    private final String srcTableName;
    // this only used in multi load, all filePaths is file not dir
    private List<Long> fileSize;
    // column names of source files
    private List<String> fileFieldNames;
    // Used for mini load
    private TNetworkAddress beAddr;
    private String columnDef;
    private long backendId;

    private String sequenceCol;

    // Merged from fileFieldNames, columnsFromPath and columnMappingList
    // ImportColumnDesc: column name to (expr or null)
    private List<NereidsImportColumnDesc> parsedColumnExprList = Lists.newArrayList();
    /*
     * This param only include the hadoop function which need to be checked in the future.
     * For hadoop load, this param is also used to persistence.
     * The function in this param is copied from 'parsedColumnExprList'
     */
    private final Map<String, Pair<String, List<String>>> columnToHadoopFunction = Maps
            .newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private boolean isHadoopLoad = false;

    private final LoadTask.MergeType mergeType;
    private final Expression deleteCondition;
    private final Map<String, String> properties;
    private boolean isMysqlLoad = false;
    // use for copy into
    private boolean ignoreCsvRedundantCol = false;

    private boolean isAnalyzed = false;

    private TUniqueKeyUpdateMode uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
    private FileFormatProperties fileFormatProperties;
    private Map<String, String> analysisMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    public NereidsDataDescription(String tableName,
            PartitionNames partitionNames,
            List<String> filePaths,
            List<String> columns,
            Separator columnSeparator,
            String fileFormat,
            boolean isNegative,
            List<Expression> columnMappingList) {
        this(tableName, partitionNames, filePaths, columns, columnSeparator, fileFormat, null,
                isNegative, columnMappingList, null, null, LoadTask.MergeType.APPEND, null, null, null);
    }

    public NereidsDataDescription(String tableName,
            PartitionNames partitionNames,
            List<String> filePaths,
            List<String> columns,
            Separator columnSeparator,
            String fileFormat,
            List<String> columnsFromPath,
            boolean isNegative,
            List<Expression> columnMappingList,
            Expression fileFilterExpr,
            Expression whereExpr,
            LoadTask.MergeType mergeType,
            Expression deleteCondition,
            String sequenceColName,
            Map<String, String> properties) {
        this(tableName, partitionNames, filePaths, columns, columnSeparator, null,
                fileFormat, null, columnsFromPath, isNegative, columnMappingList, fileFilterExpr, whereExpr,
                mergeType, deleteCondition, sequenceColName, properties);
    }

    /**
     * full param constructor
     */
    public NereidsDataDescription(String tableName,
                                  PartitionNames partitionNames,
                                  List<String> filePaths,
                                  List<String> columns,
                                  Separator columnSeparator,
                                  Separator lineDelimiter,
                                  String fileFormat,
                                  String compressType,
                                  String srcTableName,
                                  List<String> columnsFromPath,
                                  boolean isNegative,
                                  List<Expression> columnMappingList,
                                  Expression fileFilterExpr,
                                  Expression whereExpr,
                                  LoadTask.MergeType mergeType,
                                  Expression deleteCondition,
                                  String sequenceColName,
                                  Map<String, String> properties) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.filePaths = filePaths;
        this.fileFieldNames = columns;
        this.columnsFromPath = columnsFromPath;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.precedingFilterExpr = fileFilterExpr;
        this.whereExpr = whereExpr;
        this.srcTableName = srcTableName;
        this.mergeType = mergeType;
        this.deleteCondition = deleteCondition;
        this.sequenceCol = sequenceColName;
        this.properties = properties;
        if (properties != null) {
            this.analysisMap.putAll(properties);
        }
        // the default value of `read_json_by_line` must be true.
        // So that for broker load, this is always true,
        // and for stream load, it will set on demand.
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, DEFAULT_READ_JSON_BY_LINE);
        if (columnSeparator != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, columnSeparator.getOriSeparator());
        }
        if (lineDelimiter != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_LINE_DELIMITER, lineDelimiter.getOriSeparator());
        }
        putAnalysisMapIfNonNull(FileFormatProperties.PROP_FORMAT, fileFormat);
        putAnalysisMapIfNonNull(FileFormatProperties.PROP_COMPRESS_TYPE, compressType);
        columnsNameToLowerCase(fileFieldNames);
        columnsNameToLowerCase(columnsFromPath);
    }

    /**
     * NereidsDataDescription
     */
    public NereidsDataDescription(String tableName,
            PartitionNames partitionNames,
            List<String> filePaths,
            List<String> columns,
            Separator columnSeparator,
            Separator lineDelimiter,
            String fileFormat,
            String compressType,
            List<String> columnsFromPath,
            boolean isNegative,
            List<Expression> columnMappingList,
            Expression fileFilterExpr,
            Expression whereExpr,
            LoadTask.MergeType mergeType,
            Expression deleteCondition,
            String sequenceColName,
            Map<String, String> properties) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.filePaths = filePaths;
        this.fileFieldNames = columns;
        this.columnsFromPath = columnsFromPath;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.precedingFilterExpr = fileFilterExpr;
        this.whereExpr = whereExpr;
        this.srcTableName = null;
        this.mergeType = mergeType;
        this.deleteCondition = deleteCondition;
        this.sequenceCol = sequenceColName;
        this.properties = properties;
        if (properties != null) {
            this.analysisMap.putAll(properties);
        }
        // the default value of `read_json_by_line` must be true.
        // So that for broker load, this is always true,
        // and for stream load, it will set on demand.
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, DEFAULT_READ_JSON_BY_LINE);
        if (columnSeparator != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, columnSeparator.getOriSeparator());
        }
        if (lineDelimiter != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_LINE_DELIMITER, lineDelimiter.getOriSeparator());
        }
        putAnalysisMapIfNonNull(FileFormatProperties.PROP_FORMAT, fileFormat);
        putAnalysisMapIfNonNull(FileFormatProperties.PROP_COMPRESS_TYPE, compressType);
        columnsNameToLowerCase(fileFieldNames);
        columnsNameToLowerCase(columnsFromPath);
    }

    /**
     * data from table external_hive_table
     */
    public NereidsDataDescription(String tableName,
            PartitionNames partitionNames,
            String srcTableName,
            boolean isNegative,
            List<Expression> columnMappingList,
            Expression whereExpr,
            LoadTask.MergeType mergeType,
            Expression deleteCondition,
            Map<String, String> properties) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.filePaths = null;
        this.fileFieldNames = null;
        this.columnsFromPath = null;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.precedingFilterExpr = null; // external hive table does not support file filter expr
        this.whereExpr = whereExpr;
        this.srcTableName = srcTableName;
        this.mergeType = mergeType;
        this.deleteCondition = deleteCondition;
        this.properties = properties;
        if (properties != null) {
            this.analysisMap.putAll(properties);
        }
        // the default value of `read_json_by_line` must be true.
        // So that for broker load, this is always true,
        // and for stream load, it will set on demand.
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, DEFAULT_READ_JSON_BY_LINE);
    }

    /**
     * data desc for mysql client
     */
    public NereidsDataDescription(TableName tableName,
            PartitionNames partitionNames,
            String file,
            boolean clientLocal,
            List<String> columns,
            Separator columnSeparator,
            Separator lineDelimiter,
            int skipLines,
            List<Expression> columnMappingList,
            Map<String, String> properties) {
        this.tableName = tableName.getTbl();
        this.dbName = tableName.getDb();
        this.partitionNames = partitionNames;
        this.filePaths = Lists.newArrayList(file);
        this.clientLocal = clientLocal;
        this.fileFieldNames = columns;
        this.columnsFromPath = null;
        this.isNegative = false;
        this.columnMappingList = columnMappingList;
        this.precedingFilterExpr = null;
        this.whereExpr = null;
        this.srcTableName = null;
        this.mergeType = null;
        this.deleteCondition = null;
        this.properties = properties;
        if (properties != null) {
            this.analysisMap.putAll(properties);
        }
        // the default value of `read_json_by_line` must be true.
        // So that for broker load, this is always true,
        // and for stream load, it will set on demand.
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, DEFAULT_READ_JSON_BY_LINE);
        if (columnSeparator != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, columnSeparator.getOriSeparator());
        }
        if (lineDelimiter != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_LINE_DELIMITER, lineDelimiter.getOriSeparator());
        }
        putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_SKIP_LINES, String.valueOf(skipLines));
        this.isMysqlLoad = true;
        columnsNameToLowerCase(fileFieldNames);
    }

    /**
     * For stream load using external file scan node.
     */
    public NereidsDataDescription(String tableName, NereidsLoadTaskInfo taskInfo) {
        this.tableName = tableName;
        this.partitionNames = taskInfo.getPartitions();

        if (!Strings.isNullOrEmpty(taskInfo.getPath())) {
            this.filePaths = Lists.newArrayList(taskInfo.getPath());
        } else {
            // Add a dummy path to just make analyze() happy.
            this.filePaths = Lists.newArrayList("dummy");
        }

        this.fileFieldNames = taskInfo.getColumnExprDescs().getFileColNames();
        this.columnsFromPath = null;
        this.isNegative = taskInfo.getNegative();
        this.columnMappingList = taskInfo.getColumnExprDescs().getColumnMappingList();
        this.precedingFilterExpr = taskInfo.getPrecedingFilter();
        this.whereExpr = taskInfo.getWhereExpr();
        this.srcTableName = null;
        this.mergeType = taskInfo.getMergeType();
        this.deleteCondition = taskInfo.getDeleteCondition();
        this.sequenceCol = taskInfo.getSequenceCol();

        this.properties = Maps.newHashMap();
        if (properties != null) {
            this.analysisMap.putAll(properties);
        }
        putAnalysisMapIfNonNull(FileFormatProperties.PROP_FORMAT, getFileFormat(taskInfo));
        if (taskInfo.getCompressType() != null) {
            putAnalysisMapIfNonNull(FileFormatProperties.PROP_COMPRESS_TYPE, taskInfo.getCompressType().toString());
        }
        if (taskInfo.getColumnSeparator() != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR,
                    taskInfo.getColumnSeparator().getOriSeparator());
        }
        if (taskInfo.getLineDelimiter() != null) {
            putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_LINE_DELIMITER,
                    taskInfo.getLineDelimiter().getOriSeparator());
        }
        putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_ENCLOSE, new String(new byte[]{taskInfo.getEnclose()}));
        putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_ESCAPE, new String(new byte[]{taskInfo.getEscape()}));
        putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES,
                String.valueOf(taskInfo.getTrimDoubleQuotes()));
        putAnalysisMapIfNonNull(CsvFileFormatProperties.PROP_SKIP_LINES,
                String.valueOf(taskInfo.getSkipLines()));

        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY,
                String.valueOf(taskInfo.isStripOuterArray()));
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_JSON_PATHS, taskInfo.getJsonPaths());
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_JSON_ROOT, taskInfo.getJsonRoot());
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_FUZZY_PARSE, String.valueOf(taskInfo.isFuzzyParse()));
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE,
                String.valueOf(taskInfo.isReadJsonByLine()));
        putAnalysisMapIfNonNull(JsonFileFormatProperties.PROP_NUM_AS_STRING, String.valueOf(taskInfo.isNumAsString()));
        this.uniquekeyUpdateMode = taskInfo.getUniqueKeyUpdateMode();
        columnsNameToLowerCase(fileFieldNames);
    }

    private String getFileFormat(NereidsLoadTaskInfo taskInfo) {
        // get file format
        if (!Strings.isNullOrEmpty(taskInfo.getHeaderType())) {
            // for "csv_with_name" and "csv_with_name_and_type"
            return taskInfo.getHeaderType();
        } else {
            TFileFormatType type = taskInfo.getFormatType();
            if (Util.isCsvFormat(type)) {
                // ignore the "compress type" in format, such as FORMAT_CSV_GZ
                // the compress type is saved in "compressType"
                return "csv";
            } else {
                switch (type) {
                    case FORMAT_ORC:
                        return "orc";
                    case FORMAT_PARQUET:
                        return "parquet";
                    case FORMAT_JSON:
                        return "json";
                    case FORMAT_TEXT:
                        return "hive_text";
                    case FORMAT_WAL:
                        return "wal";
                    case FORMAT_ARROW:
                        return "arrow";
                    default:
                        return "unknown";
                }
            }
        }
    }

    private void putAnalysisMapIfNonNull(String key, String value) {
        if (value != null) {
            this.analysisMap.put(key, value);
        }
    }

    /**
     * validate hadoop functions
     */
    public static void validateMappingFunction(String functionName, List<String> args,
            Map<String, String> columnNameMap,
            Column mappingColumn, boolean isHadoopLoad) throws AnalysisException {
        if (functionName.equalsIgnoreCase("alignment_timestamp")) {
            validateAlignmentTimestamp(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase("strftime")) {
            validateStrftime(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase("time_format")) {
            validateTimeFormat(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase("default_value")) {
            validateDefaultValue(args, mappingColumn);
        } else if (functionName.equalsIgnoreCase("md5sum")) {
            validateMd5sum(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase("replace_value")) {
            validateReplaceValue(args, mappingColumn);
        } else if (functionName.equalsIgnoreCase(FunctionSet.HLL_HASH)) {
            validateHllHash(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase("now")) {
            validateNowFunction(mappingColumn);
        } else if (functionName.equalsIgnoreCase("substitute")) {
            validateSubstituteFunction(args, columnNameMap);
        } else {
            if (isHadoopLoad) {
                throw new AnalysisException("Unknown function: " + functionName);
            }
        }
    }

    // eg: k2 = substitute(k1)
    // this is used for creating derivative column from existing column
    private static void validateSubstituteFunction(List<String> args, Map<String, String> columnNameMap)
            throws AnalysisException {
        if (args.size() != 1) {
            throw new AnalysisException("Should has only one argument: " + args);
        }

        String argColumn = args.get(0);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(0, columnNameMap.get(argColumn));
    }

    private static void validateAlignmentTimestamp(List<String> args, Map<String, String> columnNameMap)
            throws AnalysisException {
        if (args.size() != 2) {
            throw new AnalysisException("Function alignment_timestamp args size is not 2");
        }

        String precision = args.get(0).toLowerCase();
        String regex = "^year|month|day|hour$";
        if (!precision.matches(regex)) {
            throw new AnalysisException("Alignment precision error. regex: " + regex + ", arg: " + precision);
        }

        String argColumn = args.get(1);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(1, columnNameMap.get(argColumn));
    }

    private static void validateStrftime(List<String> args, Map<String, String> columnNameMap)
            throws AnalysisException {
        if (args.size() != 2) {
            throw new AnalysisException("Function strftime needs 2 args");
        }

        String format = args.get(0);
        String regex = "^(%[YMmdHhiSs][ -:]?){0,5}%[YMmdHhiSs]$";
        if (!format.matches(regex)) {
            throw new AnalysisException("Date format error. regex: " + regex + ", arg: " + format);
        }

        String argColumn = args.get(1);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(1, columnNameMap.get(argColumn));
    }

    private static void validateTimeFormat(List<String> args, Map<String, String> columnNameMap)
            throws AnalysisException {
        if (args.size() != 3) {
            throw new AnalysisException("Function time_format needs 3 args");
        }

        String outputFormat = args.get(0);
        String inputFormat = args.get(1);
        String regex = "^(%[YMmdHhiSs][ -:]?){0,5}%[YMmdHhiSs]$";
        if (!outputFormat.matches(regex)) {
            throw new AnalysisException("Date format error. regex: " + regex + ", arg: " + outputFormat);
        }
        if (!inputFormat.matches(regex)) {
            throw new AnalysisException("Date format error. regex: " + regex + ", arg: " + inputFormat);
        }

        String argColumn = args.get(2);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(2, columnNameMap.get(argColumn));
    }

    private static void validateDefaultValue(List<String> args, Column column) throws AnalysisException {
        if (args.size() != 1) {
            throw new AnalysisException("Function default_value needs 1 arg");
        }

        if (!column.isAllowNull() && args.get(0) == null) {
            throw new AnalysisException("Column is not null, column: " + column.getName());
        }

        if (args.get(0) != null) {
            ColumnDef.validateDefaultValue(column.getOriginType(), args.get(0), column.getDefaultValueExprDef());
        }
    }

    private static void validateMd5sum(List<String> args, Map<String, String> columnNameMap) throws AnalysisException {
        for (int i = 0; i < args.size(); ++i) {
            String argColumn = args.get(i);
            if (!columnNameMap.containsKey(argColumn)) {
                throw new AnalysisException("Column is not in sources, column: " + argColumn);
            }

            args.set(i, columnNameMap.get(argColumn));
        }
    }

    private static void validateReplaceValue(List<String> args, Column column) throws AnalysisException {
        String replaceValue = null;
        if (args.size() == 1) {
            replaceValue = column.getDefaultValue();
            if (replaceValue == null) {
                throw new AnalysisException("Column " + column.getName() + " has no default value");
            }

            args.add(replaceValue);
        } else if (args.size() == 2) {
            replaceValue = args.get(1);
        } else {
            throw new AnalysisException("Function replace_value need 1 or 2 args");
        }

        if (!column.isAllowNull() && replaceValue == null) {
            throw new AnalysisException("Column is not null, column: " + column.getName());
        }

        if (replaceValue != null) {
            ColumnDef.validateDefaultValue(column.getOriginType(), replaceValue, column.getDefaultValueExprDef());
        }
    }

    private static void validateHllHash(List<String> args, Map<String, String> columnNameMap) throws AnalysisException {
        for (int i = 0; i < args.size(); ++i) {
            String argColumn = args.get(i);
            if (argColumn == null || !columnNameMap.containsKey(argColumn)) {
                throw new AnalysisException("Column is not in sources, column: " + argColumn);
            }
            args.set(i, columnNameMap.get(argColumn));
        }
    }

    private static void validateNowFunction(Column mappingColumn) throws AnalysisException {
        if (!mappingColumn.getOriginType().isDateType()) {
            throw new AnalysisException("Now() function is only support for DATE/DATETIME column");
        }
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public Expression getPrecdingFilterExpr() {
        return precedingFilterExpr;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    public LoadTask.MergeType getMergeType() {
        if (mergeType == null) {
            return LoadTask.MergeType.APPEND;
        }
        return mergeType;
    }

    public Expression getDeleteCondition() {
        return deleteCondition;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public List<String> getFileFieldNames() {
        if (fileFieldNames == null || fileFieldNames.isEmpty()) {
            return null;
        }
        return fileFieldNames;
    }

    public List<Expression> getColumnMappingList() {
        if (columnMappingList == null || columnMappingList.isEmpty()) {
            return null;
        }
        return columnMappingList;
    }

    public List<String> getColumnsFromPath() {
        return columnsFromPath;
    }

    public boolean isNegative() {
        return isNegative;
    }

    public TNetworkAddress getBeAddr() {
        return beAddr;
    }

    public void setBeAddr(TNetworkAddress addr) {
        beAddr = addr;
    }

    public String getSequenceCol() {
        return sequenceCol;
    }

    public void setColumnDef(String columnDef) {
        this.columnDef = columnDef;
    }

    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceCol);
    }

    public List<Long> getFileSize() {
        return fileSize;
    }

    public void setFileSize(List<Long> fileSize) {
        this.fileSize = fileSize;
    }

    public long getBackendId() {
        return backendId;
    }

    public FileFormatProperties getFileFormatProperties() {
        return fileFormatProperties;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public Map<String, Pair<String, List<String>>> getColumnToHadoopFunction() {
        return columnToHadoopFunction;
    }

    public List<NereidsImportColumnDesc> getParsedColumnExprList() {
        return parsedColumnExprList;
    }

    public void setIsHadoopLoad(boolean isHadoopLoad) {
        this.isHadoopLoad = isHadoopLoad;
    }

    public boolean isHadoopLoad() {
        return isHadoopLoad;
    }

    public boolean isClientLocal() {
        return clientLocal;
    }

    public String getSrcTableName() {
        return srcTableName;
    }

    public boolean isLoadFromTable() {
        return !Strings.isNullOrEmpty(srcTableName);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean getIgnoreCsvRedundantCol() {
        return ignoreCsvRedundantCol;
    }

    public void setIgnoreCsvRedundantCol(boolean ignoreCsvRedundantCol) {
        this.ignoreCsvRedundantCol = ignoreCsvRedundantCol;
    }

    /*
     * Analyze parsedExprMap and columnToHadoopFunction from columns, columns from path and columnMappingList
     * Example:
     *      columns (col1, tmp_col2, tmp_col3)
     *      columns from path as (col4, col5)
     *      set (col2=tmp_col2+1, col3=strftime("%Y-%m-%d %H:%M:%S", tmp_col3))
     *
     * Result:
     *      parsedExprMap = {"col1": null, "tmp_col2": null, "tmp_col3": null, "col4": null, "col5": null,
     *                       "col2": "tmp_col2+1", "col3": "strftime("%Y-%m-%d %H:%M:%S", tmp_col3)"}
     *      columnToHadoopFunction = {"col3": "strftime("%Y-%m-%d %H:%M:%S", tmp_col3)"}
     */
    private void analyzeColumns() throws AnalysisException {
        if ((fileFieldNames == null || fileFieldNames.isEmpty())
                && (columnsFromPath != null && !columnsFromPath.isEmpty())) {
            throw new AnalysisException("Can not specify columns_from_path without column_list");
        }

        // used to check duplicated column name in COLUMNS and COLUMNS FROM PATH
        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        // merge columns exprs from columns, columns from path and columnMappingList
        // 1. analyze columns
        if (fileFieldNames != null && !fileFieldNames.isEmpty()) {
            for (String columnName : fileFieldNames) {
                if (!columnNames.add(columnName)) {
                    throw new AnalysisException("Duplicate column: " + columnName);
                }
                NereidsImportColumnDesc importColumnDesc = new NereidsImportColumnDesc(columnName, null);
                parsedColumnExprList.add(importColumnDesc);
            }
        }

        // 2. analyze columns from path
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            if (isHadoopLoad) {
                throw new AnalysisException("Hadoop load does not support specifying columns from path");
            }
            for (String columnName : columnsFromPath) {
                if (!columnNames.add(columnName)) {
                    throw new AnalysisException("Duplicate column: " + columnName);
                }
                NereidsImportColumnDesc importColumnDesc = new NereidsImportColumnDesc(columnName, null);
                parsedColumnExprList.add(importColumnDesc);
            }
        }

        // 3: analyze column mapping
        if (columnMappingList == null || columnMappingList.isEmpty()) {
            return;
        }

        // used to check duplicated column name in SET clause
        Set<String> columnMappingNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // Step2: analyze column mapping
        // the column expr only support the SlotRef or eq binary predicate which's child(0) must be a SloRef.
        // the duplicate column name of SloRef is forbidden.
        for (Expression columnExpr : columnMappingList) {
            if (!(columnExpr instanceof EqualTo)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "Expr: " + columnExpr.toSql());
            }
            EqualTo equalTo = (EqualTo) columnExpr;
            Expression leftChild = equalTo.left();
            if (!(leftChild instanceof Slot)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "The mapping column error. column: " + leftChild.toSql());
            }
            String column = ((Slot) leftChild).getName();
            if (!columnMappingNames.add(column)) {
                throw new AnalysisException("Duplicate column mapping: " + column);
            }
            // hadoop load only supports the FunctionCallExpr
            Expression rightChild = equalTo.right();
            if (isHadoopLoad && !(rightChild instanceof Function)) {
                throw new AnalysisException(
                        "Hadoop load only supports the designated function. " + "The error mapping function is:"
                                + rightChild.toSql());
            }

            NereidsImportColumnDesc importColumnDesc = new NereidsImportColumnDesc(column, rightChild);
            parsedColumnExprList.add(importColumnDesc);
            if (rightChild instanceof Function) {
                analyzeColumnToHadoopFunction(column, (Function) rightChild);
            }
        }
    }

    private void analyzeMultiLoadColumns() throws AnalysisException {
        if (columnDef == null || columnDef.isEmpty()) {
            return;
        }
        List<Expression> expressions;
        try {
            expressions = NereidsLoadUtils.parseExpressionSeq(columnDef);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage());
        }
        for (Expression expr : expressions) {
            if (expr instanceof BinaryOperator) {
                parsedColumnExprList.add(new NereidsImportColumnDesc(expr.child(0).getExpressionName(), expr.child(1)));
            } else {
                parsedColumnExprList.add(new NereidsImportColumnDesc(expr.getExpressionName()));
            }
        }
    }

    private void analyzeColumnToHadoopFunction(String columnName, Function function) throws AnalysisException {
        String functionName = function.getName();
        if (!HADOOP_SUPPORT_FUNCTION_NAMES.contains(functionName.toLowerCase())) {
            return;
        }
        List<Expression> paramExprs = function.getArguments();
        List<String> args = Lists.newArrayList();
        for (Expression paramExpr : paramExprs) {
            if (paramExpr instanceof Slot) {
                Slot slot = (Slot) paramExpr;
                args.add(slot.getName());
            } else if (paramExpr instanceof NullLiteral) {
                args.add(null);
            } else if (paramExpr instanceof StringLikeLiteral) {
                StringLikeLiteral literal = (StringLikeLiteral) paramExpr;
                args.add(literal.getStringValue());
            } else {
                if (isHadoopLoad) {
                    // hadoop function only support slot, string and null parameters
                    throw new AnalysisException("Mapping function args error, arg: " + paramExpr.toSql());
                }
            }
        }

        Pair<String, List<String>> functionPair = Pair.of(functionName, args);
        columnToHadoopFunction.put(columnName, functionPair);
    }

    private void analyzeSequenceCol(String fullDbName) throws AnalysisException {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(fullDbName);
        OlapTable olapTable = db.getOlapTableOrAnalysisException(tableName);
        // no sequence column in load and table schema
        if (!hasSequenceCol() && !olapTable.hasSequenceCol()) {
            return;
        }
        // table has sequence map col
        if (olapTable.hasSequenceCol() && olapTable.getSequenceMapCol() != null) {
            return;
        }
        // check olapTable schema and sequenceCol
        if (olapTable.hasSequenceCol() && !hasSequenceCol()) {
            if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS) {
                return;
            }
            throw new AnalysisException("Table " + olapTable.getName()
                    + " has sequence column, need to specify the sequence column");
        }
        if (hasSequenceCol() && !olapTable.hasSequenceCol()) {
            throw new AnalysisException("There is no sequence column in the table " + olapTable.getName());
        }
        // check source sequence column is in parsedColumnExprList or Table base schema
        boolean hasSourceSequenceCol = false;
        if (!parsedColumnExprList.isEmpty()) {
            for (NereidsImportColumnDesc importColumnDesc : parsedColumnExprList) {
                if (importColumnDesc.getColumnName().equalsIgnoreCase(sequenceCol)) {
                    hasSourceSequenceCol = true;
                    break;
                }
            }
        } else {
            List<Column> columns = olapTable.getBaseSchema();
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(sequenceCol)) {
                    hasSourceSequenceCol = true;
                    break;
                }
            }
        }
        if (!hasSourceSequenceCol) {
            throw new AnalysisException("There is no sequence column " + sequenceCol + " in the " + olapTable.getName()
                    + " or the COLUMNS and SET clause");
        }
    }

    private void checkLoadPriv(String fullDbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("No table name in load statement.");
        }

        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName, tableName,
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(), fullDbName + ": " + tableName);
        }

        // check hive table auth
        if (isLoadFromTable()) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName, srcTableName,
                            PrivPredicate.SELECT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(), fullDbName + ": " + srcTableName);
            }
        }
    }

    // Change all the columns name to lower case, because Doris column is case-insensitive.
    private void columnsNameToLowerCase(List<String> columns) {
        if (columns == null || columns.isEmpty() || "json".equals(analysisMap.get(FileFormatProperties.PROP_FORMAT))) {
            return;
        }
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.remove(i);
            columns.add(i, column.toLowerCase());
        }
    }

    /**
     * analyzeFullDbName
     */
    public String analyzeFullDbName(String labelDbName, ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(labelDbName)) {
            String dbName = Strings.isNullOrEmpty(getDbName()) ? ctx.getDatabase() : getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            this.dbName = dbName;
            return this.dbName;
        } else {
            this.dbName = labelDbName;
            return labelDbName;
        }
    }

    /**
     * analyze
     */
    public void analyze(String fullDbName) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        checkLoadPriv(fullDbName);
        checkMergeType();
        analyzeWithoutCheckPriv(fullDbName);
        isAnalyzed = true;
    }

    private void checkMergeType() throws AnalysisException {
        if (mergeType != LoadTask.MergeType.MERGE && deleteCondition != null) {
            throw new AnalysisException("not support DELETE ON clause when merge type is not MERGE.");
        }
        if (mergeType == LoadTask.MergeType.MERGE && deleteCondition == null) {
            throw new AnalysisException("Excepted DELETE ON clause when merge type is MERGE.");
        }
        if (mergeType != LoadTask.MergeType.APPEND && isNegative) {
            throw new AnalysisException("not support MERGE or DELETE with NEGATIVE.");
        }
    }

    /**
     * analyzeWithoutCheckPriv
     */
    public void analyzeWithoutCheckPriv(String fullDbName) throws AnalysisException {
        analyzeFilePaths();

        if (partitionNames != null) {
            partitionNames.analyze();
        }

        analyzeColumns();
        analyzeMultiLoadColumns();
        analyzeSequenceCol(fullDbName);

        fileFormatProperties = FileFormatProperties.createFileFormatPropertiesOrAuto(analysisMap);
        fileFormatProperties.analyzeFileFormatProperties(analysisMap, false);
    }

    private void analyzeFilePaths() throws AnalysisException {
        if (!isLoadFromTable()) {
            if (filePaths == null || filePaths.isEmpty()) {
                throw new AnalysisException("No file path in load statement.");
            }
            filePaths.replaceAll(String::trim);
        }
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (isMysqlLoad) {
            sb.append("DATA ").append(isClientLocal() ? "LOCAL " : "");
            sb.append("INFILE '").append(filePaths.get(0)).append("'");
        } else if (isLoadFromTable()) {
            sb.append(mergeType.toString());
            sb.append(" DATA FROM TABLE ").append(srcTableName);
        } else {
            sb.append(mergeType.toString());
            sb.append(" DATA INFILE (");
            Joiner.on(", ").appendTo(sb, Lists.transform(filePaths, s -> "'" + s + "'")).append(")");
        }
        if (isNegative) {
            sb.append(" NEGATIVE");
        }
        sb.append(" INTO TABLE ");
        sb.append(isMysqlLoad ? ClusterNamespace.getNameFromFullName(dbName) + "." + tableName : tableName);
        if (partitionNames != null) {
            sb.append(" ");
            sb.append(partitionNames.toSql());
        }
        if (analysisMap.get(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR) != null) {
            sb.append(" COLUMNS TERMINATED BY ")
                    .append("'")
                    .append(analysisMap.get(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR))
                    .append("'");
        }
        if (analysisMap.get(CsvFileFormatProperties.PROP_LINE_DELIMITER) != null && isMysqlLoad) {
            sb.append(" LINES TERMINATED BY ")
                    .append("'")
                    .append(analysisMap.get(CsvFileFormatProperties.PROP_LINE_DELIMITER))
                    .append("'");
        }
        if (!Strings.isNullOrEmpty(analysisMap.get(FileFormatProperties.PROP_FORMAT))) {
            sb.append(" FORMAT AS '" + analysisMap.get(FileFormatProperties.PROP_FORMAT) + "'");
        }
        if (fileFieldNames != null && !fileFieldNames.isEmpty()) {
            sb.append(" (");
            Joiner.on(", ").appendTo(sb, fileFieldNames).append(")");
        }
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            sb.append(" COLUMNS FROM PATH AS (");
            Joiner.on(", ").appendTo(sb, columnsFromPath).append(")");
        }
        if (columnMappingList != null && !columnMappingList.isEmpty()) {
            sb.append(" SET (");
            Joiner.on(", ").appendTo(sb, Lists.transform(columnMappingList, expr -> expr.toSql())).append(")");
        }
        if (whereExpr != null) {
            sb.append(" WHERE ").append(whereExpr.toSql());
        }
        if (deleteCondition != null && mergeType == LoadTask.MergeType.MERGE) {
            sb.append(" DELETE ON ").append(deleteCondition.toSql());
        }
        return sb.toString();
    }

    /**
     * checkKeyTypeForLoad
     */
    public void checkKeyTypeForLoad(OlapTable table) throws AnalysisException {
        if (getMergeType() != LoadTask.MergeType.APPEND) {
            if (table.getKeysType() != KeysType.UNIQUE_KEYS) {
                throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
            } else if (!table.hasDeleteSign()) {
                throw new AnalysisException(
                        "load by MERGE or DELETE need to upgrade table to support batch delete.");
            }
        }
    }

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * this method can convert NereidsDataDescription to DataDescription
     */
    public DataDescription toDataDescription(ConnectContext ctx) throws AnalysisException {
        // we should get all member variables from NereidsDataDescription object,
        // then set them to DataDescription object
        List<Expr> legacyColumnMappingList = new ArrayList<>();
        if (this.columnMappingList != null && !this.columnMappingList.isEmpty()) {
            for (Expression expression : this.columnMappingList) {
                if (expression != null) {
                    legacyColumnMappingList.add(PlanUtils.translateToLegacyExpr(expression, null, ctx));
                }
            }
        }

        Expr legacyPrecedingFilterExpr = null;
        if (this.precedingFilterExpr != null) {
            legacyPrecedingFilterExpr = PlanUtils.translateToLegacyExpr(this.precedingFilterExpr, null, ctx);
        }
        Expr legacyWhere = null;
        if (this.whereExpr != null) {
            legacyWhere = PlanUtils.translateToLegacyExpr(this.whereExpr, null, ctx);
        }
        Expr legacyDeleteCondition = null;
        if (this.deleteCondition != null) {
            legacyDeleteCondition = PlanUtils.translateToLegacyExpr(this.deleteCondition, null, ctx);
        }
        Separator columnSeparator = analysisMap.get(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR) == null
                ? null : new Separator(analysisMap.get(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR));
        Separator lineDelimiter = analysisMap.get(CsvFileFormatProperties.PROP_LINE_DELIMITER) == null
                ? null : new Separator(analysisMap.get(CsvFileFormatProperties.PROP_LINE_DELIMITER));

        // first, we give an empty parsedColumnExprList
        List<ImportColumnDesc> parsedColumnExprList = Lists.newArrayList();
        DataDescription dataDescription = new DataDescription(
                this.tableName, this.partitionNames, this.filePaths, this.fileFieldNames,
                columnSeparator, lineDelimiter, analysisMap.get(FileFormatProperties.PROP_FORMAT),
                analysisMap.get(FileFormatProperties.PROP_COMPRESS_TYPE), this.columnsFromPath, this.isNegative,
                legacyColumnMappingList, legacyPrecedingFilterExpr, legacyWhere, this.mergeType, legacyDeleteCondition,
                this.sequenceCol, this.properties, this.isHadoopLoad, this.dbName, this.srcTableName,
                parsedColumnExprList, this.columnToHadoopFunction, this.uniquekeyUpdateMode, this.clientLocal,
                this.fileSize, this.columnDef, this.backendId, this.isMysqlLoad, this.ignoreCsvRedundantCol,
                this.fileFormatProperties, this.analysisMap);
        // then the dataDescription object should do analyze method to get parsedColumnExprList
        dataDescription.analyzeColumns();
        dataDescription.analyzeMultiLoadColumns();
        return dataDescription;
    }
}
