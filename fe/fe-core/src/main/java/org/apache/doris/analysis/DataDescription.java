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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.load.NereidsLoadUtils;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
 * It old way of transform will be removed gradually. It
 */
public class DataDescription {
    // function isn't built-in function, hll_hash is not built-in function in hadoop load.
    private static final List<String> HADOOP_SUPPORT_FUNCTION_NAMES = Arrays.asList(
            "strftime",
            "time_format",
            "alignment_timestamp",
            "default_value",
            "md5sum",
            "replace_value",
            "now",
            "hll_hash",
            "substitute");

    private static final String DEFAULT_READ_JSON_BY_LINE = "true";

    private final String tableName;

    private String dbName;
    private final PartitionNamesInfo partitionNamesInfo;
    private final List<String> filePaths;
    private boolean clientLocal = false;
    private final boolean isNegative;
    // column names in the path
    private final List<String> columnsFromPath;
    // save column mapping in SET(xxx = xxx) clause
    private final List<Expr> columnMappingList;
    private final Expr precedingFilterExpr;
    private final Expr whereExpr;
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
    private List<ImportColumnDesc> parsedColumnExprList = Lists.newArrayList();
    /*
     * This param only include the hadoop function which need to be checked in the future.
     * For hadoop load, this param is also used to persistence.
     * The function in this param is copied from 'parsedColumnExprList'
     */
    private final Map<String, Pair<String, List<String>>> columnToHadoopFunction
            = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private boolean isHadoopLoad = false;

    private final LoadTask.MergeType mergeType;
    private final Expr deleteCondition;
    private final Map<String, String> properties;
    private boolean isMysqlLoad = false;
    // use for copy into
    private boolean ignoreCsvRedundantCol = false;

    private boolean isAnalyzed = false;

    TUniqueKeyUpdateMode uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;

    private FileFormatProperties fileFormatProperties;

    // This map is used to collect information of file format properties.
    // The map should be only used in `constructor` and `analyzeWithoutCheckPriv` method.
    private Map<String, String> analysisMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    public DataDescription(String tableName,
                           PartitionNamesInfo partitionNamesInfo,
                           List<String> filePaths,
                           List<String> columns,
                           Separator columnSeparator,
                           String fileFormat,
                           List<String> columnsFromPath,
                           boolean isNegative,
                           List<Expr> columnMappingList,
                           Expr fileFilterExpr,
                           Expr whereExpr,
                           LoadTask.MergeType mergeType,
                           Expr deleteCondition,
                           String sequenceColName,
                           Map<String, String> properties) {
        this(tableName, partitionNamesInfo, filePaths, columns, columnSeparator, null,
                fileFormat, null, columnsFromPath, isNegative, columnMappingList, fileFilterExpr, whereExpr,
                mergeType, deleteCondition, sequenceColName, properties);
    }

    /**
     * this constructor is for full parameters
     */
    public DataDescription(String tableName,
                           PartitionNamesInfo partitionNamesInfo,
                           List<String> filePaths,
                           List<String> columns,
                           Separator columnSeparator,
                           Separator lineDelimiter,
                           String fileFormat,
                           String compressType,
                           List<String> columnsFromPath,
                           boolean isNegative,
                           List<Expr> columnMappingList,
                           Expr fileFilterExpr,
                           Expr whereExpr,
                           LoadTask.MergeType mergeType,
                           Expr deleteCondition,
                           String sequenceColName,
                           Map<String, String> properties,
                           boolean isHadoopLoad,
                           String dbName,
                           String srcTableName,
                           List<ImportColumnDesc> parsedColumnExprList,
                           Map<String, Pair<String, List<String>>> columnToHadoopFunction,
                           TUniqueKeyUpdateMode uniquekeyUpdateMode,
                           boolean clientLocal,
                           List<Long> fileSize,
                           String columnDef,
                           long backendId,
                           boolean isMysqlLoad,
                           boolean ignoreCsvRedundantCol,
                           FileFormatProperties fileFormatProperties,
                           Map<String, String> analysisMap) {
        this.tableName = tableName;
        this.partitionNamesInfo = partitionNamesInfo;
        this.filePaths = filePaths;
        this.fileFieldNames = columns;
        this.columnsFromPath = columnsFromPath;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.precedingFilterExpr = fileFilterExpr;
        this.whereExpr = whereExpr;
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

        this.isHadoopLoad = isHadoopLoad;
        this.dbName = dbName;
        this.srcTableName = srcTableName;
        this.parsedColumnExprList = parsedColumnExprList;
        this.columnToHadoopFunction.putAll(columnToHadoopFunction);
        this.uniquekeyUpdateMode = uniquekeyUpdateMode;
        this.clientLocal = clientLocal;
        this.fileSize = fileSize;
        this.columnDef = columnDef;
        this.backendId = backendId;
        this.isMysqlLoad = isMysqlLoad;
        this.ignoreCsvRedundantCol = ignoreCsvRedundantCol;
        this.fileFormatProperties = fileFormatProperties;
        this.analysisMap = analysisMap;
    }

    public DataDescription(String tableName,
                           PartitionNamesInfo partitionNamesInfo,
                           List<String> filePaths,
                           List<String> columns,
                           Separator columnSeparator,
                           Separator lineDelimiter,
                           String fileFormat,
                           String compressType,
                           List<String> columnsFromPath,
                           boolean isNegative,
                           List<Expr> columnMappingList,
                           Expr fileFilterExpr,
                           Expr whereExpr,
                           LoadTask.MergeType mergeType,
                           Expr deleteCondition,
                           String sequenceColName,
                           Map<String, String> properties) {
        this.tableName = tableName;
        this.partitionNamesInfo = partitionNamesInfo;
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

    private void putAnalysisMapIfNonNull(String key, String value) {
        if (value != null) {
            this.analysisMap.put(key, value);
        }
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionNamesInfo getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    public Expr getPrecdingFilterExpr() {
        return precedingFilterExpr;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public LoadTask.MergeType getMergeType() {
        if (mergeType == null) {
            return LoadTask.MergeType.APPEND;
        }
        return mergeType;
    }

    public Expr getDeleteCondition() {
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

    public List<Expr> getColumnMappingList() {
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

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public FileFormatProperties getFileFormatProperties() {
        return fileFormatProperties;
    }

    public Map<String, Pair<String, List<String>>> getColumnToHadoopFunction() {
        return columnToHadoopFunction;
    }

    public List<ImportColumnDesc> getParsedColumnExprList() {
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
    public void analyzeColumns() throws AnalysisException {
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
                ImportColumnDesc importColumnDesc = new ImportColumnDesc(columnName, null);
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
                ImportColumnDesc importColumnDesc = new ImportColumnDesc(columnName, null);
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
        for (Expr columnExpr : columnMappingList) {
            if (!(columnExpr instanceof BinaryPredicate)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "Expr: " + columnExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            }
            BinaryPredicate predicate = (BinaryPredicate) columnExpr;
            if (predicate.getOp() != Operator.EQ) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "The mapping operator error, op: " + predicate.getOp());
            }
            Expr child0 = predicate.getChild(0);
            if (child0 instanceof CastExpr && child0.getChild(0) instanceof SlotRef) {
                predicate.setChild(0, child0.getChild(0));
                child0 = predicate.getChild(0);
            } else if (!(child0 instanceof SlotRef)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "The mapping column error. column: "
                        + child0.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            }
            String column = ((SlotRef) child0).getColumnName();
            if (!columnMappingNames.add(column)) {
                throw new AnalysisException("Duplicate column mapping: " + column);
            }
            // hadoop load only supports the FunctionCallExpr
            Expr child1 = predicate.getChild(1);
            if (isHadoopLoad && !(child1 instanceof FunctionCallExpr)) {
                throw new AnalysisException(
                        "Hadoop load only supports the designated function. " + "The error mapping function is:"
                                + child1.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            }
            // Must clone the expr, because in routine load, the expr will be analyzed for each task.
            Expr cloned = child1.clone();
            ImportColumnDesc importColumnDesc = new ImportColumnDesc(column, cloned);
            parsedColumnExprList.add(importColumnDesc);
            if (cloned instanceof FunctionCallExpr) {
                analyzeColumnToHadoopFunction(column, cloned);
            }
        }
    }

    public void analyzeMultiLoadColumns() throws AnalysisException {
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
                // we should translate Expression to Expr
                Expr legacyExpr = PlanUtils.translateToLegacyExpr(expr.child(1), null, ConnectContext.get());
                parsedColumnExprList.add(new ImportColumnDesc(expr.child(0).getExpressionName(), legacyExpr));
            } else {
                parsedColumnExprList.add(new ImportColumnDesc(expr.getExpressionName()));
            }
        }
    }

    private void analyzeColumnToHadoopFunction(String columnName, Expr child1) throws AnalysisException {
        Preconditions.checkState(child1 instanceof FunctionCallExpr);
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) child1;
        String functionName = functionCallExpr.getFnName().getFunction();
        if (!HADOOP_SUPPORT_FUNCTION_NAMES.contains(functionName.toLowerCase())) {
            return;
        }
        List<Expr> paramExprs = functionCallExpr.getParams().exprs();
        List<String> args = Lists.newArrayList();

        for (Expr paramExpr : paramExprs) {
            if (paramExpr instanceof SlotRef) {
                SlotRef slot = (SlotRef) paramExpr;
                args.add(slot.getColumnName());
            } else if (paramExpr instanceof StringLiteral) {
                StringLiteral literal = (StringLiteral) paramExpr;
                args.add(literal.getValue());
            } else if (paramExpr instanceof NullLiteral) {
                args.add(null);
            } else {
                if (isHadoopLoad) {
                    // hadoop function only support slot, string and null parameters
                    throw new AnalysisException("Mapping function args error, arg: "
                            + paramExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
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
            for (ImportColumnDesc importColumnDesc : parsedColumnExprList) {
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

    public void analyzeWithoutCheckPriv(String fullDbName) throws AnalysisException {
        analyzeFilePaths();

        if (partitionNamesInfo != null) {
            partitionNamesInfo.validate();
        }

        analyzeColumns();
        analyzeMultiLoadColumns();
        analyzeSequenceCol(fullDbName);

        fileFormatProperties = FileFormatProperties.createFileFormatPropertiesOrDeferred(
                analysisMap.getOrDefault(FileFormatProperties.PROP_FORMAT, ""));
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
            Joiner.on(", ").appendTo(sb, Lists.transform(filePaths, new Function<String, String>() {
                @Override
                public String apply(String s) {
                    return "'" + s + "'";
                }
            })).append(")");
        }
        if (isNegative) {
            sb.append(" NEGATIVE");
        }
        sb.append(" INTO TABLE ");
        sb.append(isMysqlLoad ? dbName + "." + tableName : tableName);
        if (partitionNamesInfo != null) {
            sb.append(" ");
            sb.append(partitionNamesInfo.toSql());
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
            Joiner.on(", ").appendTo(sb, Lists.transform(columnMappingList, new Function<Expr, Object>() {
                @Override
                public Object apply(Expr expr) {
                    return expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE);
                }
            })).append(")");
        }
        if (whereExpr != null) {
            sb.append(" WHERE ").append(whereExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
        }
        if (deleteCondition != null && mergeType == LoadTask.MergeType.MERGE) {
            sb.append(" DELETE ON ").append(deleteCondition.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
