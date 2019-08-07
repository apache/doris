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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
// used to describe data info which is needed to import.
//
//      data_desc:
//          DATA INFILE ('file_path', ...)
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [COLUMNS TERMINATED BY separator]
//          [FORMAT AS format]
//          [(col1, ...)]
//          [SET (k1=f1(xx), k2=f2(xx))]
public class DataDescription {
    private static final Logger LOG = LogManager.getLogger(DataDescription.class);
    public static String FUNCTION_HASH_HLL = "hll_hash";
    private final String tableName;
    private final List<String> partitionNames;
    private final List<String> filePaths;
    private final List<String> columnNames;
    private final ColumnSeparator columnSeparator;
    private final String fileFormat;
    private final boolean isNegative;
    private final List<Expr> columnMappingList;

    // Used for mini load
    private TNetworkAddress beAddr;
    private String lineDelimiter;

    private Map<String, Pair<String, List<String>>> columnToFunction;
    private Map<String, Expr> parsedExprMap;

    private boolean isPullLoad = false;

    public DataDescription(String tableName, 
                           List<String> partitionNames, 
                           List<String> filePaths,
                           List<String> columnNames,
                           ColumnSeparator columnSeparator,
                           boolean isNegative,
                           List<Expr> columnMappingList) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.filePaths = filePaths;
        this.columnNames = columnNames;
        this.columnSeparator = columnSeparator;
        this.fileFormat = null;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
    }

    public DataDescription(String tableName,
                           List<String> partitionNames,
                           List<String> filePaths,
                           List<String> columnNames,
                           ColumnSeparator columnSeparator,
                           String fileFormat,
                           boolean isNegative,
                           List<Expr> columnMappingList) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.filePaths = filePaths;
        this.columnNames = columnNames;
        this.columnSeparator = columnSeparator;
        this.fileFormat = fileFormat;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public String getColumnSeparator() {
        if (columnSeparator == null) {
            return null;
        }
        return columnSeparator.getColumnSeparator();
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

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public void setLineDelimiter(String lineDelimiter) {
        this.lineDelimiter = lineDelimiter;
    }

    public void addColumnMapping(String functionName, Pair<String, List<String>> pair) {

        if (Strings.isNullOrEmpty(functionName) || pair == null) {
            return;
        }
        if (columnToFunction == null) {
            columnToFunction = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        }
        columnToFunction.put(functionName, pair);
    }

    public Map<String, Pair<String, List<String>>> getColumnMapping() {
        if (columnMappingList == null && columnToFunction == null) {
            return null;
        }

        return columnToFunction;
    }

    public List<Expr> getColumnMappingList() {
        return columnMappingList;
    }

    public Map<String, Expr> getParsedExprMap() {
        return parsedExprMap;
    }

    public void setIsPullLoad(boolean isPullLoad) {
        this.isPullLoad = isPullLoad;
    }

    public boolean isPullLoad() {
        return isPullLoad;
    }

    private void checkColumnInfo() throws AnalysisException {
        if (columnNames == null || columnNames.isEmpty()) {
            return;
        }
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String col : columnNames) {
            if (!columnSet.add(col)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col);
            }
        }
    }

    private void checkColumnMapping() throws AnalysisException {
        if (columnMappingList == null || columnMappingList.isEmpty()) {
            return;
        }

        columnToFunction = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        parsedExprMap = Maps.newHashMap();
        for (Expr expr : columnMappingList) {
            if (!(expr instanceof BinaryPredicate)) {
                throw new AnalysisException("Mapping function expr error. expr: " + expr.toSql());
            }

            BinaryPredicate predicate = (BinaryPredicate) expr;
            if (predicate.getOp() != Operator.EQ) {
                throw new AnalysisException("Mapping function operator error. op: " + predicate.getOp());
            }

            Expr child0 = predicate.getChild(0);
            if (!(child0 instanceof SlotRef)) {
                throw new AnalysisException("Mapping column error. column: " + child0.toSql());
            }

            String column = ((SlotRef) child0).getColumnName();
            if (columnToFunction.containsKey(column)) {
                throw new AnalysisException("Duplicate column mapping: " + column);
            }

            // we support function and column reference to change a column name
            Expr child1 = predicate.getChild(1);
            if (!(child1 instanceof FunctionCallExpr)) {
                if (isPullLoad && child1 instanceof SlotRef) {
                    // we only support SlotRef in pull load
                } else {
                    throw new AnalysisException("Mapping function error, function: " + child1.toSql());
                }
            }

            if (!child1.supportSerializable()) {
                throw new AnalysisException("Expr do not support serializable." + child1.toSql());
            }

            parsedExprMap.put(column, child1);

            if (!(child1 instanceof FunctionCallExpr)) {
                // only just for pass later check
                columnToFunction.put(column, Pair.create("__slot_ref", Lists.newArrayList()));
                continue;
            }

            FunctionCallExpr functionCallExpr = (FunctionCallExpr) child1;
            String functionName = functionCallExpr.getFnName().getFunction();
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
                    if (isPullLoad) {
                        continue;
                    } else {
                        throw new AnalysisException("Mapping function args error, arg: " + paramExpr.toSql());
                    }
                }
            }

            Pair<String, List<String>> functionPair = new Pair<String, List<String>>(functionName, args);
            columnToFunction.put(column, functionPair);
        }
    }

    public static void validateMappingFunction(String functionName, List<String> args,
                                               Map<String, String> columnNameMap,
                                               Column mappingColumn, boolean isPullLoad) throws AnalysisException {
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
        } else if (functionName.equalsIgnoreCase(FUNCTION_HASH_HLL)) {
            validateHllHash(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase("now")) {
            validateNowFunction(mappingColumn);
        } else {
            if (isPullLoad) {
                return;
            } else {
                throw new AnalysisException("Unknown function: " + functionName);
            }
        }
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

    private static void validateStrftime(List<String> args, Map<String, String> columnNameMap) throws
            AnalysisException {
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

    private static void validateTimeFormat(List<String> args, Map<String, String> columnNameMap) throws
            AnalysisException {
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
            ColumnDef.validateDefaultValue(column.getOriginType(), args.get(0));
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
            ColumnDef.validateDefaultValue(column.getOriginType(), replaceValue);
        }
    }

    private static void validateHllHash(List<String> args, Map<String, String> columnNameMap) throws AnalysisException {
        for (int i = 0; i < args.size(); ++i) {
            String argColumn = args.get(i);
            if (!columnNameMap.containsKey(argColumn)) {
                throw new AnalysisException("Column is not in sources, column: " + argColumn);
            }
            args.set(i, columnNameMap.get(argColumn));
        }
    }

    private static void validateNowFunction(Column mappingColumn) throws AnalysisException {
        if (mappingColumn.getOriginType() != Type.DATE && mappingColumn.getOriginType() != Type.DATETIME) {
            throw new AnalysisException("Now() function is only support for DATE/DATETIME column");
        }
    }

    public void analyze(String fullDbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("No table name in load statement.");
        }

        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), fullDbName, tableName,
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(), tableName);
        }

        if (filePaths == null || filePaths.isEmpty()) {
            throw new AnalysisException("No file path in load statement.");
        }
        for (int i = 0; i < filePaths.size(); ++i) {
            filePaths.set(i, filePaths.get(i).trim());
        }

        if (columnSeparator != null) {
            columnSeparator.analyze();
        }

        checkColumnInfo();
        checkColumnMapping();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DATA INFILE (");
        Joiner.on(", ").appendTo(sb, Lists.transform(filePaths, new Function<String, String>() {
            @Override
            public String apply(String s) {
                return "'" + s + "'";
            }
        })).append(")");
        if (isNegative) {
            sb.append(" NEGATIVE");
        }
        sb.append(" INTO TABLE ").append(tableName);
        if (partitionNames != null && !partitionNames.isEmpty()) {
            sb.append(" PARTITION (");
            Joiner.on(", ").appendTo(sb, partitionNames).append(")");
        }
        if (columnSeparator != null) {
            sb.append(" COLUMNS TERMINATED BY ").append(columnSeparator.toSql());
        }
        if (columnNames != null && !columnNames.isEmpty()) {
            sb.append(" (");
            Joiner.on(", ").appendTo(sb, columnNames).append(")");
        }
        if (columnMappingList != null && !columnMappingList.isEmpty()) {
            sb.append(" SET (");
            Joiner.on(", ").appendTo(sb, Lists.transform(columnMappingList, new Function<Expr, Object>() {
                @Override
                public Object apply(Expr expr) {
                    return expr.toSql();
                }
            })).append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
