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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToJson;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.ComplexTypeDefaultValue;
import org.apache.doris.nereids.trees.plans.commands.info.DefaultValue;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * process column mapping expressions, delete conditions and sequence columns
 */
public class NereidsLoadScanProvider {
    private static final Logger LOG = LogManager.getLogger(NereidsLoadScanProvider.class);
    private static final String HLL_HASH = "hll_hash";
    private static final String HLL_FROM_BASE64 = "hll_from_base64";
    private NereidsFileGroupInfo fileGroupInfo;
    private Set<String> partialUpdateInputColumns;

    public NereidsLoadScanProvider(NereidsFileGroupInfo fileGroupInfo, Set<String> partialUpdateInputColumns) {
        this.fileGroupInfo = fileGroupInfo;
        this.partialUpdateInputColumns = partialUpdateInputColumns;
    }

    /**
     * creating a NereidsParamCreateContext contains column mapping expressions and scan slots
     */
    public NereidsParamCreateContext createLoadContext() throws UserException {
        NereidsParamCreateContext context = new NereidsParamCreateContext();
        context.fileGroup = fileGroupInfo.getFileGroup();
        NereidsLoadTaskInfo.NereidsImportColumnDescs columnDescs = new NereidsLoadTaskInfo.NereidsImportColumnDescs();
        columnDescs.descs = context.fileGroup.getColumnExprList();
        handleDeleteCondition(columnDescs.descs, context.fileGroup);
        handleSequenceColumn(columnDescs.descs, context.fileGroup);
        fillContextExprMap(columnDescs.descs, context);
        return context;
    }

    private void handleDeleteCondition(List<NereidsImportColumnDesc> columnDescList, NereidsBrokerFileGroup fileGroup) {
        if (fileGroup.getMergeType() == LoadTask.MergeType.MERGE) {
            columnDescList.add(
                    NereidsImportColumnDesc.newDeleteSignImportColumnDesc(fileGroup.getDeleteCondition()));
        } else if (fileGroup.getMergeType() == LoadTask.MergeType.DELETE) {
            columnDescList.add(NereidsImportColumnDesc.newDeleteSignImportColumnDesc(new IntegerLiteral(1)));
        }
    }

    private void handleSequenceColumn(List<NereidsImportColumnDesc> columnDescList, NereidsBrokerFileGroup fileGroup)
            throws UserException {
        TableIf targetTable = fileGroupInfo.getTargetTable();
        if (targetTable instanceof OlapTable && ((OlapTable) targetTable).hasSequenceCol()) {
            OlapTable olapTable = (OlapTable) targetTable;
            String sequenceCol = olapTable.getSequenceMapCol();
            if (sequenceCol != null) {
                String finalSequenceCol = sequenceCol;
                Optional<NereidsImportColumnDesc> foundCol = columnDescList.stream()
                        .filter(c -> c.getColumnName().equalsIgnoreCase(finalSequenceCol)).findAny();
                // if `columnDescs.descs` is empty, that means it's not a partial update load, and user not specify
                // column name.
                if (foundCol.isPresent() || shouldAddSequenceColumn(columnDescList)) {
                    columnDescList.add(new NereidsImportColumnDesc(Column.SEQUENCE_COL,
                            new UnboundSlot(sequenceCol)));
                } else if (!fileGroupInfo.isFixedPartialUpdate()) {
                    Column seqCol = olapTable.getFullSchema().stream()
                            .filter(col -> col.getName().equals(olapTable.getSequenceMapCol()))
                            .findFirst().get();
                    boolean isCurrentTimestamp = seqCol.getDefaultValue() != null
                            && (seqCol.getDefaultValue().equals(DefaultValue.CURRENT_TIMESTAMP)
                            || (seqCol.getType().isTimeStampNs()
                            && org.apache.doris.analysis.ColumnDef.DefaultValue
                                    .isCurrentTimeStampDefaultValue(seqCol.getDefaultValue())));
                    if (!isCurrentTimestamp) {
                        throw new UserException("Table " + olapTable.getName()
                                + " has sequence column, need to specify the sequence column");
                    }
                }
            } else if (!fileGroupInfo.isFlexiblePartialUpdate()) {
                sequenceCol = fileGroup.getSequenceCol();
                columnDescList.add(new NereidsImportColumnDesc(Column.SEQUENCE_COL, new UnboundSlot(sequenceCol)));
            }
        }
    }

    private void fillContextExprMap(List<NereidsImportColumnDesc> columnDescList, NereidsParamCreateContext context)
            throws UserException {
        NereidsBrokerFileGroup fileGroup = context.fileGroup;
        context.exprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.scanSlots = new ArrayList<>(columnDescList.size());
        Table tbl = fileGroupInfo.getTargetTable();

        // rewrite column list
        List<NereidsImportColumnDesc> columnDescs = new ArrayList<>(columnDescList.size());
        Map<UnboundSlot, Expression> replaceMap = Maps.newHashMap();
        for (NereidsImportColumnDesc desc : columnDescList) {
            if (!desc.isColumn()) {
                NereidsImportColumnDesc newDesc = desc.withExpr(ExpressionUtils.replace(desc.getExpr(), replaceMap));
                columnDescs.add(newDesc);
                replaceMap.put(new UnboundSlot(newDesc.getColumnName()), newDesc.getExpr());
            } else {
                columnDescs.add(desc);
            }
        }

        // We make a copy of the columnExprs so that our subsequent changes
        // to the columnExprs will not affect the original columnExprs.
        // skip the mapping columns not exist in schema
        // eg: the origin column list is:
        //          (k1, k2, tmpk3 = k1 + k2, k3 = tmpk3)
        //     after calling rewriteColumns(), it will become
        //          (k1, k2, tmpk3 = k1 + k2, k3 = k1 + k2)
        //     so "tmpk3 = k1 + k2" is not needed anymore, we can skip it.
        List<NereidsImportColumnDesc> copiedColumnExprs = new ArrayList<>(columnDescs.size());
        Set<String> constantMappingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (NereidsImportColumnDesc importColumnDesc : columnDescs) {
            String mappingColumnName = importColumnDesc.getColumnName();
            if (importColumnDesc.isColumn()) {
                copiedColumnExprs.add(importColumnDesc);
            } else if (tbl.getColumn(mappingColumnName) != null) {
                copiedColumnExprs.add(importColumnDesc);
                // Only track columns with constant expressions (e.g., k1 = 'constant', k1 = 'uuid()', ...)
                // Non-constant expressions (e.g., "k1 = k1 + 1") still need to read from file
                if (importColumnDesc.getExpr().getInputSlots().isEmpty()) {
                    constantMappingColumns.add(mappingColumnName);
                }
            }
            // Skip mapping columns that don't exist in table schema
        }

        // check whether the OlapTable has sequenceCol and skipBitmapCol
        boolean hasSequenceCol = false;
        boolean hasSequenceMapCol = false;
        boolean hasSkipBitmapColumn = false;
        if (tbl instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) tbl;
            hasSequenceCol = olapTable.hasSequenceCol();
            hasSequenceMapCol = (olapTable.getSequenceMapCol() != null);
            hasSkipBitmapColumn = olapTable.hasSkipBitmapColumn();
        }

        // If user does not specify the file field names, generate it by using base schema of table.
        // So that the following process can be unified
        boolean specifyFileFieldNames = copiedColumnExprs.stream().anyMatch(p -> p.isColumn());
        if (!specifyFileFieldNames) {
            List<Column> columns = tbl.getBaseSchema(false);
            for (Column column : columns) {
                if (constantMappingColumns.contains(column.getName())) {
                    // Skip this column because user has already specified a constant mapping expression for it
                    // in the COLUMNS parameter (e.g., "column_name = 'constant_value'")
                    continue;
                }
                NereidsImportColumnDesc columnDesc;
                TFileFormatType fileFormatType = fileGroup.getFileFormatProperties().getFileFormatType();
                if (Util.isCasePreservingFormat(fileFormatType)) {
                    columnDesc = new NereidsImportColumnDesc(column.getName());
                } else {
                    columnDesc = new NereidsImportColumnDesc(column.getName().toLowerCase());
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("add base column {} to stream load task", column.getName());
                }
                copiedColumnExprs.add(columnDesc);
            }

            List<String> hiddenColumns = fileGroupInfo.getHiddenColumns();
            if (hasSkipBitmapColumn
                    && fileGroupInfo.getUniqueKeyUpdateMode() == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS) {
                Preconditions.checkArgument(hiddenColumns == null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("add hidden column {} to stream load task", Column.DELETE_SIGN);
                }
                copiedColumnExprs.add(new NereidsImportColumnDesc(Column.DELETE_SIGN));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("add hidden column {} to stream load task", Column.SKIP_BITMAP_COL);
                }
                // allow to specify __DORIS_SEQUENCE_COL__ if table has sequence type column
                if (hasSequenceCol && !hasSequenceMapCol) {
                    copiedColumnExprs.add(new NereidsImportColumnDesc(Column.SEQUENCE_COL));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("add hidden column {} to stream load task", Column.SEQUENCE_COL);
                    }
                }
                copiedColumnExprs.add(new NereidsImportColumnDesc(Column.SKIP_BITMAP_COL));
            }

            if (hiddenColumns != null) {
                for (String columnName : hiddenColumns) {
                    Column column = tbl.getColumn(columnName);
                    if (column != null && !column.isVisible()) {
                        NereidsImportColumnDesc columnDesc = new NereidsImportColumnDesc(column.getName());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("add hidden column {} to stream load task", column.getName());
                        }
                        copiedColumnExprs.add(columnDesc);
                    }
                }
            }
        }

        // generate a map for checking easily
        Map<String, Expression> columnExprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (NereidsImportColumnDesc importColumnDesc : copiedColumnExprs) {
            columnExprMap.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
        }

        Map<String, Pair<String, List<String>>> columnToHadoopFunction = fileGroup.getColumnToHadoopFunction();
        // validate hadoop functions
        if (columnToHadoopFunction != null) {
            Map<String, String> columnNameMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (NereidsImportColumnDesc importColumnDesc : copiedColumnExprs) {
                if (importColumnDesc.isColumn()) {
                    columnNameMap.put(importColumnDesc.getColumnName(), importColumnDesc.getColumnName());
                }
            }
            for (Map.Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                String mappingColumnName = entry.getKey();
                Column mappingColumn = tbl.getColumn(mappingColumnName);
                if (mappingColumn == null) {
                    throw new DdlException("Mapping column is not in table. column: " + mappingColumnName);
                }
                Pair<String, List<String>> function = entry.getValue();
                try {
                    NereidsDataDescription.validateMappingFunction(function.first, function.second, columnNameMap,
                            mappingColumn, false);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
        }

        // check default value and auto-increment column
        for (Column column : tbl.getBaseSchema()) {
            if (fileGroupInfo.getUniqueKeyUpdateMode() == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS
                    && !partialUpdateInputColumns.contains(column.getName())) {
                continue;
            }
            String columnName = column.getName();
            Expression expression = null;
            if (column.getGeneratedColumnInfo() != null) {
                // the generated column will be handled by bindSink
                continue;
            }

            if (columnExprMap.get(columnName) != null) {
                expression = columnExprMap.get(columnName);
                expression = transformHadoopFunctionExpr(tbl, columnName, expression);
            } else {
                // If NEGATIVE keyword is specified and column aggregate type is SUM, create an
                // unbound slot
                // This allows subsequent negative value processing (multiply by -1) for SUM
                // type columns
                if (fileGroup.isNegative() && column.getAggregationType() != null
                        && column.getAggregationType() == AggregateType.SUM) {
                    expression = new UnboundSlot(columnName);
                }
                // other column with default value will be handled by bindSink
            }

            if (expression != null) {
                // check hll_hash
                if (column.getDataType() == PrimitiveType.HLL) {
                    if (!(expression instanceof UnboundFunction)) {
                        throw new AnalysisException("HLL column must use " + HLL_HASH + " function, like "
                                + columnName + "=" + HLL_HASH + "(xxx)");
                    }
                    UnboundFunction function = (UnboundFunction) expression;
                    String functionName = function.getName();
                    if (!functionName.equalsIgnoreCase(HLL_HASH)
                            && !functionName.equalsIgnoreCase("hll_empty")
                            && !functionName.equalsIgnoreCase(HLL_FROM_BASE64)) {
                        throw new AnalysisException("HLL column must use " + HLL_HASH + " function, like "
                                + columnName + "=" + HLL_HASH + "(xxx) or "
                                + columnName + "=" + HLL_FROM_BASE64 + "(xxx) or "
                                + columnName + "=hll_empty()");
                    }
                }

                // If NEGATIVE keyword is specified and column aggregate type is SUM, multiply expression by -1
                // This implements negative value import functionality, converting additive values to subtractive values
                if (fileGroup.isNegative() && column.getAggregationType() != null
                        && column.getAggregationType() == AggregateType.SUM) {
                    expression = new Multiply(expression, new IntegerLiteral(-1));
                }

                // check Bitmap Compatibility and check QuantileState Compatibility need be checked after binding
                // for jsonb type, use jsonb_parse_xxx to parse src string to jsonb.
                // and if input string is not a valid json string, return null. this need be handled after binding
                // expression = ExpressionUtils.replace(expression, replaceMap);
                // replaceMap.put(new UnboundSlot(columnName), expression);
                context.exprMap.put(column.getName(), expression);
            }
        }

        // create scan SlotReferences
        boolean hasColumnFromTable = false;
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (NereidsImportColumnDesc importColumnDesc : copiedColumnExprs) {
            // make column name case match with real column name
            String columnName = importColumnDesc.getColumnName();
            Column tblColumn = tbl.getColumn(columnName);
            if (tblColumn != null) {
                hasColumnFromTable = true;
            }
            String realColName;
            if (tblColumn == null || tblColumn.getName() == null || importColumnDesc.getExpr() == null) {
                realColName = columnName;
            } else {
                realColName = tblColumn.getName();
            }
            if (importColumnDesc.getExpr() != null) {
                if (tblColumn.getGeneratedColumnInfo() == null && !context.exprMap.containsKey(realColName)) {
                    context.exprMap.put(realColName, importColumnDesc.getExpr());
                }
            } else {
                Column slotColumn;
                TFileFormatType fileFormatType = fileGroup.getFileFormatProperties().getFileFormatType();
                // Use real column type for arrow format, other formats read as varchar first
                if (fileFormatType == TFileFormatType.FORMAT_ARROW) {
                    if (tblColumn == null) {
                        throw new AnalysisException("Unknown column " + realColName + " in table " + tbl.getName()
                                + " for " + fileFormatType + " load");
                    }
                    slotColumn = new Column(realColName, tblColumn.getType(), true);
                } else {
                    if (fileGroupInfo.getUniqueKeyUpdateMode() == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS
                            && hasSkipBitmapColumn) {
                        // we store the unique ids of missing columns in skip bitmap column in flexible partial update
                        int colUniqueId = tblColumn.getUniqueId();
                        if (realColName.equals(Column.SKIP_BITMAP_COL)) {
                            // don't change the skip_bitmap_col's type to varchar becasue we will fill this column
                            // in NewJsonReader manually rather than reading them from files as varchar type and then
                            // converting them to their real type
                            slotColumn = new Column(realColName, PrimitiveType.BITMAP, true);
                        } else {
                            // columns default be varchar type
                            slotColumn = new Column(realColName, PrimitiveType.VARCHAR, true);
                        }
                        // In flexible partial update, every row can update different columns, we should check
                        // key columns intergrity for every row in XXXReader on BE rather than checking it on FE
                        // directly for all rows like in fixed columns partial update. So we should set if a slot
                        // is key column here
                        slotColumn.setIsKey(tblColumn.isKey());
                        slotColumn.setIsAutoInc(tblColumn.isAutoInc());
                        slotColumn.setUniqueId(colUniqueId);
                    } else {
                        slotColumn = new Column(realColName, PrimitiveType.VARCHAR, true);
                    }
                }
                context.scanSlots.add(
                        SlotReference.fromColumn(exprIdGenerator.getNextId(), tbl, slotColumn, tbl.getFullQualifiers())
                );
            }
        }
        if (!hasColumnFromTable) {
            // we should add at least one column for target table to make bindSink happy
            Column column = null;
            for (Column col : tbl.getBaseSchema()) {
                if (col.getGeneratedColumnInfo() == null) {
                    column = col;
                    break;
                }
            }
            if (column == null) {
                throw new DdlException(String.format("can't find non-generated column in table %s", tbl.getName()));
            }
            context.exprMap.put(column.getName(), new NullLiteral(DataType.fromCatalogType(column.getType())));
        }
    }

    /**
     * if not set sequence column and column size is null or only have deleted sign ,return true
     */
    private boolean shouldAddSequenceColumn(List<NereidsImportColumnDesc> columnDescList) {
        if (columnDescList.isEmpty()) {
            return true;
        }
        return columnDescList.size() == 1 && columnDescList.get(0).getColumnName().equalsIgnoreCase(Column.DELETE_SIGN);
    }

    private TFileFormatType formatType(String fileFormat) throws UserException {
        if (fileFormat == null) {
            // get file format by the file path
            return TFileFormatType.FORMAT_CSV_PLAIN;
        }
        TFileFormatType formatType = Util.getFileFormatTypeFromName(fileFormat);
        if (formatType == TFileFormatType.FORMAT_UNKNOWN) {
            throw new UserException("Not supported file format: " + fileFormat);
        }
        return formatType;
    }

    /**
     * This method is used to transform hadoop function.
     * The hadoop function includes: replace_value, strftime, time_format, alignment_timestamp, default_value, now.
     * It rewrites those function with real function name and param.
     * For the other function, the expr only go through this function and the origin expr is returned.
     */
    private Expression transformHadoopFunctionExpr(Table tbl, String columnName, Expression originExpr)
            throws UserException {
        Column column = tbl.getColumn(columnName);
        if (column == null) {
            // the unknown column will be checked later.
            return originExpr;
        }
        boolean nativeComplex = readsNativeComplexType(column);

        // To compatible with older load version
        if (originExpr instanceof Function) {
            Function funcExpr = (Function) originExpr;
            String funcName = funcExpr.getName();

            if (funcName.equalsIgnoreCase("replace_value")) {
                List<Expression> exprs = Lists.newArrayList();
                UnboundSlot slot = new UnboundSlot(columnName);
                // We will convert this to IF(`col` != child0, `col`, child1),
                // because we need the if return type equal to `col`, we use NE

                /*
                 * We will convert this based on different cases:
                 * case 1: k1 = replace_value(null, anyval);
                 *     to: k1 = if (k1 is not null, k1, anyval);
                 *
                 * case 2: k1 = replace_value(anyval1, anyval2);
                 *     to: k1 = if (k1 is not null, if(k1 != anyval1, k1, anyval2), null);
                 */
                if (funcExpr.child(0) instanceof NullLiteral) {
                    // case 1
                    exprs.add(new Not(new IsNull(slot)));
                    exprs.add(slot);
                    if (funcExpr.children().size() > 1) {
                        exprs.add(mappingValue(column, funcExpr.child(1), nativeComplex));
                    } else {
                        if (column.getDefaultValue() != null) {
                            String exprSql = column.getDefaultValueSql();
                            exprs.add(mappingValue(column, NereidsLoadUtils.parseExpressionSeq(exprSql).get(0),
                                    nativeComplex));
                        } else {
                            if (column.isAllowNull()) {
                                exprs.add(nullMappingValue(nativeComplex));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                } else {
                    // case 2
                    exprs.add(new Not(new IsNull(slot)));
                    List<Expression> innerIfExprs = Lists.newArrayList();
                    innerIfExprs.add(new Not(searchValueComparison(slot, column, funcExpr.child(0), nativeComplex)));
                    innerIfExprs.add(slot);
                    if (funcExpr.children().size() > 1) {
                        innerIfExprs.add(mappingValue(column, funcExpr.child(1), nativeComplex));
                    } else {
                        if (column.getDefaultValue() != null) {
                            String exprSql = column.getDefaultValueSql();
                            innerIfExprs.add(mappingValue(column,
                                    NereidsLoadUtils.parseExpressionSeq(exprSql).get(0), nativeComplex));
                        } else {
                            if (column.isAllowNull()) {
                                innerIfExprs.add(nullMappingValue(nativeComplex));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                    UnboundFunction innerIfFn = new UnboundFunction("if", innerIfExprs);
                    exprs.add(innerIfFn);
                    exprs.add(nullMappingValue(nativeComplex));
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("replace_value expr: {}", exprs);
                }
                UnboundFunction newFn = new UnboundFunction("if", exprs);
                return newFn;
            } else if (funcName.equalsIgnoreCase("strftime")) {
                // FROM_UNIXTIME(val)
                return new UnboundFunction("from_unixtime", Lists.newArrayList(funcExpr.child(1)));
            } else if (funcName.equalsIgnoreCase("time_format")) {
                // DATE_FORMAT(STR_TO_DATE(dt_str, dt_fmt))
                List<Expression> strToDateExprs = Lists.newArrayList(funcExpr.child(2), funcExpr.child(1));
                UnboundFunction strToDateFuncExpr = new UnboundFunction("str_to_date", strToDateExprs);
                List<Expression> dateFormatArgs = Lists.newArrayList(strToDateFuncExpr, funcExpr.child(0));
                UnboundFunction dateFormatFunc = new UnboundFunction("date_format", dateFormatArgs);
                return dateFormatFunc;
            } else if (funcName.equalsIgnoreCase("alignment_timestamp")) {
                /*
                 * change to:
                 * UNIX_TIMESTAMP(DATE_FORMAT(FROM_UNIXTIME(ts), "%Y-01-01 00:00:00"));
                 *
                 */

                // FROM_UNIXTIME
                UnboundFunction fromUnixFunc = new UnboundFunction("from_unixtime",
                        Lists.newArrayList(funcExpr.child(1)));

                // DATE_FORMAT
                StringLiteral precision = (StringLiteral) funcExpr.child(0);
                StringLiteral format;
                if (precision.getStringValue().equalsIgnoreCase("year")) {
                    format = new StringLiteral("%Y-01-01 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("month")) {
                    format = new StringLiteral("%Y-%m-01 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("day")) {
                    format = new StringLiteral("%Y-%m-%d 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("hour")) {
                    format = new StringLiteral("%Y-%m-%d %H:00:00");
                } else {
                    throw new UserException("Unknown precision(" + precision.getStringValue() + ")");
                }
                List<Expression> dateFormatArgs = Lists.newArrayList(fromUnixFunc, format);
                UnboundFunction dateFormatFunc = new UnboundFunction("date_format", dateFormatArgs);

                // UNIX_TIMESTAMP
                List<Expression> unixTimeArgs = Lists.newArrayList();
                unixTimeArgs.add(dateFormatFunc);
                UnboundFunction unixTimeFunc = new UnboundFunction("unix_timestamp", unixTimeArgs);

                return unixTimeFunc;
            } else if (funcName.equalsIgnoreCase("default_value")) {
                return canonicalizeComplexValue(column, funcExpr.child(0));
            } else if (funcName.equalsIgnoreCase("now")) {
                UnboundFunction newFunc = new UnboundFunction("now", Lists.newArrayList());
                return newFunc;
            } else if (funcName.equalsIgnoreCase("substitute")) {
                return funcExpr.child(0);
            }
        }
        return originExpr;
    }

    /**
     * The explicit value of a default_value or replace_value mapping on an ARRAY/MAP/STRUCT column was
     * validated by {@link NereidsDataDescription#validateMappingFunction} as a SQL literal, but BE casts
     * the mapping expression as a string. Replace it with the canonical text so that both agree, e.g.
     * {@code '[1e3]'} for an array of INT is written as {@code [1000]} instead of a NULL element.
     */
    private static Expression canonicalizeComplexValue(Column column, Expression value) {
        if (!column.getOriginType().isComplexType() || !(value instanceof StringLikeLiteral)) {
            return value;
        }
        return new StringLiteral(ComplexTypeDefaultValue.canonicalize(
                DataType.fromCatalogType(column.getOriginType()), ((StringLikeLiteral) value).getStringValue()));
    }

    /**
     * Arrow reads a column with its declared type, so an ARRAY/MAP/STRUCT column keeps the complex type
     * in its scan slot instead of being read as VARCHAR like the text formats. The mapping expressions
     * have to work on that complex type, otherwise the IF branches mix complex and string values.
     */
    private boolean readsNativeComplexType(Column column) {
        return column.getOriginType().isComplexType()
                && fileGroupInfo.getFileGroup().getFileFormatProperties().getFileFormatType()
                        == TFileFormatType.FORMAT_ARROW;
    }

    /**
     * The mapping value of a complex column is written as canonical text. When the scan slot keeps the
     * native complex type, parse that text into the column type: a string branch and a complex branch
     * have no common type under the legacy type coercion behavior ({@code
     * enable_new_type_coercion_behavior=false}).
     */
    private static Expression mappingValue(Column column, Expression value, boolean nativeComplex) {
        Expression canonical = canonicalizeComplexValue(column, value);
        if (!nativeComplex) {
            return canonical;
        }
        return new Cast(canonical, DataType.fromCatalogType(column.getOriginType()));
    }

    /**
     * The NULL branch of a mapping IF. The typed VARCHAR NULL used for text sources has no common type
     * with a native complex branch under the legacy type coercion behavior, so leave it untyped.
     */
    private static Expression nullMappingValue(boolean nativeComplex) {
        return nativeComplex ? new NullLiteral() : new NullLiteral(VarcharType.SYSTEM_DEFAULT);
    }

    /**
     * A text source compares its raw value with the search value of {@code replace_value}. A native
     * complex slot needs a type-aware comparison instead of the display text, whose nested strings are
     * not escaped: a single element {@code a", "b} renders exactly like the two element array
     * {@code ["a", "b"]}. The search is a transient comparison value, so it must not be canonicalized
     * as a column default, whose format rejects quotes and backslashes that the complex cast accepts.
     */
    private static Expression searchValueComparison(Expression slot, Column column, Expression search,
            boolean nativeComplex) throws AnalysisException {
        if (!nativeComplex) {
            return new EqualTo(slot, search);
        }
        DataType complexType = DataType.fromCatalogType(column.getOriginType());
        // Cast the search expression, literal or slot, to the column type so that the comparison sees
        // exactly the bytes the sink writes. The cast is strict: Stream Load disables
        // enable_strict_cast, so a malformed non-NULL search would otherwise become SQL NULL, which
        // the mapping IF treats as "not equal" and replaces every row, including rows that do not
        // match the search. A strict cast fails the load instead.
        Expression typedSearch = new Cast(search, complexType, false, true);
        return buildEquality(slot, typedSearch, complexType, true, column.getOriginType().toSql());
    }

    /**
     * Equality of two expressions of {@code type}, built from comparisons the engine can evaluate.
     * Primitive values and arrays of them are compared structurally ({@code compare_at}), which keeps
     * nested NULLs apart from values and treats two NULLs as equal like the engine does inside a
     * complex value; {@code <=>} expresses that for a nested value. MAP and STRUCT are rejected by
     * {@code ComparisonPredicate}, so their sub-values are compared recursively: a MAP is identified
     * by its deduplicated keys and values in order, and a STRUCT that cannot be serialized losslessly
     * enough for a whole JSON comparison both by its own NULL flag and by its fields. A STRUCT whose
     * JSON text is a lossless identity is compared as one value, and a STRUCT whose leaves have no
     * lossless JSON identity but are comparable recursively is compared by one engine call, so the
     * search is parsed a bounded number of times instead of once per field.
     *
     * <p>JSON is used only for a value the engine cannot compare structurally. A MAP is never
     * rendered as a JSON object: JSONB object keys are limited to 255 bytes and lose the key NULL
     * flag, so the empty string key and a NULL key would render the same. Nested MAPs are rejected
     * for the same reason.
     *
     * @param topLevel whether this is the whole search comparison. A NULL search then keeps the SQL
     *                 three-valued result of the text formats instead of matching a NULL value.
     */
    private static Expression buildEquality(Expression left, Expression right, DataType type,
            boolean topLevel, String columnTypeSql) throws AnalysisException {
        if (supportsStructuralComparison(type)) {
            // A top level ARRAY is compared by EqualTo, which is NULL when the search is NULL; a
            // nested value uses <=> so that two NULLs match, like compare_at inside a complex value.
            return topLevel ? new EqualTo(left, right) : new NullSafeEqual(left, right);
        }
        if (type.isMapType()) {
            MapType mapType = (MapType) type;
            DataType keyType = mapType.getKeyType();
            DataType valueType = mapType.getValueType();
            if (!supportsStructuralComparison(keyType) || !supportsMapValueComparison(valueType)) {
                throw unsupportedComplexSearch(columnTypeSql, type);
            }
            // An Arrow MAP keeps every entry, so a repeated key makes its key and value arrays
            // longer than the ones of the search text, which the string cast deduplicates with the
            // last value winning. The sink applies the same last-wins rule before it writes the
            // column, so normalize the native source the same way; otherwise a row that is written
            // as the search value would not match it and would not be replaced.
            Expression leftMap = deduplicateMap(left);
            // Only the top level search is the string cast, which already deduplicated its keys. A
            // MAP field of a STRUCT is parsed by the struct SerDe, which leaves repeated keys in
            // place, so it needs the same normalization as the source.
            Expression rightMap = topLevel ? right : deduplicateMap(right);
            List<Expression> equalities = Lists.newArrayList(
                    new NullSafeEqual(mapFunction("map_keys", leftMap), mapFunction("map_keys", rightMap)));
            if (supportsStructuralComparison(valueType)) {
                equalities.add(new NullSafeEqual(mapFunction("map_values", leftMap),
                        mapFunction("map_values", rightMap)));
            } else {
                // The value contains a STRUCT or an ARRAY of one, which the engine cannot compare as
                // an array element. Its JSON rendering is escaped, keeps NULL apart from the "null"
                // text and never uses the value as a JSON object key.
                equalities.add(new NullSafeEqual(jsonText(mapFunction("map_values", leftMap)),
                        jsonText(mapFunction("map_values", rightMap))));
            }
            Expression equality = new And(equalities);
            return topLevel ? guardNullSearch(right, equality) : equality;
        }
        if (type.isStructType()) {
            if (supportsJsonEncoding(type)) {
                // The engine rejects a direct STRUCT comparison, so its JSON text is the comparison
                // key. Compare the whole value at once: expanding the fields would embed the typed
                // search in every field comparison and each occurrence parses the whole search
                // string, which makes a dynamic search over an N field STRUCT convert N times the
                // N fields for every row.
                return topLevel
                        ? new EqualTo(jsonText(left), jsonText(right))
                        : new NullSafeEqual(jsonText(left), jsonText(right));
            }
            if (supportsDeepStructuralComparison(type)) {
                // The JSON text is not a lossless identity for this STRUCT (a float signed zero, a
                // version one DATE/DATETIME, DECIMALV2, ...), so the fields have to be compared
                // one by one. Doing that in the expression tree embeds the typed search in every
                // field comparison, and each occurrence parses the whole search string: an N field
                // STRUCT converts N fields N+1 times per row. One engine call compares both values
                // recursively instead, so the search is parsed once.
                Expression equality = complexEquals(left, right);
                return topLevel ? guardNullSearch(right, equality) : equality;
            }
            List<StructField> fields = ((StructType) type).getFields();
            List<Expression> equalities = Lists.newArrayListWithCapacity(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                equalities.add(buildEquality(
                        structField(left, i), structField(right, i),
                        fields.get(i).getDataType(), false, columnTypeSql));
            }
            Expression equality;
            if (equalities.isEmpty()) {
                equality = BooleanLiteral.TRUE;
            } else if (equalities.size() == 1) {
                equality = equalities.get(0);
            } else {
                equality = new And(equalities);
            }
            if (topLevel) {
                return guardNullSearch(right, equality);
            }
            // element_at merges a NULL parent STRUCT into every field, so the field comparisons
            // alone cannot tell a NULL nested STRUCT from a non-NULL one whose fields are all NULL.
            return new And(new EqualTo(new IsNull(left), new IsNull(right)), equality);
        }
        if (type.isArrayType() && supportsJsonEncoding(((ArrayType) type).getItemType())) {
            // An ARRAY of STRUCT is not comparable by the engine. Its JSON rendering is escaped and
            // keeps NULL apart from the "null" text, and object keys are never involved.
            return topLevel
                    ? new EqualTo(jsonText(left), jsonText(right))
                    : new NullSafeEqual(jsonText(left), jsonText(right));
        }
        throw unsupportedComplexSearch(columnTypeSql, type);
    }

    /**
     * The rewritten search comparison of a top level MAP or STRUCT: keep the text format behavior,
     * where a NULL search is unknown and therefore replaces the value. A nested value uses
     * {@code <=>}, so the comparison is only guarded where the search is the whole search.
     */
    private static Expression guardNullSearch(Expression search, Expression equality) {
        return new UnboundFunction("if", Lists.newArrayList(
                new IsNull(search), new NullLiteral(BooleanType.INSTANCE), equality));
    }

    private static Expression mapFunction(String name, Expression map) {
        return new UnboundFunction(name, Lists.newArrayList(map));
    }

    private static Expression deduplicateMap(Expression map) {
        // The BE function keeps the last value of a repeated key, like the sink and the string cast.
        return new UnboundFunction("deduplicate_map", Lists.newArrayList(map));
    }

    private static Expression complexEquals(Expression left, Expression right) {
        // The BE function compares two values of the same complex type recursively, with the NULL
        // semantics of a nested value (two NULLs are equal) and without parsing a search again.
        return new UnboundFunction("complex_equals", Lists.newArrayList(left, right));
    }

    private static Expression structField(Expression struct, int index) {
        // element_at resolves a STRUCT field by its 1-based constant index.
        return new UnboundFunction("element_at", Lists.newArrayList(struct, new IntegerLiteral(index + 1)));
    }

    private static Expression jsonText(Expression value) {
        // to_json returns JSONB, which is not comparable; its text is.
        return new Cast(new ToJson(value), StringType.INSTANCE);
    }

    private static AnalysisException unsupportedComplexSearch(String columnTypeSql, DataType type) {
        return new AnalysisException(String.format(
                "replace_value search on %s is not supported for Arrow files: no lossless comparison "
                        + "is available for %s",
                columnTypeSql, type.toSql()));
    }

    /**
     * Whether the engine has a structural comparison for the type. Mirrors
     * {@code TypeCoercionUtils#supportCompare(type, false)}: ARRAYs recurse into their item type, and
     * JSON, Variant and the non-primitive types are not comparable.
     */
    private static boolean supportsStructuralComparison(DataType type) {
        if (type.isArrayType()) {
            return supportsStructuralComparison(((ArrayType) type).getItemType());
        }
        return type.isPrimitive() && !type.isJsonType() && !type.isObjectOrVariantType();
    }

    /**
     * Whether a value can be compared by its JSON text. A MAP anywhere inside the value is rejected
     * because JSONB object keys are limited to 255 bytes and lose the key NULL flag; a STRUCT field
     * name is such a key and is checked at every level. Floats are rejected because the text keeps
     * the sign of zero while numeric equality does not, and TIMEV2/TIMESTAMPTZ below the maximum
     * scale because the text drops the stored microseconds that the Arrow reader keeps. The version
     * one DATE/DATETIME, DECIMALV2 and the sketch types have no JSONB serde.
     */
    private static boolean supportsJsonEncoding(DataType type) {
        if (type.isArrayType()) {
            return supportsJsonEncoding(((ArrayType) type).getItemType());
        }
        if (type.isMapType()) {
            return false;
        }
        if (type.isStructType()) {
            for (StructField field : ((StructType) type).getFields()) {
                // A nested STRUCT field name becomes a JSONB object key, which the BE serde limits
                // to 255 UTF-8 bytes.
                if (field.getName().getBytes(StandardCharsets.UTF_8).length > 255
                        || !supportsJsonEncoding(field.getDataType())) {
                    return false;
                }
            }
            return true;
        }
        // A float keeps its sign bit in the JSON text (-0.0 renders as "-0"), while numeric equality
        // treats -0.0 and 0.0 as equal, so their JSON text is not a lossless equality key.
        return type.isPrimitive() && !type.isFloatLikeType() && !type.isDateType()
                && !type.isDateTimeType() && !hasLossyJsonScale(type)
                && !type.isDecimalV2Type() && !type.isHllType()
                && !type.isBitmapType() && !type.isQuantileStateType() && !type.isAggStateType();
    }

    /**
     * Whether {@code Column::compare_at} can compare every leaf of the type, including nested
     * ARRAY and STRUCT values. A MAP is excluded because its comparison is order-sensitive and
     * does not deduplicate repeated keys, which the map branch handles explicitly; the sketch and
     * aggregate state types are excluded because their column only implements the comparison for
     * tests.
     */
    private static boolean supportsDeepStructuralComparison(DataType type) {
        if (type.isArrayType()) {
            return supportsDeepStructuralComparison(((ArrayType) type).getItemType());
        }
        if (type.isStructType()) {
            for (StructField field : ((StructType) type).getFields()) {
                if (!supportsDeepStructuralComparison(field.getDataType())) {
                    return false;
                }
            }
            return true;
        }
        if (type.isMapType()) {
            return false;
        }
        return type.isPrimitive() && !type.isJsonType() && !type.isObjectOrVariantType()
                && !type.isHllType() && !type.isBitmapType()
                && !type.isQuantileStateType() && !type.isAggStateType();
    }

    /**
     * Whether the JSON text of a scaled time type drops digits that the stored value keeps. The
     * Arrow reader stores the full microseconds of TIMEV2 and TIMESTAMPTZ whatever the declared
     * scale is, while their JSON text is written at that scale, so two different values can render
     * the same text. Only the maximum scale keeps every stored digit. DATETIMEV2 is not affected:
     * its reader rounds the payload to the declared scale before it is stored.
     */
    private static boolean hasLossyJsonScale(DataType type) {
        if (type.isTimeStampTzType()) {
            return ((TimeStampTzType) type).getScale() != TimeStampTzType.MAX_SCALE;
        }
        if (type.isTimeType()) {
            return ((TimeV2Type) type).getScale() != TimeV2Type.MAX_SCALE;
        }
        return false;
    }

    private static boolean supportsMapValueComparison(DataType type) {
        return supportsStructuralComparison(type) || supportsJsonEncoding(type);
    }
}
