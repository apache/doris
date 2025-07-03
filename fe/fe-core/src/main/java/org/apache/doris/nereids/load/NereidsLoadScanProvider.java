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

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.DefaultValue;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * process column mapping expressions, delete conditions and sequence columns
 */
public class NereidsLoadScanProvider {
    private static final Logger LOG = LogManager.getLogger(NereidsLoadScanProvider.class);
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
                    if (seqCol.getDefaultValue() == null
                            || !seqCol.getDefaultValue().equals(DefaultValue.CURRENT_TIMESTAMP)) {
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
        for (NereidsImportColumnDesc importColumnDesc : columnDescs) {
            String mappingColumnName = importColumnDesc.getColumnName();
            if (importColumnDesc.isColumn() || tbl.getColumn(mappingColumnName) != null) {
                copiedColumnExprs.add(importColumnDesc);
            }
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
                NereidsImportColumnDesc columnDesc;
                if (fileGroup.getFileFormatProperties().getFileFormatType() == TFileFormatType.FORMAT_JSON) {
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

        HashMap<String, Type> colToType = new HashMap<>();
        // check default value and auto-increment column
        for (Column column : tbl.getBaseSchema()) {
            if (fileGroupInfo.getUniqueKeyUpdateMode() == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS
                    && !partialUpdateInputColumns.contains(column.getName())) {
                continue;
            }
            String columnName = column.getName();
            colToType.put(columnName, column.getType());
            Expression expression = null;
            if (column.getGeneratedColumnInfo() != null) {
                // the generated column will be handled by bindSink
            } else {
                if (columnExprMap.get(columnName) != null) {
                    expression = columnExprMap.get(columnName);
                } else {
                    // other column with default value will be handled by bindSink
                }
            }
            if (expression != null) {
                // check hll_hash
                if (column.getDataType() == PrimitiveType.HLL) {
                    if (!(expression instanceof UnboundFunction)) {
                        throw new AnalysisException("HLL column must use " + FunctionSet.HLL_HASH + " function, like "
                                + columnName + "=" + FunctionSet.HLL_HASH + "(xxx)");
                    }
                    UnboundFunction function = (UnboundFunction) expression;
                    String functionName = function.getName();
                    if (!functionName.equalsIgnoreCase(FunctionSet.HLL_HASH)
                            && !functionName.equalsIgnoreCase("hll_empty")
                            && !functionName.equalsIgnoreCase(FunctionSet.HLL_FROM_BASE64)) {
                        throw new AnalysisException("HLL column must use " + FunctionSet.HLL_HASH + " function, like "
                                + columnName + "=" + FunctionSet.HLL_HASH + "(xxx) or "
                                + columnName + "=" + FunctionSet.HLL_FROM_BASE64 + "(xxx) or "
                                + columnName + "=hll_empty()");
                    }
                }

                if (fileGroup.isNegative() && column.getAggregationType() != null
                        && column.getAggregationType() == AggregateType.SUM) {
                    expression = new Multiply(expression, new IntegerLiteral(-1));
                }

                // check Bitmap Compatibility and check QuantileState Compatibility need be checked after binding
                // for jsonb type, use jsonb_parse_xxx to parse src string to jsonb.
                // and if input string is not a valid json string, return null. this need be handled after binding
                expression = ExpressionUtils.replace(expression, replaceMap);
                replaceMap.put(new UnboundSlot(columnName), expression);
                context.exprMap.put(column.getName(), expression);
            }
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

        // create scan SlotReferences and transform hadoop functions
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
                if (tblColumn.getGeneratedColumnInfo() == null) {
                    Expression expr = transformHadoopFunctionExpr(tbl, realColName, importColumnDesc.getExpr());
                    context.exprMap.put(realColName, expr);
                }
            } else {
                Column slotColumn;
                if (fileGroup.getFileFormatProperties().getFileFormatType() == TFileFormatType.FORMAT_ARROW) {
                    slotColumn = new Column(realColName, colToType.get(realColName), true);
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
     * When doing schema change, there may have some 'shadow' columns, with prefix '__doris_shadow_' in
     * their names. These columns are invisible to user, but we need to generate data for these columns.
     * So we add column mappings for these column.
     * eg1:
     * base schema is (A, B, C), and B is under schema change, so there will be a shadow column: '__doris_shadow_B'
     * So the final column mapping should looks like: (A, B, C, __doris_shadow_B = substitute(B));
     */
    private List<NereidsImportColumnDesc> getSchemaChangeShadowColumnDesc(Table tbl,
            Map<String, Expression> columnExprMap) {
        List<NereidsImportColumnDesc> shadowColumnDescs = Lists.newArrayList();
        for (Column column : tbl.getFullSchema()) {
            if (!column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                continue;
            }

            String originCol = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX);
            if (columnExprMap.containsKey(originCol)) {
                Expression mappingExpr = columnExprMap.get(originCol);
                if (mappingExpr != null) {
                    /*
                     * eg:
                     * (A, C) SET (B = func(xx))
                     * ->
                     * (A, C) SET (B = func(xx), __doris_shadow_B = func(xx))
                     */
                    NereidsImportColumnDesc importColumnDesc = new NereidsImportColumnDesc(column.getName(),
                            mappingExpr);
                    shadowColumnDescs.add(importColumnDesc);
                } else {
                    /*
                     * eg:
                     * (A, B, C)
                     * ->
                     * (A, B, C) SET (__doris_shadow_B = B)
                     */
                    UnboundSlot slot = new UnboundSlot(originCol);
                    //                    TODO: check if it's OK to remove setType
                    //                    slot.setType(column.getType());
                    NereidsImportColumnDesc importColumnDesc = new NereidsImportColumnDesc(column.getName(), slot);
                    shadowColumnDescs.add(importColumnDesc);
                }
            } else {
                /*
                 * There is a case that if user does not specify the related origin column, eg:
                 * COLUMNS (A, C), and B is not specified, but B is being modified
                 * so there is a shadow column '__doris_shadow_B'.
                 * We can not just add a mapping function "__doris_shadow_B = substitute(B)",
                 * because Doris can not find column B.
                 * In this case, __doris_shadow_B can use its default value, so no need to add it to column mapping
                 */
                // do nothing
            }
        }
        return shadowColumnDescs;
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
                        exprs.add(funcExpr.child(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                String exprSql = column.getDefaultValueExpr().toSql();
                                exprs.add(NereidsLoadUtils.parseExpressionSeq(exprSql).get(0));
                            } else {
                                exprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                exprs.add(new NullLiteral(VarcharType.SYSTEM_DEFAULT));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                } else {
                    // case 2
                    exprs.add(new Not(new IsNull(slot)));
                    List<Expression> innerIfExprs = Lists.newArrayList();
                    innerIfExprs.add(new Not(new EqualTo(slot, funcExpr.child(0))));
                    innerIfExprs.add(slot);
                    if (funcExpr.children().size() > 1) {
                        innerIfExprs.add(funcExpr.child(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                String exprSql = column.getDefaultValueExpr().toSql();
                                innerIfExprs.add(NereidsLoadUtils.parseExpressionSeq(exprSql).get(0));
                            } else {
                                innerIfExprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                innerIfExprs.add(new NullLiteral(VarcharType.SYSTEM_DEFAULT));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                    UnboundFunction innerIfFn = new UnboundFunction("if", innerIfExprs);
                    exprs.add(innerIfFn);
                    exprs.add(new NullLiteral(VarcharType.SYSTEM_DEFAULT));
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
                return funcExpr.child(0);
            } else if (funcName.equalsIgnoreCase("now")) {
                UnboundFunction newFunc = new UnboundFunction("now", Lists.newArrayList());
                return newFunc;
            } else if (funcName.equalsIgnoreCase("substitute")) {
                return funcExpr.child(0);
            }
        }
        return originExpr;
    }
}
