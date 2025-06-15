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

package org.apache.doris.load;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.GeneratedColumnInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ExprUtil;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class provides utility methods for transforming expressions in load statement
 */
public class LoadExprTransformUtils {
    private static final Logger LOG = LogManager.getLogger(LoadExprTransformUtils.class);

    /**
     * When doing schema change, there may have some 'shadow' columns, with prefix '__doris_shadow_' in
     * their names. These columns are invisible to user, but we need to generate data for these columns.
     * So we add column mappings for these column.
     * eg1:
     * base schema is (A, B, C), and B is under schema change, so there will be a shadow column: '__doris_shadow_B'
     * So the final column mapping should look like: (A, B, C, __doris_shadow_B = substitute(B));
     */
    public static List<ImportColumnDesc> getSchemaChangeShadowColumnDesc(Table tbl, Map<String, Expr> columnExprMap) {
        List<ImportColumnDesc> shadowColumnDescs = Lists.newArrayList();
        for (Column column : tbl.getFullSchema()) {
            if (!column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                continue;
            }

            String originCol = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX);
            if (columnExprMap.containsKey(originCol)) {
                Expr mappingExpr = columnExprMap.get(originCol);
                if (mappingExpr != null) {
                    /*
                     * eg:
                     * (A, C) SET (B = func(xx))
                     * ->
                     * (A, C) SET (B = func(xx), __doris_shadow_B = func(xx))
                     */
                    ImportColumnDesc importColumnDesc = new ImportColumnDesc(column.getName(), mappingExpr);
                    shadowColumnDescs.add(importColumnDesc);
                } else {
                    /*
                     * eg:
                     * (A, B, C)
                     * ->
                     * (A, B, C) SET (__doris_shadow_B = B)
                     */
                    SlotRef slot = new SlotRef(null, originCol);
                    slot.setType(column.getType());
                    ImportColumnDesc importColumnDesc = new ImportColumnDesc(column.getName(), slot);
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

    /*
     * This function should be used for broker load v2 and stream load.
     * And it must be called in same db lock when planing.
     */
    public static void initColumns(Table tbl, LoadTaskInfo.ImportColumnDescs columnDescs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction, Map<String, Expr> exprsByName,
            Analyzer analyzer, TupleDescriptor srcTupleDesc, Map<String, SlotDescriptor> slotDescByName,
            List<Integer> srcSlotIds, TFileFormatType formatType, List<String> hiddenColumns,
            TUniqueKeyUpdateMode uniquekeyUpdateMode)
            throws UserException {
        rewriteColumns(columnDescs);
        initColumns(tbl, columnDescs.descs, columnToHadoopFunction, exprsByName, analyzer, srcTupleDesc, slotDescByName,
                srcSlotIds, formatType, hiddenColumns, true, uniquekeyUpdateMode);
    }

    /*
     * This function will do followings:
     * 1. fill the column exprs if user does not specify any column or column mapping.
     * 2. For not specified columns, check if they have default value or they are auto-increment columns.
     * 3. Add any shadow columns if have.
     * 4. validate hadoop functions
     * 5. init slot descs and expr map for load plan
     */
    private static void initColumns(Table tbl, List<ImportColumnDesc> columnExprs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction, Map<String, Expr> exprsByName,
            Analyzer analyzer, TupleDescriptor srcTupleDesc, Map<String, SlotDescriptor> slotDescByName,
            List<Integer> srcSlotIds, TFileFormatType formatType, List<String> hiddenColumns,
            boolean needInitSlotAndAnalyzeExprs, TUniqueKeyUpdateMode uniquekeyUpdateMode) throws UserException {
        // We make a copy of the columnExprs so that our subsequent changes
        // to the columnExprs will not affect the original columnExprs.
        // skip the mapping columns not exist in schema
        // eg: the origin column list is:
        //          (k1, k2, tmpk3 = k1 + k2, k3 = tmpk3)
        //     after calling rewriteColumns(), it will become
        //          (k1, k2, tmpk3 = k1 + k2, k3 = k1 + k2)
        //     so "tmpk3 = k1 + k2" is not needed anymore, we can skip it.
        List<ImportColumnDesc> copiedColumnExprs = new ArrayList<>();
        for (ImportColumnDesc importColumnDesc : columnExprs) {
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
                // columnExprs has sequence column, don't need to generate the sequence column
                if (hasSequenceCol && column.isSequenceColumn()) {
                    continue;
                }
                ImportColumnDesc columnDesc = null;
                if (formatType == TFileFormatType.FORMAT_JSON) {
                    columnDesc = new ImportColumnDesc(column.getName());
                } else {
                    columnDesc = new ImportColumnDesc(column.getName().toLowerCase());
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("add base column {} to stream load task", column.getName());
                }
                copiedColumnExprs.add(columnDesc);
            }
            if (hasSkipBitmapColumn && uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS) {
                Preconditions.checkArgument(!specifyFileFieldNames);
                Preconditions.checkArgument(hiddenColumns == null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("add hidden column {} to stream load task", Column.DELETE_SIGN);
                }
                copiedColumnExprs.add(new ImportColumnDesc(Column.DELETE_SIGN));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("add hidden column {} to stream load task", Column.SKIP_BITMAP_COL);
                }
                // allow to specify __DORIS_SEQUENCE_COL__ if table has sequence type column
                if (hasSequenceCol && !hasSequenceMapCol) {
                    copiedColumnExprs.add(new ImportColumnDesc(Column.SEQUENCE_COL));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("add hidden column {} to stream load task", Column.SEQUENCE_COL);
                    }
                }
                copiedColumnExprs.add(new ImportColumnDesc(Column.SKIP_BITMAP_COL));
            }
            if (hiddenColumns != null) {
                for (String columnName : hiddenColumns) {
                    Column column = tbl.getColumn(columnName);
                    if (column != null && !column.isVisible()) {
                        ImportColumnDesc columnDesc = new ImportColumnDesc(column.getName());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("add hidden column {} to stream load task", column.getName());
                        }
                        copiedColumnExprs.add(columnDesc);
                    }
                }
            }
        }
        // generate a map for checking easily
        Map<String, Expr> columnExprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            columnExprMap.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
        }
        HashMap<String, Type> colToType = new HashMap<>();

        // check default value and auto-increment column
        for (Column column : tbl.getBaseSchema()) {
            String columnName = column.getName();
            colToType.put(columnName, column.getType());
            if (column.getGeneratedColumnInfo() != null) {
                GeneratedColumnInfo info = column.getGeneratedColumnInfo();
                exprsByName.put(column.getName(), info.getExpandExprForLoad());
                continue;
            }
            if (columnExprMap.containsKey(columnName)) {
                continue;
            }
            if (column.getDefaultValue() != null) {
                exprsByName.put(column.getName(), column.getDefaultValueExpr());
                continue;
            }
            if (column.isAllowNull()) {
                exprsByName.put(column.getName(), NullLiteral.create(column.getType()));
                continue;
            }
            if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
                continue;
            }
            if (column.isAutoInc()) {
                continue;
            }
            throw new DdlException("Column has no default value. column: " + columnName);
        }

        // get shadow column desc when table schema change
        copiedColumnExprs.addAll(getSchemaChangeShadowColumnDesc(tbl, columnExprMap));

        // validate hadoop functions
        if (columnToHadoopFunction != null) {
            Map<String, String> columnNameMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
                if (importColumnDesc.isColumn()) {
                    columnNameMap.put(importColumnDesc.getColumnName(), importColumnDesc.getColumnName());
                }
            }
            for (Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                String mappingColumnName = entry.getKey();
                Column mappingColumn = tbl.getColumn(mappingColumnName);
                if (mappingColumn == null) {
                    throw new DdlException("Mapping column is not in table. column: " + mappingColumnName);
                }
                Pair<String, List<String>> function = entry.getValue();
                try {
                    DataDescription.validateMappingFunction(function.first, function.second, columnNameMap,
                            mappingColumn, false);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
        }

        if (!needInitSlotAndAnalyzeExprs) {
            return;
        }
        Set<String> exprSrcSlotName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            if (importColumnDesc.isColumn()) {
                continue;
            }
            List<SlotRef> slots = Lists.newArrayList();
            importColumnDesc.getExpr().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                String slotColumnName = slot.getColumnName();
                exprSrcSlotName.add(slotColumnName);
            }
        }

        // init slot desc add expr map, also transform hadoop functions
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            // make column name case match with real column name
            String columnName = importColumnDesc.getColumnName();
            Column tblColumn = tbl.getColumn(columnName);
            String realColName;
            if (tblColumn == null || tblColumn.getName() == null || importColumnDesc.getExpr() == null) {
                realColName = columnName;
            } else {
                realColName = tblColumn.getName();
            }
            if (importColumnDesc.getExpr() != null) {
                Expr expr = transformHadoopFunctionExpr(tbl, realColName, importColumnDesc.getExpr());
                exprsByName.put(realColName, expr);
            } else {
                SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(srcTupleDesc);

                if (formatType == TFileFormatType.FORMAT_ARROW) {
                    slotDesc.setColumn(new Column(realColName, colToType.get(realColName)));
                } else {
                    if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS && hasSkipBitmapColumn) {
                        // we store the unique ids of missing columns in skip bitmap column in flexible partial update
                        int colUniqueId = tblColumn.getUniqueId();
                        Column slotColumn = null;
                        if (realColName.equals(Column.SKIP_BITMAP_COL)) {
                            // don't change the skip_bitmap_col's type to varchar becasue we will fill this column
                            // in NewJsonReader manually rather than reading them from files as varchar type and then
                            // converting them to their real type
                            slotColumn = new Column(realColName, PrimitiveType.BITMAP);
                        } else {
                            // columns default be varchar type
                            slotColumn = new Column(realColName, PrimitiveType.VARCHAR);
                        }
                        // In flexible partial update, every row can update different columns, we should check
                        // key columns intergrity for every row in XXXReader on BE rather than checking it on FE
                        // directly for all rows like in fixed columns partial update. So we should set if a slot
                        // is key column here
                        slotColumn.setIsKey(tblColumn.isKey());
                        slotColumn.setUniqueId(colUniqueId);
                        slotDesc.setAutoInc(tblColumn.isAutoInc());
                        slotDesc.setColumn(slotColumn);
                    } else {
                        // columns default be varchar type
                        slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                        slotDesc.setColumn(new Column(realColName, PrimitiveType.VARCHAR));
                    }
                }

                // ISSUE A: src slot should be nullable even if the column is not nullable.
                // because src slot is what we read from file, not represent to real column value.
                // If column is not nullable, error will be thrown when filling the dest slot,
                // which is not nullable.
                slotDesc.setIsNullable(true);

                slotDesc.setIsMaterialized(true);
                srcSlotIds.add(slotDesc.getId().asInt());
                slotDescByName.put(realColName, slotDesc);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("plan srcTupleDesc {}", srcTupleDesc.toString());
        }

        /*
         * The extension column of the materialized view is added to the expression evaluation of load
         * To avoid nested expressions. eg : column(a, tmp_c, c = expr(tmp_c)) ,
         * __doris_materialized_view_bitmap_union_c need be analyzed after exprsByName
         * So the columns of the materialized view are stored separately here
         */
        Map<String, Expr> mvDefineExpr = Maps.newHashMap();
        for (Column column : tbl.getFullSchema()) {
            if (column.getDefineExpr() != null) {
                if (column.getDefineExpr().getType().isInvalid()) {
                    column.getDefineExpr().setType(column.getType());
                }
                mvDefineExpr.put(column.getName(), column.getDefineExpr());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("slotDescByName: {}, exprsByName: {}, mvDefineExpr: {}",
                    slotDescByName, exprsByName, mvDefineExpr);
        }

        // in vectorized load, reanalyze exprs with castExpr type
        // otherwise analyze exprs with varchar type
        analyzeAllExprs(tbl, analyzer, exprsByName, mvDefineExpr, slotDescByName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("after init column, exprMap: {}", exprsByName);
        }
    }

    private static SlotRef getSlotFromDesc(SlotDescriptor slotDesc) {
        SlotRef slot = new SlotRef(slotDesc);
        slot.setType(slotDesc.getType());
        return slot;
    }

    public static Expr getExprFromDesc(Analyzer analyzer, Expr rhs, SlotRef slot) throws AnalysisException {
        Type rhsType = rhs.getType();
        rhs = rhs.castTo(slot.getType());

        if (slot.getDesc() == null) {
            // shadow column
            return rhs;
        }

        if (rhs.isNullable() && !slot.isNullable()) {
            rhs = new FunctionCallExpr("non_nullable", Lists.newArrayList(rhs));
            rhs.setType(rhsType);
            rhs.analyze(analyzer);
        } else if (!rhs.isNullable() && slot.isNullable()) {
            rhs = new FunctionCallExpr("nullable", Lists.newArrayList(rhs));
            rhs.setType(rhsType);
            rhs.analyze(analyzer);
        }
        return rhs;
    }

    private static void analyzeAllExprs(Table tbl, Analyzer analyzer, Map<String, Expr> exprsByName,
            Map<String, Expr> mvDefineExpr, Map<String, SlotDescriptor> slotDescByName) throws UserException {
        // analyze all exprs
        for (Map.Entry<String, Expr> entry : exprsByName.entrySet()) {
            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            List<SlotRef> slots = Lists.newArrayList();
            entry.getValue().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                SlotDescriptor slotDesc = slotDescByName.get(slot.getColumnName());
                if (slotDesc == null) {
                    if (entry.getKey() != null) {
                        if (entry.getKey().equalsIgnoreCase(Column.DELETE_SIGN)) {
                            throw new UserException("unknown reference column in DELETE ON clause:"
                                    + slot.getColumnName());
                        } else if (entry.getKey().equalsIgnoreCase(Column.SEQUENCE_COL)) {
                            throw new UserException("unknown reference column in ORDER BY clause:"
                                    + slot.getColumnName());
                        }
                    }
                    throw new UserException("unknown reference column, column=" + entry.getKey()
                            + ", reference=" + slot.getColumnName());
                }
                smap.getLhs().add(slot);
                smap.getRhs().add(new SlotRef(slotDesc));
            }
            Expr expr = entry.getValue().clone(smap);
            expr.analyze(analyzer);

            // check if contain aggregation
            List<FunctionCallExpr> funcs = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, funcs);
            for (FunctionCallExpr fn : funcs) {
                if (fn.isAggregateFunction()) {
                    throw new AnalysisException("Don't support aggregation function in load expression");
                }
            }

            // Array/Map/Struct type do not support cast now
            Type exprReturnType = expr.getType();
            if (exprReturnType.isComplexType()) {
                Type schemaType = tbl.getColumn(entry.getKey()).getType();
                if (!exprReturnType.matchesType(schemaType)) {
                    throw new AnalysisException("Don't support load from type:" + exprReturnType + " to type:"
                            + schemaType + " for column:" + entry.getKey());
                }
            }

            exprsByName.put(entry.getKey(), expr);
        }

        for (Map.Entry<String, Expr> entry : mvDefineExpr.entrySet()) {
            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            List<SlotRef> slots = Lists.newArrayList();
            entry.getValue().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                if (exprsByName.get(slot.getColumnName()) != null) {
                    smap.getLhs().add(slot);
                    if (tbl.getColumn(slot.getColumnName()).getType()
                            .equals(exprsByName.get(slot.getColumnName()).getType())) {
                        smap.getRhs().add(exprsByName.get(slot.getColumnName()));
                    } else {
                        smap.getRhs().add(new CastExpr(tbl.getColumn(slot.getColumnName()).getType(),
                                exprsByName.get(slot.getColumnName())));
                    }
                } else if (slotDescByName.get(slot.getColumnName()) != null) {
                    smap.getLhs().add(slot);
                    smap.getRhs().add(
                            getExprFromDesc(analyzer, getSlotFromDesc(slotDescByName.get(slot.getColumnName())), slot));
                } else {
                    if (entry.getKey().equalsIgnoreCase(Column.DELETE_SIGN)) {
                        throw new UserException("unknown reference column in DELETE ON clause:" + slot.getColumnName());
                    } else if (entry.getKey().equalsIgnoreCase(Column.SEQUENCE_COL)) {
                        throw new UserException("unknown reference column in ORDER BY clause:" + slot.getColumnName());
                    } else {
                        throw new UserException("unknown reference column, column=" + entry.getKey()
                                + ", reference=" + slot.getColumnName());
                    }
                }
            }
            Expr expr = entry.getValue().clone(smap);
            expr.analyze(analyzer);

            exprsByName.put(entry.getKey(), expr);
        }
    }

    public static void rewriteColumns(LoadTaskInfo.ImportColumnDescs columnDescs) {
        if (columnDescs.isColumnDescsRewrited) {
            return;
        }

        Map<String, Expr> derivativeColumns = new HashMap<>();
        // find and rewrite the derivative columns
        // e.g. (v1,v2=v1+1,v3=v2+1) --> (v1, v2=v1+1, v3=v1+1+1)
        // 1. find the derivative columns
        for (ImportColumnDesc importColumnDesc : columnDescs.descs) {
            if (!importColumnDesc.isColumn()) {
                if (importColumnDesc.getExpr() instanceof SlotRef) {
                    String columnName = ((SlotRef) importColumnDesc.getExpr()).getColumnName();
                    if (derivativeColumns.containsKey(columnName)) {
                        importColumnDesc.setExpr(derivativeColumns.get(columnName));
                    }
                } else {
                    ExprUtil.recursiveRewrite(importColumnDesc.getExpr(), derivativeColumns);
                }
                derivativeColumns.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
            }
        }

        columnDescs.isColumnDescsRewrited = true;
    }

    /**
     * This method is used to transform hadoop function.
     * The hadoop function includes: replace_value, strftime, time_format, alignment_timestamp, default_value, now.
     * It rewrites those function with real function name and param.
     * For the other function, the expr only go through this function and the origin expr is returned.
     *
     * @param columnName
     * @param originExpr
     * @return
     * @throws UserException
     */
    private static Expr transformHadoopFunctionExpr(Table tbl, String columnName, Expr originExpr)
            throws UserException {
        Column column = tbl.getColumn(columnName);
        if (column == null) {
            // the unknown column will be checked later.
            return originExpr;
        }

        // To compatible with older load version
        if (originExpr instanceof FunctionCallExpr) {
            FunctionCallExpr funcExpr = (FunctionCallExpr) originExpr;
            String funcName = funcExpr.getFnName().getFunction();

            if (funcName.equalsIgnoreCase("replace_value")) {
                List<Expr> exprs = Lists.newArrayList();
                SlotRef slotRef = new SlotRef(null, columnName);
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
                if (funcExpr.getChild(0) instanceof NullLiteral) {
                    // case 1
                    exprs.add(new IsNullPredicate(slotRef, true));
                    exprs.add(slotRef);
                    if (funcExpr.hasChild(1)) {
                        exprs.add(funcExpr.getChild(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                exprs.add(column.getDefaultValueExpr());
                            } else {
                                exprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                exprs.add(NullLiteral.create(Type.VARCHAR));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                } else {
                    // case 2
                    exprs.add(new IsNullPredicate(slotRef, true));
                    List<Expr> innerIfExprs = Lists.newArrayList();
                    innerIfExprs.add(new BinaryPredicate(BinaryPredicate.Operator.NE, slotRef, funcExpr.getChild(0)));
                    innerIfExprs.add(slotRef);
                    if (funcExpr.hasChild(1)) {
                        innerIfExprs.add(funcExpr.getChild(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                innerIfExprs.add(column.getDefaultValueExpr());
                            } else {
                                innerIfExprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                innerIfExprs.add(NullLiteral.create(Type.VARCHAR));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                    FunctionCallExpr innerIfFn = new FunctionCallExpr("if", innerIfExprs);
                    exprs.add(innerIfFn);
                    exprs.add(NullLiteral.create(Type.VARCHAR));
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("replace_value expr: {}", exprs);
                }
                FunctionCallExpr newFn = new FunctionCallExpr("if", exprs);
                return newFn;
            } else if (funcName.equalsIgnoreCase("strftime")) {
                // FROM_UNIXTIME(val)
                FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                        fromUnixName, new FunctionParams(false, fromUnixArgs));

                return fromUnixFunc;
            } else if (funcName.equalsIgnoreCase("time_format")) {
                // DATE_FORMAT(STR_TO_DATE(dt_str, dt_fmt))
                FunctionName strToDateName = new FunctionName("STR_TO_DATE");
                List<Expr> strToDateExprs = Lists.newArrayList(funcExpr.getChild(2), funcExpr.getChild(1));
                FunctionCallExpr strToDateFuncExpr = new FunctionCallExpr(
                        strToDateName, new FunctionParams(false, strToDateExprs));

                FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                List<Expr> dateFormatArgs = Lists.newArrayList(strToDateFuncExpr, funcExpr.getChild(0));
                FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                        dateFormatName, new FunctionParams(false, dateFormatArgs));

                return dateFormatFunc;
            } else if (funcName.equalsIgnoreCase("alignment_timestamp")) {
                /*
                 * change to:
                 * UNIX_TIMESTAMP(DATE_FORMAT(FROM_UNIXTIME(ts), "%Y-01-01 00:00:00"));
                 *
                 */

                // FROM_UNIXTIME
                FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                        fromUnixName, new FunctionParams(false, fromUnixArgs));

                // DATE_FORMAT
                StringLiteral precision = (StringLiteral) funcExpr.getChild(0);
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
                FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                List<Expr> dateFormatArgs = Lists.newArrayList(fromUnixFunc, format);
                FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                        dateFormatName, new FunctionParams(false, dateFormatArgs));

                // UNIX_TIMESTAMP
                FunctionName unixTimeName = new FunctionName("UNIX_TIMESTAMP");
                List<Expr> unixTimeArgs = Lists.newArrayList();
                unixTimeArgs.add(dateFormatFunc);
                FunctionCallExpr unixTimeFunc = new FunctionCallExpr(
                        unixTimeName, new FunctionParams(false, unixTimeArgs));

                return unixTimeFunc;
            } else if (funcName.equalsIgnoreCase("default_value")) {
                return funcExpr.getChild(0);
            } else if (funcName.equalsIgnoreCase("now")) {
                FunctionName nowFunctionName = new FunctionName("NOW");
                FunctionCallExpr newFunc = new FunctionCallExpr(nowFunctionName, new FunctionParams(null));
                return newFunc;
            } else if (funcName.equalsIgnoreCase("substitute")) {
                return funcExpr.getChild(0);
            }
        }
        return originExpr;
    }
}
