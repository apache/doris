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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.mvrewrite.CountFieldToSum;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Materialized view is performed to materialize the results of query.
 * This clause is used to create a new materialized view for a specified table
 * through a specified query stmt.
 * <p>
 * Syntax:
 * CREATE MATERIALIZED VIEW [MV name] (
 * SELECT select_expr[, select_expr ...]
 * FROM [Base view name]
 * GROUP BY column_name[, column_name ...]
 * ORDER BY column_name[, column_name ...])
 * [PROPERTIES ("key" = "value")]
 */
public class CreateMaterializedViewStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateMaterializedViewStmt.class);

    public static final String MATERIALIZED_VIEW_NAME_PREFIX = "mv_";
    public static final String MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX = "mva_";
    public static final String MATERIALIZED_VIEW_AGGREGATE_NAME_LINK = "__";
    public static final Map<String, MVColumnPattern> FN_NAME_TO_PATTERN;

    static {
        FN_NAME_TO_PATTERN = Maps.newHashMap();
        FN_NAME_TO_PATTERN.put(AggregateType.SUM.name().toLowerCase(),
                new MVColumnOneChildPattern(AggregateType.SUM.name().toLowerCase()));
        FN_NAME_TO_PATTERN.put(AggregateType.MIN.name().toLowerCase(),
                new MVColumnOneChildPattern(AggregateType.MIN.name().toLowerCase()));
        FN_NAME_TO_PATTERN.put(AggregateType.MAX.name().toLowerCase(),
                new MVColumnOneChildPattern(AggregateType.MAX.name().toLowerCase()));
        FN_NAME_TO_PATTERN.put(FunctionSet.COUNT, new MVColumnOneChildPattern(FunctionSet.COUNT));
        FN_NAME_TO_PATTERN.put(FunctionSet.BITMAP_UNION, new MVColumnBitmapUnionPattern());
        FN_NAME_TO_PATTERN.put(FunctionSet.HLL_UNION, new MVColumnHLLUnionPattern());
    }

    public static final ImmutableSet<String> invalidFn = ImmutableSet.of("now", "current_time", "current_date",
            "utc_timestamp", "uuid", "random", "unix_timestamp", "curdate");

    private String mvName;
    private SelectStmt selectStmt;
    private Map<String, String> properties;

    private int beginIndexOfAggregation = -1;
    /**
     * origin stmt: select k1, k2, v1, sum(v2) from base_table group by k1, k2, v1
     * mvColumnItemList: [k1: {name: k1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * k2: {name: k2, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * v1: {name: v1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     * v2: {name: v2, isKey: false, aggType: sum, isAggregationTypeImplicit: false}]
     * This order of mvColumnItemList is meaningful.
     */
    private List<MVColumnItem> mvColumnItemList = Lists.newArrayList();
    MVColumnItem whereClauseItem;
    private String baseIndexName;
    private String dbName;
    private KeysType mvKeysType = KeysType.DUP_KEYS;
    //if process is replaying log, isReplay is true, otherwise is false, avoid replay process error report,
    // only in Rollup or MaterializedIndexMeta is true
    private boolean isReplay = false;

    public CreateMaterializedViewStmt(String mvName, SelectStmt selectStmt, Map<String, String> properties) {
        this.mvName = mvName;
        this.selectStmt = selectStmt;
        this.properties = properties;
    }

    public void setIsReplay(boolean isReplay) {
        this.isReplay = isReplay;
    }

    public boolean isReplay() {
        return isReplay;
    }

    public String getMVName() {
        return mvName;
    }

    public SelectStmt getSelectStmt() {
        return selectStmt;
    }

    public List<MVColumnItem> getMVColumnItemList() {
        return mvColumnItemList;
    }

    public String getBaseIndexName() {
        return baseIndexName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getDBName() {
        return dbName;
    }

    public void setMVKeysType(KeysType type) {
        mvKeysType = type;
    }

    public KeysType getMVKeysType() {
        return mvKeysType;
    }

    public Column getWhereClauseItemExpr(OlapTable olapTable) throws DdlException {
        if (whereClauseItem == null) {
            return null;
        }
        return whereClauseItem.toMVColumn(olapTable);
    }

    public Expr getWhereClause() {
        return selectStmt.getWhereClause();
    }

    private void checkExprValidInMv(Expr expr, String functionName) throws AnalysisException {
        if (!isReplay && expr.haveFunction(functionName)) {
            throw new AnalysisException("The materialized view contain " + functionName + " is disallowed");
        }
    }

    private void checkExprValidInMv(Expr expr) throws AnalysisException {
        if (isReplay) {
            return;
        }
        for (String function : invalidFn) {
            checkExprValidInMv(expr, function);
        }
    }

    private void checkExprValidInMv() throws AnalysisException {
        if (selectStmt.getWhereClause() != null) {
            checkExprValidInMv(selectStmt.getWhereClause());
        }
        SelectList selectList = selectStmt.getSelectList();
        for (SelectListItem selectListItem : selectList.getItems()) {
            checkExprValidInMv(selectListItem.getExpr());
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        checkExprValidInMv();

        FeNameFormat.checkTableName(mvName);
        rewriteToBitmapWithCheck();
        // TODO(ml): The mv name in from clause should pass the analyze without error.
        selectStmt.forbiddenMVRewrite();
        selectStmt.analyze(analyzer);

        ExprRewriter rewriter = analyzer.getExprRewriter();
        rewriter.reset();
        selectStmt.rewriteExprs(rewriter);
        selectStmt.reset();
        analyzer = new Analyzer(analyzer.getEnv(), analyzer.getContext());
        selectStmt.analyze(analyzer);

        if (selectStmt.getAggInfo() != null) {
            mvKeysType = KeysType.AGG_KEYS;
        }
        analyzeSelectClause(analyzer);
        analyzeFromClause();
        if (selectStmt.getWhereClause() != null) {
            if (!isReplay && selectStmt.getWhereClause().hasAggregateSlot()) {
                throw new AnalysisException(
                        "The where clause contained aggregate column is not supported, expr:"
                                + selectStmt.getWhereClause().toSql());
            }
            whereClauseItem = new MVColumnItem(selectStmt.getWhereClause());
        }
        if (selectStmt.getHavingPred() != null) {
            throw new AnalysisException("The having clause is not supported in add materialized view clause, expr:"
                    + selectStmt.getHavingPred().toSql());
        }
        analyzeOrderByClause();
        analyzeGroupByClause();
        if (selectStmt.getLimit() != -1) {
            throw new AnalysisException("The limit clause is not supported in add materialized view clause, expr:"
                    + " limit " + selectStmt.getLimit());
        }
    }

    public void analyzeSelectClause(Analyzer analyzer) throws AnalysisException {
        SelectList selectList = selectStmt.getSelectList();
        if (selectList.getItems().isEmpty()) {
            throw new AnalysisException("The materialized view must contain at least one column");
        }
        boolean meetAggregate = false;
        /**
         * 1. The columns of mv must be a single column or a aggregate column without any calculate.
         *    Also the children of aggregate column must be a single column without any calculate.
         *    For example:
         *        a, sum(b) is legal.
         *        a+b, sum(a+b) is illegal.
         * 2. The SUM, MIN, MAX function is supported. The other function will be supported in the future.
         * 3. The aggregate column must be declared after the single column.
         */
        for (int i = 0; i < selectList.getItems().size(); i++) {
            SelectListItem selectListItem = selectList.getItems().get(i);

            if (selectListItem.isStar()) {
                throw new AnalysisException("The materialized view not support select star");
            }

            Expr selectListItemExpr = selectListItem.getExpr();
            if (!(selectListItemExpr instanceof SlotRef) && !(selectListItemExpr instanceof FunctionCallExpr)
                    && !(selectListItemExpr instanceof ArithmeticExpr)) {
                throw new AnalysisException("The materialized view only support the single column or function expr. "
                        + "Error column: " + selectListItemExpr.toSql());
            }
            List<SlotRef> slots = new ArrayList<>();
            selectListItemExpr.collect(SlotRef.class, slots);
            if (!isReplay && slots.size() == 0) {
                throw new AnalysisException(
                        "The materialized view contain constant expr is disallowed, expr: "
                                + selectListItemExpr.toSql());
            }


            if (selectListItemExpr instanceof FunctionCallExpr
                    && ((FunctionCallExpr) selectListItemExpr).isAggregateFunction()) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;

                if (beginIndexOfAggregation == -1) {
                    beginIndexOfAggregation = i;
                }
                meetAggregate = true;
                // build mv column item
                mvColumnItemList.add(buildMVColumnItem(analyzer, functionCallExpr));
            } else {
                if (meetAggregate) {
                    throw new AnalysisException("The aggregate column should be after the single column");
                }
                MVColumnItem mvColumnItem = new MVColumnItem(selectListItemExpr);
                mvColumnItemList.add(mvColumnItem);
            }
        }
        // TODO(ml): only value columns of materialized view, such as select sum(v1) from table
        if (beginIndexOfAggregation == 0) {
            throw new AnalysisException("The materialized view must contain at least one key column");
        }
    }

    private void analyzeFromClause() throws AnalysisException {
        List<TableRef> tableRefList = selectStmt.getTableRefs();
        if (tableRefList.size() != 1) {
            throw new AnalysisException("The materialized view only support one table in from clause.");
        }
        if (!isReplay && tableRefList.get(0).hasExplicitAlias()) {
            throw new AnalysisException("The materialized view not support table with alias.");
        }
        TableName tableName = tableRefList.get(0).getName();
        if (tableName == null) {
            throw new AnalysisException("table in from clause is invalid, please check if it's single table "
                    + "and not sub-query");
        }
        baseIndexName = tableName.getTbl();
        dbName = tableName.getDb();
    }

    private void analyzeGroupByClause() throws AnalysisException {
        if (isReplay || selectStmt.getGroupByClause() == null) {
            return;
        }
        List<Expr> groupingExprs = selectStmt.getGroupByClause().getGroupingExprs();
        List<FunctionCallExpr> aggregateExprs = selectStmt.getAggInfo().getAggregateExprs();
        List<Expr> selectExprs = selectStmt.getSelectList().getExprs();
        for (Expr expr : selectExprs) {
            boolean match = false;
            String lhs = selectStmt.getExprFromAliasSMap(expr).toSqlWithoutTbl();
            for (Expr groupExpr : groupingExprs) {
                String rhs = selectStmt.getExprFromAliasSMap(groupExpr).toSqlWithoutTbl();
                if (lhs.equalsIgnoreCase(rhs)) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                for (Expr groupExpr : aggregateExprs) {
                    String rhs = selectStmt.getExprFromAliasSMap(groupExpr).toSqlWithoutTbl();
                    if (lhs.equalsIgnoreCase(rhs)) {
                        match = true;
                        break;
                    }
                }
            }

            if (!match) {
                throw new AnalysisException("The select expr " + lhs + " not in grouping or aggregate columns");
            }
        }

        for (Expr groupExpr : groupingExprs) {
            boolean match = false;
            String rhs = selectStmt.getExprFromAliasSMap(groupExpr).toSqlWithoutTbl();
            for (Expr expr : selectExprs) {
                String lhs = selectStmt.getExprFromAliasSMap(expr).toSqlWithoutTbl();
                if (lhs.equalsIgnoreCase(rhs)) {
                    match = true;
                    break;
                }
            }

            if (!match) {
                throw new AnalysisException("The grouping expr " + rhs + " not in select list.");
            }
        }
    }

    private void analyzeOrderByClause() throws AnalysisException {
        if (selectStmt.getOrderByElements() == null) {
            supplyOrderColumn();
            return;
        }

        List<OrderByElement> orderByElements = selectStmt.getOrderByElements();
        if (orderByElements.size() > mvColumnItemList.size()) {
            throw new AnalysisException("The number of columns in order clause must be less than " + "the number of "
                    + "columns in select clause");
        }
        if (beginIndexOfAggregation != -1 && (orderByElements.size() != (beginIndexOfAggregation))) {
            throw new AnalysisException("The key of columns in mv must be all of group by columns");
        }
        for (int i = 0; i < orderByElements.size(); i++) {
            Expr orderByElement = selectStmt.getExprFromAliasSMapDirect(orderByElements.get(i).getExpr());

            MVColumnItem mvColumnItem = mvColumnItemList.get(i);
            if (mvColumnItem.getName() == null) {
                throw new AnalysisException("mvColumnItem.getName() is null");
            }

            if (!mvColumnItem.getDefineExpr().equals(orderByElement)) {
                throw new AnalysisException("The order of columns in order by clause must be same as "
                        + "the order of columns in select list, " + mvColumnItem.getDefineExpr().toSql() + " vs "
                        + orderByElement.toSql());
            }
            Preconditions.checkState(mvColumnItem.getAggregationType() == null);
            mvColumnItem.setIsKey(true);
        }

        // supplement none aggregate type
        for (MVColumnItem mvColumnItem : mvColumnItemList) {
            if (mvColumnItem.isKey()) {
                continue;
            }
            if (mvColumnItem.getAggregationType() != null) {
                break;
            }
            mvColumnItem.setAggregationType(AggregateType.NONE, true);
        }
    }

    /*
    This function is used to supply order by columns and calculate short key count
     */
    private void supplyOrderColumn() throws AnalysisException {
        /**
         * The keys type of Materialized view is aggregation.
         * All of group by columns are keys of materialized view.
         */
        if (mvKeysType == KeysType.AGG_KEYS) {
            for (MVColumnItem mvColumnItem : mvColumnItemList) {
                if (mvColumnItem.getAggregationType() != null) {
                    break;
                }
                mvColumnItem.setIsKey(true);
            }
        } else if (mvKeysType == KeysType.DUP_KEYS) {
            /**
             * There is no aggregation function in materialized view.
             * Supplement key of MV columns
             * The key is same as the short key in duplicate table
             * For example: select k1, k2 ... kn from t1
             * The default key columns are first 36 bytes of the columns in define order.
             * If the number of columns in the first 36 is more than 3, the first 3 columns will be used.
             * column: k1, k2, k3. The key is true.
             * Supplement non-key of MV columns
             * column: k4... kn. The key is false, aggregation type is none, isAggregationTypeImplicit is true.
             */
            int theBeginIndexOfValue = 0;
            // supply key
            int keySizeByte = 0;
            for (; theBeginIndexOfValue < mvColumnItemList.size(); theBeginIndexOfValue++) {
                MVColumnItem column = mvColumnItemList.get(theBeginIndexOfValue);
                keySizeByte += column.getType().getIndexSize();
                if (theBeginIndexOfValue + 1 > FeConstants.shortkey_max_column_count
                        || keySizeByte > FeConstants.shortkey_maxsize_bytes) {
                    if (theBeginIndexOfValue == 0 && column.getType().getPrimitiveType().isCharFamily()) {
                        column.setIsKey(true);
                        theBeginIndexOfValue++;
                    }
                    break;
                }
                if (column.getType().isFloatingPointType()) {
                    break;
                }
                if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                    column.setIsKey(true);
                    theBeginIndexOfValue++;
                    break;
                }
                column.setIsKey(true);
            }
            if (theBeginIndexOfValue == 0) {
                throw new AnalysisException("The first column could not be float or double type, use decimal instead");
            }
            // supply value
            for (; theBeginIndexOfValue < mvColumnItemList.size(); theBeginIndexOfValue++) {
                MVColumnItem mvColumnItem = mvColumnItemList.get(theBeginIndexOfValue);
                mvColumnItem.setAggregationType(AggregateType.NONE, true);
            }
        }
    }

    private MVColumnItem buildMVColumnItem(Analyzer analyzer, FunctionCallExpr functionCallExpr)
            throws AnalysisException {
        String functionName = functionCallExpr.getFnName().getFunction();
        List<Expr> childs = functionCallExpr.getChildren();
        Expr defineExpr = childs.get(0);
        Type baseType = defineExpr.getType();
        AggregateType mvAggregateType = null;
        Type type;
        switch (functionName.toLowerCase()) {
            case "sum":
                mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
                PrimitiveType baseColumnType = baseType.getPrimitiveType();
                if (baseColumnType == PrimitiveType.TINYINT || baseColumnType == PrimitiveType.SMALLINT
                        || baseColumnType == PrimitiveType.INT) {
                    type = Type.BIGINT;
                } else if (baseColumnType == PrimitiveType.FLOAT) {
                    type = Type.DOUBLE;
                } else {
                    type = baseType;
                }
                if (type != baseType) {
                    defineExpr = new CastExpr(type, defineExpr);
                    if (analyzer != null) {
                        defineExpr.analyze(analyzer);
                    }
                }
                break;
            case "min":
            case "max":
                type = baseType;
                break;
            case FunctionSet.BITMAP_UNION:
                type = Type.BITMAP;
                if (!isReplay && analyzer != null && !baseType.isBitmapType()) {
                    throw new AnalysisException(
                            "BITMAP_UNION need input a bitmap column, but input " + baseType.toString());
                }
                break;
            case FunctionSet.HLL_UNION:
                type = Type.HLL;
                if (!isReplay && analyzer != null && !baseType.isHllType()) {
                    throw new AnalysisException("HLL_UNION need input a hll column, but input " + baseType.toString());
                }
                break;
            case FunctionSet.COUNT:
                mvAggregateType = AggregateType.SUM;
                defineExpr = CountFieldToSum.slotToCaseWhen(defineExpr);
                if (analyzer != null) {
                    defineExpr.analyze(analyzer);
                }
                type = Type.BIGINT;
                break;
            default:
                mvAggregateType = AggregateType.GENERIC_AGGREGATION;
                if (functionCallExpr.getParams().isDistinct() || functionCallExpr.getParams().isStar()) {
                    throw new AnalysisException(
                            "The Materialized-View's generic aggregation not support star or distinct");
                }
                defineExpr = Function.convertToStateCombinator(functionCallExpr);
                type = defineExpr.type;
        }
        if (mvAggregateType == null) {
            mvAggregateType = AggregateType.valueOf(functionName.toUpperCase());
        }

        if (!isReplay && defineExpr.hasAggregateSlot()) {
            SlotRef slot = null;
            if (defineExpr instanceof SlotRef) {
                slot = (SlotRef) defineExpr;
            } else if (defineExpr instanceof CastExpr && defineExpr.getChild(0) instanceof SlotRef) {
                slot = (SlotRef) defineExpr.getChild(0);
            } else {
                throw new AnalysisException(
                        "Aggregate function require single slot argument, invalid argument is: " + defineExpr.toSql());
            }

            AggregateType input = slot.getColumn().getAggregationType();
            if (!input.equals(mvAggregateType)) {
                throw new AnalysisException("Aggregate function require same with slot aggregate type, input: "
                        + input.name() + ", required: " + mvAggregateType.name());
            }
        }
        return new MVColumnItem(type, mvAggregateType, defineExpr, mvColumnBuilder(defineExpr.toSql()));
    }

    public Map<String, Expr> parseDefineExpr(Analyzer analyzer) throws AnalysisException {
        Map<String, Expr> result = Maps.newHashMap();
        SelectList selectList = selectStmt.getSelectList();
        for (SelectListItem selectListItem : selectList.getItems()) {
            Expr selectListItemExpr = selectListItem.getExpr();
            Expr expr = selectListItemExpr;
            String name = mvColumnBuilder(MaterializedIndexMeta.normalizeName(expr.toSql()));
            if (selectListItemExpr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItemExpr;
                switch (functionCallExpr.getFnName().getFunction().toLowerCase()) {
                    case "sum":
                    case "min":
                    case "max":
                    case FunctionSet.BITMAP_UNION:
                    case FunctionSet.HLL_UNION:
                    case FunctionSet.COUNT:
                        MVColumnItem item = buildMVColumnItem(analyzer, functionCallExpr);
                        expr = item.getDefineExpr();
                        name = item.getName();
                        break;
                    default:
                        if (Env.getCurrentEnv()
                                .isAggFunctionName(functionCallExpr.getFnName().getFunction().toLowerCase())) {
                            MVColumnItem genericItem = buildMVColumnItem(analyzer, functionCallExpr);
                            expr = genericItem.getDefineExpr();
                            name = genericItem.getName();
                        }
                        break;
                }
            }
            result.put(name, expr);
        }
        return result;
    }

    // for bitmap_union(to_bitmap(column)) function, we should check value is not
    // negative
    // in vectorized schema_change mode, so we should rewrite the function to
    // bitmap_union(to_bitmap_with_check(column))
    public void rewriteToBitmapWithCheck() {
        for (SelectListItem item : selectStmt.getSelectList().getItems()) {
            if (item.getExpr() instanceof FunctionCallExpr) {
                String functionName = ((FunctionCallExpr) item.getExpr()).getFnName().getFunction();
                if (functionName.equalsIgnoreCase("bitmap_union")) {
                    if (item.getExpr().getChildren().size() == 1
                            && item.getExpr().getChild(0) instanceof FunctionCallExpr) {
                        FunctionCallExpr childFunctionCallExpr = (FunctionCallExpr) item.getExpr().getChild(0);
                        if (childFunctionCallExpr.getFnName().getFunction().equalsIgnoreCase("to_bitmap")) {
                            childFunctionCallExpr.setFnName(FunctionName.createBuiltinName("to_bitmap_with_check"));
                        }
                    }
                }
            }
        }
    }

    public static String mvColumnBuilder(String functionName, String sourceColumnName) {
        return new StringBuilder().append(MATERIALIZED_VIEW_NAME_PREFIX).append(functionName).append("_")
                .append(sourceColumnName).toString();
    }

    public static String mvColumnBuilder(AggregateType aggregateType, String sourceColumnName) {
        return new StringBuilder().append(MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX).append(aggregateType.toSql())
                .append("__")
                .append(mvColumnBreaker(sourceColumnName)).toString();
    }

    public static String mvAggregateColumnBuilder(String functionName, String sourceColumnName) {
        return new StringBuilder().append(MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX).append(functionName.toUpperCase())
                .append(MATERIALIZED_VIEW_AGGREGATE_NAME_LINK)
                .append(sourceColumnName).toString();
    }

    public static String mvColumnBuilder(String name) {
        return new StringBuilder().append(MATERIALIZED_VIEW_NAME_PREFIX).append(name).toString();
    }

    public static String mvColumnBuilder(Optional<String> functionName, String sourceColumnName) {
        return functionName.map(s -> mvAggregateColumnBuilder(s, sourceColumnName))
                    .orElseGet(() -> mvColumnBuilder(sourceColumnName));
    }

    public static String mvColumnBreaker(String name) {
        if (name.startsWith(MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX)) {
            // mva_SUM__k2 -> k2
            return mvColumnBreaker(name.substring(name.indexOf(MATERIALIZED_VIEW_AGGREGATE_NAME_LINK)
                    + MATERIALIZED_VIEW_AGGREGATE_NAME_LINK.length()));
        } else if (name.startsWith(MATERIALIZED_VIEW_NAME_PREFIX)) {
            // mv_k2 -> k2
            return mvColumnBreaker(name.substring(MATERIALIZED_VIEW_NAME_PREFIX.length()));
        }
        return name;
    }

    public static String oldmvColumnBreaker(String name) {
        if (name.startsWith(MATERIALIZED_VIEW_NAME_PREFIX)) {
            // mv_count_k2 -> k2
            name = name.substring(MATERIALIZED_VIEW_NAME_PREFIX.length());
            for (String prefix : FN_NAME_TO_PATTERN.keySet()) {
                if (name.startsWith(prefix)) {
                    return name.substring(prefix.length() + 1);
                }
            }
        }
        if (name.startsWith(MATERIALIZED_VIEW_NAME_PREFIX)) {
            // mv_k2 -> k2
            return mvColumnBreaker(name.substring(MATERIALIZED_VIEW_NAME_PREFIX.length()));
        }
        return name;
    }

    private static boolean mvMatch(String name, String prefix) {
        return MaterializedIndexMeta.normalizeName(name).startsWith(prefix);
    }

    public static boolean isMVColumn(String name) {
        return isMVColumnAggregate(name) || isMVColumnNormal(name);
    }

    public static boolean isMVColumnAggregate(String name) {
        return mvMatch(name, MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX);
    }

    public static boolean isMVColumnNormal(String name) {
        return mvMatch(name, MATERIALIZED_VIEW_NAME_PREFIX);
    }

    @Override
    public String toSql() {
        return null;
    }
}
