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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Materialized view is performed to materialize the results of query.
 * This clause is used to create a new materialized view for a specified table
 * through a specified query stmt.
 * <p>
 * Syntax:
 * CREATE MATERIALIZED VIEW [MV name] (
 *     SELECT select_expr[, select_expr ...]
 *     FROM [Base view name]
 *     GROUP BY column_name[, column_name ...]
 *     ORDER BY column_name[, column_name ...])
 * [PROPERTIES ("key" = "value")]
 */
public class CreateMaterializedViewStmt extends DdlStmt {
    public static final String MATERIALIZED_VIEW_NAME_PRFIX = "__doris_materialized_view_";

    private String mvName;
    private SelectStmt selectStmt;
    private Map<String, String> properties;

    private int beginIndexOfAggregation = -1;
    /**
     * origin stmt: select k1, k2, v1, sum(v2) from base_table group by k1, k2, v1
     * mvColumnItemList: [k1: {name: k1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     *                    k2: {name: k2, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     *                    v1: {name: v1, isKey: true, aggType: null, isAggregationTypeImplicit: false},
     *                    v2: {name: v2, isKey: false, aggType: sum, isAggregationTypeImplicit: false}]
     * This order of mvColumnItemList is meaningful.
     */
    private List<MVColumnItem> mvColumnItemList = Lists.newArrayList();
    private String baseIndexName;
    private String dbName;
    private KeysType mvKeysType = KeysType.DUP_KEYS;

    public CreateMaterializedViewStmt(String mvName, SelectStmt selectStmt, Map<String, String> properties) {
        this.mvName = mvName;
        this.selectStmt = selectStmt;
        this.properties = properties;
    }

    public String getMVName() {
        return mvName;
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

    public KeysType getMVKeysType() {
        return mvKeysType;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!Config.enable_materialized_view) {
            throw new AnalysisException("The materialized view is disabled");
        }
        super.analyze(analyzer);
        FeNameFormat.checkTableName(mvName);
        // TODO(ml): The mv name in from clause should pass the analyze without error.
        selectStmt.analyze(analyzer);
        if (selectStmt.getAggInfo() != null) {
            mvKeysType = KeysType.AGG_KEYS;
        }
        analyzeSelectClause();
        analyzeFromClause();
        if (selectStmt.getWhereClause() != null) {
            throw new AnalysisException("The where clause is not supported in add materialized view clause, expr:"
                    + selectStmt.getWhereClause().toSql());
        }
        if (selectStmt.getHavingPred() != null) {
            throw new AnalysisException("The having clause is not supported in add materialized view clause, expr:"
                    + selectStmt.getHavingPred().toSql());
        }
        analyzeOrderByClause();
        if (selectStmt.getLimit() != -1) {
            throw new AnalysisException("The limit clause is not supported in add materialized view clause, expr:"
                    + " limit " + selectStmt.getLimit());
        }
    }

    public void analyzeSelectClause() throws AnalysisException {
        SelectList selectList = selectStmt.getSelectList();
        if (selectList.getItems().isEmpty()) {
            throw new AnalysisException("The materialized view must contain at least one column");
        }
        boolean meetAggregate = false;
        // TODO(ml): support same column with different aggregation function
        Set<String> mvColumnNameSet = Sets.newHashSet();
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
            Expr selectListItemExpr = selectListItem.getExpr();
            if (!(selectListItemExpr instanceof SlotRef) && !(selectListItemExpr instanceof FunctionCallExpr)) {
                throw new AnalysisException("The materialized view only support the single column or function expr. "
                        + "Error column: " + selectListItemExpr.toSql());
            }
            if (selectListItem.getExpr() instanceof SlotRef) {
                if (meetAggregate) {
                    throw new AnalysisException("The aggregate column should be after the single column");
                }
                SlotRef slotRef = (SlotRef) selectListItem.getExpr();
                // check duplicate column
                String columnName = slotRef.getColumnName().toLowerCase();
                if (!mvColumnNameSet.add(columnName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, columnName);
                }
                MVColumnItem mvColumnItem = new MVColumnItem(columnName);
                mvColumnItem.setType(slotRef.getType());
                mvColumnItemList.add(mvColumnItem);
            } else if (selectListItem.getExpr() instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) selectListItem.getExpr();
                String functionName = functionCallExpr.getFnName().getFunction();
                Expr defineExpr = null;
                // TODO(ml): support REPLACE, REPLACE_IF_NOT_NULL only for aggregate table, HLL_UNION, BITMAP_UNION
                if (!functionName.equalsIgnoreCase("sum")
                        && !functionName.equalsIgnoreCase("min")
                        && !functionName.equalsIgnoreCase("max")) {
                    throw new AnalysisException("The materialized view only support the sum, min and max aggregate "
                            + "function. Error function: " + functionCallExpr.toSqlImpl());
                }

                Preconditions.checkState(functionCallExpr.getChildren().size() == 1);
                Expr functionChild0 = functionCallExpr.getChild(0);
                SlotRef slotRef;
                if (functionChild0 instanceof SlotRef) {
                    slotRef = (SlotRef) functionChild0;
                } else if (functionChild0 instanceof CastExpr && (functionChild0.getChild(0) instanceof SlotRef)) {
                    slotRef = (SlotRef) functionChild0.getChild(0);
                } else {
                    throw new AnalysisException("The children of aggregate function only support one original column. "
                            + "Error function: " + functionCallExpr.toSqlImpl());
                }
                meetAggregate = true;
                // check duplicate column
                String columnName = slotRef.getColumnName().toLowerCase();
                if (!mvColumnNameSet.add(columnName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, columnName);
                }

                if (beginIndexOfAggregation == -1) {
                    beginIndexOfAggregation = i;
                }
                // TODO(ml): support different type of column, int -> bigint(sum)
                // TODO: change the column name of bitmap and hll
                MVColumnItem mvColumnItem = new MVColumnItem(columnName);
                mvColumnItem.setAggregationType(AggregateType.valueOf(functionName.toUpperCase()), false);
                mvColumnItem.setDefineExpr(defineExpr);
                mvColumnItemList.add(mvColumnItem);
            }
        }
        // TODO(ML): only value columns of materialized view, such as select sum(v1) from table
        if (beginIndexOfAggregation == 0) {
            throw new AnalysisException("The materialized view must contain at least one key column");
        }
    }

    private void analyzeFromClause() throws AnalysisException {
        List<TableRef> tableRefList = selectStmt.getTableRefs();
        if (tableRefList.size() != 1) {
            throw new AnalysisException("The materialized view only support one table in from clause.");
        }
        TableName tableName = tableRefList.get(0).getName();
        baseIndexName = tableName.getTbl();
        dbName = tableName.getDb();
    }

    private void analyzeOrderByClause() throws AnalysisException {
        if (selectStmt.getOrderByElements() == null) {
            supplyOrderColumn();
            return;
        }

        List<OrderByElement> orderByElements = selectStmt.getOrderByElements();
        if (orderByElements.size() > mvColumnItemList.size()) {
            throw new AnalysisException("The number of columns in order clause must be less then " + "the number of "
                    + "columns in select clause");
        }
        if (beginIndexOfAggregation != -1 && (orderByElements.size() != (beginIndexOfAggregation))) {
            throw new AnalysisException("The key of columns in mv must be all of group by columns");
        }
        for (int i = 0; i < orderByElements.size(); i++) {
            Expr orderByElement = orderByElements.get(i).getExpr();
            if (!(orderByElement instanceof SlotRef)) {
                throw new AnalysisException("The column in order clause must be original column without calculation. "
                        + "Error column: " + orderByElement.toSql());
            }
            MVColumnItem mvColumnItem = mvColumnItemList.get(i);
            SlotRef slotRef = (SlotRef) orderByElement;
            if (!mvColumnItem.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                throw new AnalysisException("The order of columns in order by clause must be same as "
                        + "the order of columns in select list");
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

    @Override
    public String toSql() {
        return null;
    }
}
