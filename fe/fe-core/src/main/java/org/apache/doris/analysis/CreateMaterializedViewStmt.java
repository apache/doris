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
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class CreateMaterializedViewStmt extends DdlStmt implements NotFallbackInParser {
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

    public void setIsReplay(boolean isReplay) {
        this.isReplay = isReplay;
    }

    public boolean isReplay() {
        return isReplay;
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

    public Column getWhereClauseItemExpr(OlapTable olapTable) throws DdlException {
        if (whereClauseItem == null) {
            return null;
        }
        return whereClauseItem.toMVColumn(olapTable);
    }


    @Override
    public void analyze() throws UserException {

    }

    @Override
    public void checkPriv() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, baseIndexName,
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
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
            // mva_SUM__`k2` -> `k2`;
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

    @Override
    public String toSql() {
        return null;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
