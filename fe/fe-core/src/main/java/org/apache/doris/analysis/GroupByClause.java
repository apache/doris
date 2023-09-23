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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Wraps all information of group by clause. support normal GROUP BY clause and extended GROUP BY clause like
 * ROLLUP, GROUPING SETS, CUBE syntax like
 *   SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
 *   SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY ROLLUP(a,b,c)
 *   SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY CUBE(a,b,c)
 *   GROUP BY `GROUPING SETS` ｜ `CUBE` ｜ `ROLLUP` is an extension to  GROUP BY clause.
 *   This syntax lets you define multiple groupings in the same query.
 *   GROUPING SETS produce a single result set that is equivalent to a UNION ALL of differently grouped rows.
 * In this class we produce the rule of generating rows base on the group by clause.
 */
public class GroupByClause implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(GroupByClause.class);

    // max num of distinct sets in grouping sets clause
    private static final int MAX_GROUPING_SETS_NUM = 64;
    // max num of distinct expressions
    private boolean analyzed = false;
    private boolean exprGenerated = false;
    private GroupingType groupingType;
    private ArrayList<Expr> groupingExprs;
    private ArrayList<Expr> oriGroupingExprs;
    // reserve this info for toSQL
    private List<ArrayList<Expr>> groupingSetList;

    public GroupByClause(List<ArrayList<Expr>> groupingSetList, GroupingType type) {
        this.groupingType = type;
        this.groupingSetList = groupingSetList;
        Preconditions.checkState(type == GroupingType.GROUPING_SETS);
    }

    public GroupByClause(ArrayList<Expr> groupingExprs, GroupingType type) {
        this.groupingType = type;
        this.oriGroupingExprs = groupingExprs;
        this.groupingExprs = new ArrayList<>();
        this.groupingExprs.addAll(oriGroupingExprs);
        Preconditions.checkState(type != GroupingType.GROUPING_SETS);
    }

    protected GroupByClause(GroupByClause other) {
        this.groupingType = other.groupingType;
        this.groupingExprs = (other.groupingExprs != null) ? Expr.cloneAndResetList(other.groupingExprs) : null;
        this.oriGroupingExprs =
                (other.oriGroupingExprs != null) ? Expr.cloneAndResetList(other.oriGroupingExprs) : null;

        if (other.groupingSetList != null) {
            this.groupingSetList = new ArrayList<>();
            for (List<Expr> exprList : other.groupingSetList) {
                this.groupingSetList.add(Expr.cloneAndResetList(exprList));
            }
        }
    }

    public List<ArrayList<Expr>> getGroupingSetList() {
        return groupingSetList;
    }

    public GroupingType getGroupingType() {
        return groupingType;
    }

    public void reset() {
        groupingExprs = new ArrayList<>();
        analyzed = false;
        exprGenerated = false;
        if (oriGroupingExprs != null) {
            Expr.resetList(oriGroupingExprs);
            groupingExprs.addAll(oriGroupingExprs);
        }
        if (groupingSetList != null) {
            for (List<Expr> s : groupingSetList) {
                for (Expr e : s) {
                    if (e != null) {
                        e.reset();
                    }
                }
            }
        }
    }

    public List<Expr> getOriGroupingExprs() {
        return oriGroupingExprs;
    }

    public void setOriGroupingExprs(ArrayList<Expr> list) {
        oriGroupingExprs = list;
    }

    public ArrayList<Expr> getGroupingExprs() {
        if (!exprGenerated) {
            try {
                genGroupingExprs();
            } catch (AnalysisException e) {
                if (ConnectContext.get() != null) {
                    ConnectContext.get().getState().reset();
                }
                LOG.error("gen grouping expr error:", e);
                return null;
            }
        }
        return groupingExprs;
    }

    public void setGroupingExpr(ArrayList<Expr> list) {
        groupingExprs = list;
    }

    // generate grouping exprs from group by, grouping sets, cube, rollup clause
    public void genGroupingExprs() throws AnalysisException {
        if (exprGenerated) {
            return;
        }
        if (CollectionUtils.isNotEmpty(groupingExprs)) {
            // remove repeated element
            Set<Expr> groupingExprSet = new LinkedHashSet<>(groupingExprs);
            groupingExprs.clear();
            groupingExprs.addAll(groupingExprSet);
        }
        if (groupingType == GroupingType.CUBE || groupingType == GroupingType.ROLLUP) {
            if (CollectionUtils.isEmpty(groupingExprs)) {
                throw new AnalysisException(
                        "The expressions in GROUPING CUBE or ROLLUP can not be empty");
            }
        } else if (groupingType == GroupingType.GROUPING_SETS) {
            if (CollectionUtils.isEmpty(groupingSetList)) {
                throw new AnalysisException("The expressions in GROUPING SETS can not be empty");
            }
            // collect all Expr elements
            Set<Expr> groupingExprSet = new LinkedHashSet<>();
            for (ArrayList<Expr> list : groupingSetList) {
                groupingExprSet.addAll(list);
            }
            groupingExprs = new ArrayList<>(groupingExprSet);
        }
        exprGenerated = true;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (analyzed) {
            return;
        }
        genGroupingExprs();

        // disallow subqueries in the GROUP BY clause
        for (Expr expr : groupingExprs) {
            if (expr.contains(Predicates.instanceOf(Subquery.class))) {
                throw new AnalysisException(
                        "Subqueries are not supported in the GROUP BY clause.");
            }
        }
        //TODO add the analysis for grouping and grouping_id functions
        for (Expr groupingExpr : groupingExprs) {
            groupingExpr.analyze(analyzer);
            if (groupingExpr.contains(Expr.isAggregatePredicate())) {
                // reference the original expr in the error msg
                throw new AnalysisException(
                        "GROUP BY expression must not contain aggregate functions: "
                                + groupingExpr.toSql());
            }
            if (groupingExpr.contains(AnalyticExpr.class)) {
                // reference the original expr in the error msg
                throw new AnalysisException(
                        "GROUP BY expression must not contain analytic expressions: "
                                + groupingExpr.toSql());
            }

            if (groupingExpr.type.isOnlyMetricType()) {
                throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
            }
        }

        if (isGroupByExtension() && groupingExprs != null && groupingExprs.size() > MAX_GROUPING_SETS_NUM) {
            throw new AnalysisException("Too many sets in GROUP BY clause, the max grouping sets item is "
                    + MAX_GROUPING_SETS_NUM);
        }
        analyzed = true;
    }

    // check if group by clause is contain grouping set/rollup/cube
    public boolean isGroupByExtension() {
        return groupingType != GroupingType.GROUP_BY;
    }

    @Override
    public String toSql() {
        StringBuilder strBuilder = new StringBuilder();
        switch (groupingType) {
            case GROUP_BY:
                if (oriGroupingExprs != null) {
                    for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                        strBuilder.append(oriGroupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                    }
                }
                break;
            case GROUPING_SETS:
                if (groupingSetList != null) {
                    strBuilder.append("GROUPING SETS (");
                    boolean first = true;
                    for (List<Expr> groupingExprs : groupingSetList) {
                        if (first) {
                            strBuilder.append("(");
                            first = false;
                        } else {
                            strBuilder.append(", (");
                        }
                        for (int i = 0; i < groupingExprs.size(); ++i) {
                            strBuilder.append(groupingExprs.get(i).toSql());
                            strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                        }
                        strBuilder.append(")");
                    }
                    strBuilder.append(")");
                }
                break;
            case CUBE:
                if (oriGroupingExprs != null) {
                    strBuilder.append("CUBE (");
                    for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                        strBuilder.append(oriGroupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                    }
                    strBuilder.append(")");
                }
                break;
            case ROLLUP:
                if (oriGroupingExprs != null) {
                    strBuilder.append("ROLLUP (");
                    for (int i = 0; i < oriGroupingExprs.size(); ++i) {
                        strBuilder.append(oriGroupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != oriGroupingExprs.size()) ? ", " : "");
                    }
                    strBuilder.append(")");
                }
                break;
            default:
                break;
        }
        return strBuilder.toString();
    }

    @Override
    public GroupByClause clone() {
        return new GroupByClause(this);
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(groupingExprs);
    }

    public void substituteGroupingExprs(Set<VirtualSlotRef> groupingSlots, ExprSubstitutionMap smap,
                                        Analyzer analyzer) {
        groupingExprs = Expr.substituteList(groupingExprs, smap, analyzer, true);
        for (VirtualSlotRef vs : groupingSlots) {
            ArrayList<Expr> exprs = Expr.substituteList(vs.getRealSlots(), smap, analyzer, true);
            if (exprs != null) {
                vs.setRealSlots(exprs);
            } else {
                vs.setRealSlots(new ArrayList<Expr>());
            }
        }
    }

    public enum GroupingType {
        GROUP_BY,
        GROUPING_SETS,
        ROLLUP,
        CUBE
    }
}
