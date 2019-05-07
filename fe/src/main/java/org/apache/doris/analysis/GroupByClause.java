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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Wraps all information of group by clause.
 */
public class GroupByClause implements ParseNode {
    private final static Logger LOG = LogManager.getLogger(GroupByClause.class);

    // max num of distinct sets in grouping sets clause
    private final static int MAX_GROUPING_SETS_NUM = 16;
    // max num of distinct expressions
    private final static int MAX_GROUPING_SETS_EXPRESSION_NUM = 64;
    // GROUPING_ID is a fake column
    private final static String GROUPING__ID = "GROUPING__ID";

    public enum GroupingType {
        GROUP_BY,
        GROUPING_SETS,
        ROLLUP,
        CUBE;
    };

    private boolean analyzed_ = false;
    private GroupingType groupingType;
    private ArrayList<Expr> groupingExprs;
    private List<BitSet> groupingIdList;
    private SlotRef groupingIdSlotRef;

    // reserve this info for toSQL
    private List<ArrayList<Expr>> groupingSetList;

    public GroupByClause(List<ArrayList<Expr>> groupingSetList, GroupingType type) {
        this.groupingType = type;
        this.groupingSetList = groupingSetList;
        Preconditions.checkState(type == GroupingType.GROUPING_SETS);
    }

    public GroupByClause(ArrayList<Expr> groupingExprs, GroupingType type) {
        this.groupingType = type;
        this.groupingExprs = groupingExprs;
        Preconditions.checkState(type != GroupingType.GROUPING_SETS);
    }

    public ArrayList<Expr> getGroupingExprs() {
        Preconditions.checkState(analyzed_);
        return groupingExprs;
    }

    public void setGroupingExprs(ArrayList<Expr> groupingExprs) {
        this.groupingExprs = groupingExprs;
    }

    protected GroupByClause(GroupByClause other) {
        this.groupingType = other.groupingType;
        this.groupingExprs = Expr.cloneAndResetList(groupingExprs);
        this.groupingIdList = other.groupingIdList;
        this.groupingSetList = other.groupingSetList;
    }


    public SlotRef getGroupingIdSlotRef() {
        Preconditions.checkState(groupingIdSlotRef != null);
        return groupingIdSlotRef;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (analyzed_) return;

        if (groupingType == GroupingType.CUBE || groupingType == GroupingType.ROLLUP) {
            if (groupingExprs == null || groupingExprs.isEmpty()) {
                throw new AnalysisException(
                        "The expresions in GROUPING CUBE or ROLLUP can not be empty");
            }

            buildGroupingClause(analyzer);

        } else if (groupingType == GroupingType.GROUPING_SETS) {
            if (groupingSetList == null || groupingSetList.isEmpty()) {
                throw new AnalysisException(
                        "The expresions in GROUPINGING SETS can not be empty");
            }
            // collect all Expr elements
            Set<Expr> groupingExprSet = new HashSet<>();
            for(ArrayList<Expr> list: groupingSetList) {
                groupingExprSet.addAll(list);
            }
            groupingExprs = new ArrayList<>(groupingExprSet);

            // regard as ordinary group by clause if less than one expression
            if (groupingSetList.size() > 1) {
                buildGroupingClause(analyzer);
            }
        }

        // disallow subqueries in the GROUP BY clause
        for (Expr expr: groupingExprs) {
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

            if (groupingExpr.type.isHllType()) {
                throw new AnalysisException(
                        "GROUP BY expression must not contain hll column: "
                                + groupingExpr.toSql());
            }
        }

        if (groupingIdList != null && groupingIdList.size() > MAX_GROUPING_SETS_NUM) {
            throw new AnalysisException(
                    "Too many sets in GROUP BY clause, it must be not more than "
                            + MAX_GROUPING_SETS_NUM);
        }

        if (isGroupByExtension() && groupingExprs != null) {
            if (groupingExprs.size() >= MAX_GROUPING_SETS_EXPRESSION_NUM) {
                throw new AnalysisException(
                        "Too many expressions in GROUP BY clause, it must be not more than "
                                + MAX_GROUPING_SETS_EXPRESSION_NUM);
            }
        }

        analyzed_ = true;
    }

    public boolean isGroupByExtension() {
        if (groupingType != GroupingType.GROUPING_SETS
                && groupingType != GroupingType.CUBE
                && groupingType != GroupingType.ROLLUP) {
            return false;
        }

        if (groupingType == GroupingType.GROUPING_SETS &&
                groupingSetList != null && groupingSetList.size() <= 1) {
            return false;
        }

        return true;
    }

    @Override
    public String toSql() {
        StringBuilder strBuilder = new StringBuilder();
        switch (groupingType) {
            case GROUP_BY:
                if (groupingExprs != null) {
                    for (int i = 0; i < groupingExprs.size(); ++i) {
                        strBuilder.append(groupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                    }
                }
                break;
            case GROUPING_SETS:
                if (groupingSetList != null) {
                    strBuilder.append(" GROUPING SETS (");
                    boolean first = true;
                    for(List<Expr> groupingExprs : groupingSetList) {
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
                if (groupingExprs != null) {
                    strBuilder.append("CUBE (");
                    for (int i = 0; i < groupingExprs.size(); ++i) {
                        strBuilder.append(groupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                    }
                    strBuilder.append(")");
                }
                break;
            case ROLLUP:
                if (groupingExprs != null) {
                    strBuilder.append("ROLLUP (");
                    for (int i = 0; i < groupingExprs.size(); ++i) {
                        strBuilder.append(groupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
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

    public void reset() {
        if (groupingExprs != null) Expr.resetList(groupingExprs);
        this.analyzed_ = false;
    }

    public boolean isEmpty() {
        return groupingExprs == null || groupingExprs.isEmpty();
    }

    private void addGroupingId(Analyzer analyzer) throws  AnalysisException {
        TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor(GROUPING__ID);
        groupingIdSlotRef = new VirtualSlotRef(GROUPING__ID, Type.BIGINT, tupleDesc);
        groupingIdSlotRef.analyze(analyzer);
        groupingExprs.add(groupingIdSlotRef);
    }

    private void buildGroupingClause(Analyzer analyzer) throws AnalysisException {
        groupingIdList = new ArrayList<>();
        BitSet bitSetAll = new BitSet();
        bitSetAll.set(0, groupingExprs.size(), true);
        switch (groupingType) {
            case CUBE:
                int size = (1 << groupingExprs.size()) - 1;
                groupingIdList.add(bitSetAll);
                for(int i = 0; i < size; i++) {
                    String s = Integer.toBinaryString(i);
                    BitSet bitSet = new BitSet();
                    for(int j = 0; j < s.length(); j++) {
                        bitSet.set(s.length() - j - 1, s.charAt(j) == '1');
                    }
                    groupingIdList.add(bitSet);
                }
                break;

            case ROLLUP:
                groupingIdList.add(bitSetAll);
                for(int i = 0; i < groupingExprs.size(); i++) {
                    BitSet bitSet = new BitSet();
                    bitSet.set(0, i);
                    groupingIdList.add(bitSet);
                }
                break;

            case GROUPING_SETS:
                groupingIdList.add(bitSetAll);

                BitSet bitSetBase = new BitSet();
                bitSetBase.set(0, groupingExprs.size());
                for(ArrayList<Expr> list: groupingSetList) {
                    BitSet bitSet = new BitSet();
                    for(int i = 0; i < groupingExprs.size(); i++) {
                        bitSet.set(i, list.contains(groupingExprs.get(i)));
                    }
                    if (!bitSet.equals(bitSetBase)) {
                        if (!groupingIdList.contains(bitSet)) {
                            groupingIdList.add(bitSet);
                        }
                    }
                }
                break;

            default:
                Preconditions.checkState(false);
                return;
        }
        addGroupingId(analyzer);
    }

    public List<BitSet> getGroupingIdList() {
        return groupingIdList;
    }

}

