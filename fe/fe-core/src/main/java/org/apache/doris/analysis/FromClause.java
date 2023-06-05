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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/FromClause.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Wraps a list of TableRef instances that form a FROM clause, allowing them to be
 * analyzed independently of the statement using them. To increase the flexibility of
 * the class it implements the Iterable interface.
 */
public class FromClause implements ParseNode, Iterable<TableRef> {

    private final ArrayList<TableRef> tablerefs;

    private boolean analyzed = false;
    private boolean needToSql = false;

    // the tables positions may be changed by 'join reorder' optimization
    // after reset, the original order information is lost
    // in the next re-analyze phase, the mis-ordered tables may lead to 'unable to find column xxx' error
    // now we use originalTableRefOrders to keep track of table order information
    // so that in reset method, we can recover the original table orders.
    private final ArrayList<TableRef> originalTableRefOrders = new ArrayList<TableRef>();

    public FromClause(List<TableRef> tableRefs) {
        tablerefs = Lists.newArrayList(tableRefs);
        // Set left table refs to ensure correct toSql() before analysis.
        for (int i = 1; i < tablerefs.size(); ++i) {
            tablerefs.get(i).setLeftTblRef(tablerefs.get(i - 1));
        }
        // save the tableRef's order, will use in reset method later
        originalTableRefOrders.clear();
        for (int i = 0; i < tablerefs.size(); ++i) {
            originalTableRefOrders.add(tablerefs.get(i));
        }
    }

    public FromClause() {
        tablerefs = Lists.newArrayList();
    }

    public List<TableRef> getTableRefs() {
        return tablerefs;
    }

    public void setNeedToSql(boolean needToSql) {
        this.needToSql = needToSql;
    }

    /**
     * In some cases, the reorder method of select stmt will incorrectly sort the tableRef with on clause.
     * The meaning of this function is to reset those tableRefs with on clauses.
     * For example:
     * Origin stmt: select * from t1 inner join t2 on t1.k1=t2.k1
     * After analyze: select * from t2 on t1.k1=t2.k1 inner join t1
     *
     * If this statement just needs to be reanalyze (query rewriter), an error will be reported
     * because the table t1 in the on clause cannot be recognized.
     */
    private void sortTableRefKeepSequenceOfOnClause() {
        Collections.sort(this.tablerefs, new Comparator<TableRef>() {
            @Override
            public int compare(TableRef tableref1, TableRef tableref2) {
                int i1 = 0;
                int i2 = 0;
                if (tableref1.getOnClause() != null || tableref1.getUsingClause() != null) {
                    i1 = 1;
                }
                if (tableref2.getOnClause() != null || tableref2.getUsingClause() != null) {
                    i2 = 1;
                }
                return i1 - i2;
            }
        });
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (analyzed) {
            return;
        }

        if (tablerefs.isEmpty()) {
            analyzed = true;
            return;
        }

        // The order of the tables may have changed during the previous analyzer process.
        // For example, a join b on xxx is changed to b on xxx join a.
        // This change will cause the predicate in on clause be adjusted to the front of the association table,
        // causing semantic analysis to fail. Unknown column 'column1' in 'table1'
        // So we need to readjust the order of the tables here.
        if (analyzer.enableStarJoinReorder()) {
            sortTableRefKeepSequenceOfOnClause();
        }

        // Start out with table refs to establish aliases.
        TableRef leftTblRef = null;  // the one to the left of tblRef
        for (int i = 0; i < tablerefs.size(); ++i) {
            // Resolve and replace non-InlineViewRef table refs with a BaseTableRef or ViewRef.
            TableRef tblRef = tablerefs.get(i);
            tblRef = analyzer.resolveTableRef(tblRef);
            tablerefs.set(i, Preconditions.checkNotNull(tblRef));
            tblRef.setLeftTblRef(leftTblRef);
            boolean setExternalCtl = false;
            String preExternalCtl = null;
            if (tblRef instanceof InlineViewRef) {
                ((InlineViewRef) tblRef).setNeedToSql(needToSql);
                String externalCtl = ((InlineViewRef) tblRef).getExternalCtl();
                if (StringUtils.isNotEmpty(externalCtl)) {
                    preExternalCtl = analyzer.getExternalCtl();
                    analyzer.setExternalCtl(externalCtl);
                    setExternalCtl = true;
                }
            }
            tblRef.analyze(analyzer);
            if (setExternalCtl) {
                analyzer.setExternalCtl(preExternalCtl);
            }
            leftTblRef = tblRef;
            Expr clause = tblRef.getOnClause();
            if (clause != null && clause.contains(Subquery.class)) {
                throw new AnalysisException("Not support OnClause contain Subquery, expr:"
                        + clause.toSql());
            }
        }
        // Fix the problem of column nullable attribute error caused by inline view + outer join
        changeTblRefToNullable(analyzer);

        analyzed = true;

        // save the tableRef's order, will use in reset method later
        originalTableRefOrders.clear();
        for (int i = 0; i < tablerefs.size(); ++i) {
            originalTableRefOrders.add(tablerefs.get(i));
        }
    }

    // set null-side inlinve view column
    // For example: select * from (select a as k1 from t) tmp right join b on tmp.k1=b.k1
    // The columns from tmp should be nullable.
    // The table ref tmp will be used by HashJoinNode.computeOutputTuple()
    private void changeTblRefToNullable(Analyzer analyzer) {
        for (TableRef tableRef : tablerefs) {
            if (!(tableRef instanceof InlineViewRef)) {
                continue;
            }
            InlineViewRef inlineViewRef = (InlineViewRef) tableRef;
            if (analyzer.isOuterJoined(inlineViewRef.getId())) {
                for (SlotDescriptor slotDescriptor : inlineViewRef.getDesc().getSlots()) {
                    slotDescriptor.setIsNullable(true);
                }
            }
        }
    }

    public FromClause clone() {
        ArrayList<TableRef> clone = Lists.newArrayList();
        for (TableRef tblRef : tablerefs) {
            clone.add(tblRef.clone());
        }
        FromClause result = new FromClause(clone);
        for (int i = 0; i < clone.size(); ++i) {
            result.originalTableRefOrders.add(clone.get(i));
        }
        return result;
    }

    public void reset() {
        for (int i = 0; i < size(); ++i) {
            get(i).reset();
        }
        // recover original table orders
        for (int i = 0; i < size(); ++i) {
            tablerefs.set(i, originalTableRefOrders.get(i));
        }
        this.analyzed = false;
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        if (!tablerefs.isEmpty()) {
            builder.append(" FROM");
            for (int i = 0; i < tablerefs.size(); ++i) {
                builder.append(" " + tablerefs.get(i).toSql());
            }
        }
        return builder.toString();
    }

    public String toDigest() {
        StringBuilder builder = new StringBuilder();
        if (!tablerefs.isEmpty()) {
            builder.append(" FROM");
            for (int i = 0; i < tablerefs.size(); ++i) {
                builder.append(" " + tablerefs.get(i).toDigest());
            }
        }
        return builder.toString();
    }

    public boolean isEmpty() {
        return tablerefs.isEmpty();
    }

    @Override
    public Iterator<TableRef> iterator() {
        return tablerefs.iterator();
    }

    public int size() {
        return tablerefs.size();
    }

    public TableRef get(int i) {
        return tablerefs.get(i);
    }

    public void set(int i, TableRef tableRef) {
        tablerefs.set(i, tableRef);
        originalTableRefOrders.set(i, tableRef);
    }

    public void add(TableRef t) {
        tablerefs.add(t);
        // join reorder will call add method after call clear method.
        // we want to keep tableRefPositions unchanged in that case
        // in other cases, tablerefs.size() would larger than tableRefPositions.size()
        // then we can update tableRefPositions. same logic in addAll method.
        if (tablerefs.size() > originalTableRefOrders.size()) {
            originalTableRefOrders.add(t);
        }
    }

    public void addAll(List<TableRef> t) {
        tablerefs.addAll(t);
        if (tablerefs.size() > originalTableRefOrders.size()) {
            for (int i = originalTableRefOrders.size(); i < tablerefs.size(); ++i) {
                originalTableRefOrders.add(tablerefs.get(i));
            }
        }
    }

    public void clear() {
        // this method on be called in reorder table
        // we want to keep tableRefPositions, only clear tablerefs
        tablerefs.clear();
    }
}
