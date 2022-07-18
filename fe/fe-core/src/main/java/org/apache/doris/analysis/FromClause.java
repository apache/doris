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

    public FromClause(List<TableRef> tableRefs) {
        tablerefs = Lists.newArrayList(tableRefs);
        // Set left table refs to ensure correct toSql() before analysis.
        for (int i = 1; i < tablerefs.size(); ++i) {
            tablerefs.get(i).setLeftTblRef(tablerefs.get(i - 1));
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
            if (tblRef instanceof InlineViewRef) {
                ((InlineViewRef) tblRef).setNeedToSql(needToSql);
            }
            tblRef.analyze(analyzer);
            leftTblRef = tblRef;
        }

        analyzed = true;
    }

    public FromClause clone() {
        ArrayList<TableRef> clone = Lists.newArrayList();
        for (TableRef tblRef : tablerefs) {
            clone.add(tblRef.clone());
        }
        return new FromClause(clone);
    }

    public void reset() {
        for (int i = 0; i < size(); ++i) {
            get(i).reset();
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
    }

    public void add(TableRef t) {
        tablerefs.add(t);
    }

    public void addAll(List<TableRef> t) {
        tablerefs.addAll(t);
    }

    public void clear() {
        tablerefs.clear();
    }
}
