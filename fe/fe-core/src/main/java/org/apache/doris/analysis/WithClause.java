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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of the WITH clause that may appear before a query statement or insert
 * statement. A WITH clause contains a list of named view definitions that may be
 * referenced in the query statement that follows it.
 *
 * Scoping rules:
 * A WITH-clause view is visible inside the query statement that it belongs to.
 * This includes inline views and nested WITH clauses inside the query statement.
 *
 * Each WITH clause establishes a new analysis scope. A WITH-clause view definition
 * may refer to views from the same WITH-clause appearing to its left, and to all
 * WITH-clause views from outer scopes.
 *
 * References to WITH-clause views are resolved inside out, i.e., a match is found by
 * first looking in the current scope and then in the enclosing scope(s).
 *
 * Views defined within the same WITH-clause may not use the same alias.
 */
public class WithClause implements ParseNode {
    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    private final ArrayList<View> views_;

    // END: Members that need to be reset()
    /////////////////////////////////////////

    public WithClause(ArrayList<View> views) {
        Preconditions.checkNotNull(views);
        Preconditions.checkState(!views.isEmpty());
        views_ = views;
    }

    /**
     * Analyzes all views and registers them with the analyzer. Enforces scoping rules.
     * All local views registered with the analyzer are have QueryStmts with resolved
     * TableRefs to simplify the analysis of view references.
     */
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        // Create a new analyzer for the WITH clause with a new global state (IMPALA-1357)
        // but a child of 'analyzer' so that the global state for 'analyzer' is not polluted
        // during analysis of the WITH clause. withClauseAnalyzer is a child of 'analyzer' so
        // that local views registered in parent blocks are visible here.
        Analyzer withClauseAnalyzer = Analyzer.createWithNewGlobalState(analyzer);
        withClauseAnalyzer.setIsWithClause();
        if (analyzer.isExplain()) withClauseAnalyzer.setIsExplain();
        for (View view: views_) {
            Analyzer viewAnalyzer = new Analyzer(withClauseAnalyzer);
            view.getQueryStmt().analyze(viewAnalyzer);
            // Register this view so that the next view can reference it.
            withClauseAnalyzer.registerLocalView(view);
        }
        // Register all local views with the analyzer.
        for (View localView: withClauseAnalyzer.getLocalViews().values()) {
            analyzer.registerLocalView(localView);
        }
    }

    /**
     * C'tor for cloning.
     */
    private WithClause(WithClause other) {
        Preconditions.checkNotNull(other);
        views_ = Lists.newArrayList();
        for (View view: other.views_) {
            views_.add(new View(view.getName(), view.getQueryStmt().clone(),
                    view.getOriginalColLabels()));
        }
    }

    public void reset() {
        for (View view: views_) view.getQueryStmt().reset();
    }

    public void getTables(Analyzer analyzer, Map<Long, Table> tableMap, Set<String> parentViewNameSet) throws AnalysisException {
        for (View view : views_) {
            QueryStmt stmt = view.getQueryStmt();
            stmt.getTables(analyzer, tableMap, parentViewNameSet);
        }
    }

    @Override
    public WithClause clone() { return new WithClause(this); }

    @Override
    public String toSql() {
        List<String> viewStrings = Lists.newArrayList();
        for (View view: views_) {
            // Enclose the view alias and explicit labels in quotes if Hive cannot parse it
            // without quotes. This is needed for view compatibility between Impala and Hive.
            String aliasSql = ToSqlUtils.getIdentSql(view.getName());
            if (view.hasColLabels()) {
                aliasSql += "(" + Joiner.on(", ").join(
                        ToSqlUtils.getIdentSqlList(view.getOriginalColLabels())) + ")";
            }
            viewStrings.add(aliasSql + " AS (" + view.getQueryStmt().toSql() + ")");
        }
        return "WITH " + Joiner.on(",").join(viewStrings);
    }

    public List<View> getViews() { return views_; }
}
