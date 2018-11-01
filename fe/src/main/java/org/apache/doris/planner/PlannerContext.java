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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.thrift.TQueryOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Contains the analysis result of a query as well as planning-specific
 * parameters and state such as plan-node and plan-fragment id generators.
 */
public class PlannerContext {
    private final static Logger LOG = LogManager.getLogger(PlannerContext.class);

    // Estimate of the overhead imposed by storing data in a hash tbl;
    // used for determining whether a broadcast join is feasible.
    public final static double HASH_TBL_SPACE_OVERHEAD = 1.1;

    private final IdGenerator<PlanNodeId> nodeIdGenerator_ = PlanNodeId.createGenerator();
    private final IdGenerator<PlanFragmentId> fragmentIdGenerator_ =
            PlanFragmentId.createGenerator();

    // TODO(zc) private final TQueryCtx queryCtx_;
    // TODO(zc) private final AnalysisContext.AnalysisResult analysisResult_;
    private final Analyzer analyzer_;
    private final TQueryOptions queryOptions_;
    private final QueryStmt queryStmt_;
    private final StatementBase statement_;

    public PlannerContext(Analyzer analyzer, QueryStmt queryStmt, TQueryOptions queryOptions, StatementBase statement) {
        this.analyzer_ = analyzer;
        this.queryStmt_ = queryStmt;
        this.queryOptions_ = queryOptions;
        this.statement_ = statement;
    }

    public QueryStmt getQueryStmt() { return queryStmt_; }
    public TQueryOptions getQueryOptions() { return queryOptions_; } // getRootAnalyzer().getQueryOptions(); }
    public Analyzer getRootAnalyzer() { return analyzer_; } // analysisResult_.getAnalyzer(); }
    public boolean isSingleNodeExec() { return getQueryOptions().num_nodes == 1; }
    public PlanNodeId getNextNodeId() { return nodeIdGenerator_.getNextId(); }
    public PlanFragmentId getNextFragmentId() { return fragmentIdGenerator_.getNextId(); }

    public boolean isInsert() { return statement_ instanceof InsertStmt; }
}
