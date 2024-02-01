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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlannerContext.java
// and modified by Doris

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
    private static final Logger LOG = LogManager.getLogger(PlannerContext.class);

    // Estimate of the overhead imposed by storing data in a hash tbl;
    // used for determining whether a broadcast join is feasible.
    public static final double HASH_TBL_SPACE_OVERHEAD = 1.1;

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();
    private final IdGenerator<PlanFragmentId> fragmentIdGenerator =
            PlanFragmentId.createGenerator();

    // TODO(zc) private final TQueryCtx queryCtx_;
    // TODO(zc) private final AnalysisContext.AnalysisResult analysisResult_;
    private final Analyzer analyzer;
    private final TQueryOptions queryOptions;
    private final QueryStmt queryStmt;
    private final StatementBase statement;

    public PlannerContext(Analyzer analyzer, QueryStmt queryStmt, TQueryOptions queryOptions, StatementBase statement) {
        this.analyzer = analyzer;
        this.queryStmt = queryStmt;
        this.queryOptions = queryOptions;
        this.statement = statement;
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }

    public StatementBase getStatement() {
        return statement;
    }

    public TQueryOptions getQueryOptions() {
        return queryOptions;
    } // getRootAnalyzer().getQueryOptions(); }

    public Analyzer getRootAnalyzer() {
        return analyzer;
    } // analysisResult_.getAnalyzer(); }

    public boolean isSingleNodeExec() {
        return getQueryOptions().num_nodes == 1;
    }

    public PlanNodeId getNextNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public PlanFragmentId getNextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public boolean isInsert() {
        return statement instanceof InsertStmt;
    }
}
