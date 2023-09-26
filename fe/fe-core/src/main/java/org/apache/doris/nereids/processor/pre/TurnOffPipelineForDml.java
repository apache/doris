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

package org.apache.doris.nereids.processor.pre;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

/**
 * TODO turnoff pipeline for any dml temporary, remove this pre-process when pipeline-sink is ok.
 */
public class TurnOffPipelineForDml extends PlanPreprocessor {

    @Override
    public Plan visitUnboundOlapTableSink(UnboundOlapTableSink<? extends Plan> unboundOlapTableSink,
            StatementContext context) {
        turnOffPipeline(context);
        return unboundOlapTableSink;
    }

    @Override
    public Plan visitLogicalFileSink(LogicalFileSink<? extends Plan> fileSink, StatementContext context) {
        turnOffPipeline(context);
        return fileSink;
    }

    private void turnOffPipeline(StatementContext context) {
        SessionVariable sessionVariable = context.getConnectContext().getSessionVariable();
        // set temporary session value, and then revert value in the 'finally block' of StmtExecutor#execute
        sessionVariable.setIsSingleSetVar(true);
        try {
            VariableMgr.setVar(sessionVariable,
                    new SetVar(SessionVariable.ENABLE_PIPELINE_ENGINE, new StringLiteral("false")));
        } catch (Throwable t) {
            throw new AnalysisException("Can not set turn off pipeline for DML", t);
        }
    }
}
