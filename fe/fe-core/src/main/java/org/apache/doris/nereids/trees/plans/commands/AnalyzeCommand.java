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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.AnalyzeDatabaseOp;
import org.apache.doris.nereids.trees.plans.commands.info.AnalyzeOp;
import org.apache.doris.nereids.trees.plans.commands.info.AnalyzeTableOp;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * AnalyzeCommand
 */
public class AnalyzeCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(AnalyzeCommand.class);

    private AnalyzeOp analyzeOp;

    /**
     * AnalyzeCommand
     */
    public AnalyzeCommand(AnalyzeOp analyzeOp) {
        super(analyzeOp.getPlanType());
        this.analyzeOp = analyzeOp;
    }

    /**
     * checkAndSetSample
     */
    public void checkAndSetSample() throws AnalysisException {
        if (analyzeOp.getAnalyzeProperties().forceFull()) {
            // if the user trys hard to do full, we stop him hard.
            throw new AnalysisException(
                    "analyze with full is forbidden for performance issue in cloud mode, use `with sample` then");
        }
        if (!analyzeOp.getAnalyzeProperties().isSample()) {
            // otherwise, we gently translate it to use sample
            LOG.warn("analyze with full is forbidden for performance issue in cloud mode, force to use sample");
            analyzeOp.getAnalyzeProperties().setSampleRows(StatisticsUtil.getHugeTableSampleRows());
        }
    }

    public AnalyzeOp getAnalyzeOp() {
        return analyzeOp;
    }

    /**
     * validate
     */
    private void validate(ConnectContext ctx) throws UserException {
        Preconditions.checkState((analyzeOp instanceof AnalyzeDatabaseOp
                || analyzeOp instanceof AnalyzeTableOp));

        analyzeOp.validate(ctx);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().analyze(this, executor.isProxy());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
