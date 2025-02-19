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

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * AnalyzeCommand
 */
public abstract class AnalyzeCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(AnalyzeCommand.class);

    protected AnalyzeProperties analyzeProperties;

    /**
     * AnalyzeCommand
     */
    public AnalyzeCommand(PlanType planType, AnalyzeProperties analyzeProperties) {
        super(planType);
        this.analyzeProperties = analyzeProperties;
    }

    /**
     * checkAndSetSample
     */
    public void checkAndSetSample() throws AnalysisException {
        if (analyzeProperties.forceFull()) {
            // if the user trys hard to do full, we stop him hard.
            throw new AnalysisException(
                    "analyze with full is forbidden for performance issue in cloud mode, use `with sample` then");
        }
        if (!analyzeProperties.isSample()) {
            // otherwise, we gently translate it to use sample
            LOG.warn("analyze with full is forbidden for performance issue in cloud mode, force to use sample");
            analyzeProperties.setSampleRows(StatisticsUtil.getHugeTableSampleRows());
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        doRun(ctx, executor);
    }

    public abstract void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception;

}
