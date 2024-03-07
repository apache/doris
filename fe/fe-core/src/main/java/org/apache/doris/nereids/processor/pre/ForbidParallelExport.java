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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;

/**
 * TODO turnoff pipeline for any dml temporary, remove this pre-process when pipeline-sink is ok.
 */
public class ForbidParallelExport extends PlanPreprocessor {

    @Override
    public Plan visitLogicalFileSink(LogicalFileSink<? extends Plan> fileSink, StatementContext context) {
        if (context.getConnectContext().getSessionVariable().enableParallelOutfile) {
            throw new AnalysisException("Nereids do not support parallel outfile now,"
                    + "please turn off it or change to legacy planner by set enable_nereids_planner=false");
        }
        return fileSink;
    }
}
