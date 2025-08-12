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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.errors.QueryPlanningErrors;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.qe.SessionVariable;

import java.util.concurrent.TimeUnit;

/** RewriteJob */
public interface RewriteJob {

    void execute(JobContext jobContext);

    boolean isOnce();

    /** checkTimeout */
    default void checkTimeout(JobContext jobContext) {
        CascadesContext context = jobContext.getCascadesContext();
        SessionVariable sessionVariable = context.getConnectContext().getSessionVariable();
        long elapsedS = context.getStatementContext().getStopwatch().elapsed(TimeUnit.MILLISECONDS) / 1000;
        if (sessionVariable.enableNereidsTimeout && elapsedS > sessionVariable.nereidsTimeoutSecond) {
            throw QueryPlanningErrors.planTimeoutError(elapsedS, sessionVariable.nereidsTimeoutSecond,
                    context.getConnectContext().getExecutor().getSummaryProfile());
        }
    }
}
