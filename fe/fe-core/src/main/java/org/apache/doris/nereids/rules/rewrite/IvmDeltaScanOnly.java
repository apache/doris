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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;

/**
 * IVM delta rule for SCAN_ONLY pattern.
 * Matches a bare OlapScan MV define plan and produces a delta bundle per changed base table.
 * Implementation deferred — currently a no-op placeholder.
 */
public class IvmDeltaScanOnly implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext == null || !connectContext.getSessionVariable().isEnableIvmDeltaRewrite()) {
            return plan;
        }
        // TODO: implement SCAN_ONLY delta plan generation
        return plan;
    }

    public RuleType getRuleType() {
        return RuleType.IVM_DELTA_SCAN_ONLY;
    }
}
