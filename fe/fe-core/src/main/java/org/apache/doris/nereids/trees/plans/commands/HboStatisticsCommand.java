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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * Manual HBO statistics management statements:
 * <pre>
 *   HBO SET STATISTICS '&lt;fingerprint&gt;' = &lt;rows&gt;    -- inject/overwrite a pinned statistics entry
 *   HBO DELETE STATISTICS '&lt;fingerprint&gt;'             -- remove a pinned statistics entry
 * </pre>
 * Pinned entries are authoritative over the automatically collected (learned) hbo statistics:
 * on the read side a pinned hit directly overrides the estimated output row count with the
 * injected value (withRowCountAndHboFlag semantics), and the automatic profile-based publish
 * never overwrites or evicts pinned entries.
 */
public class HboStatisticsCommand extends Command {

    /** Operation kind. */
    public enum Op {
        SET,
        DELETE
    }

    private final Op op;
    private final String fingerprint;
    private final long rows;

    /**
     * HboStatisticsCommand
     * @param op SET or DELETE
     * @param fingerprint hbo fingerprint (sha256 of the simplified group struct info)
     * @param rows injected output row count (only meaningful for SET)
     */
    public HboStatisticsCommand(Op op, String fingerprint, long rows) {
        super(PlanType.HBO_STATISTICS_COMMAND);
        this.op = op;
        this.fingerprint = fingerprint;
        this.rows = rows;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied: HBO statistics management requires ADMIN privilege");
        }
        HboPlanStatisticsManager hboManager = Env.getCurrentEnv().getHboPlanStatisticsManager();
        switch (op) {
            case SET:
                hboManager.putPinnedPlanStatistics(fingerprint, rows, "");
                break;
            case DELETE:
                hboManager.removePinnedPlanStatistics(fingerprint);
                break;
            default:
                throw new IllegalStateException("unexpected hbo statistics op: " + op);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    public Op getOp() {
        return op;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public long getRows() {
        return rows;
    }
}
