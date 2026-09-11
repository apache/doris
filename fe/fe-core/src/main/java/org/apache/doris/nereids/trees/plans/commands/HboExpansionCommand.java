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

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Manual join expansion management:
 * <pre>
 *   HBO SET EXPANSION '&lt;condFingerprint&gt;' = &lt;expansion&gt; [COND '&lt;JE{...}&gt;']
 *   HBO DELETE EXPANSION '&lt;condFingerprint&gt;'
 * </pre>
 * The injected value is the measured fan-out factor of the join equality conditions
 * ({@code output rows / max(left rows, right rows)}, so {@code >= 1}). At planning time the join
 * estimate becomes {@code clamp(expansion * max(leftEst, rightEst), 1, leftEst * rightEst)}, which
 * makes a join known to explode look expensive and therefore scheduled as late as possible. It is
 * a join-order control knob, not an accuracy correction; semi / anti style joins can not expand
 * and deliberately ignore the entry.
 */
public class HboExpansionCommand extends Command {

    /** Operation kind. */
    public enum Op {
        SET,
        DELETE
    }

    private static final Pattern FINGERPRINT_PATTERN = Pattern.compile("[0-9a-fA-F]{64}");

    private final Op op;
    private final String condFingerprint;
    private final double expansion;
    private final String condCanonical;

    /**
     * HboExpansionCommand
     * @param op SET or DELETE
     * @param condFingerprint sha256 of the canonical join equality-condition set
     * @param expansion measured expansion factor (&gt;= 1), only meaningful for SET
     * @param condCanonical optional human readable canonical condition string
     */
    public HboExpansionCommand(Op op, String condFingerprint, double expansion, String condCanonical) {
        super(PlanType.HBO_EXPANSION_COMMAND);
        this.op = op;
        this.condFingerprint = condFingerprint == null ? null : condFingerprint.toLowerCase(Locale.ROOT);
        this.expansion = expansion;
        this.condCanonical = condCanonical == null ? "" : condCanonical;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied: HBO statistics management requires ADMIN privilege");
        }
        if (condFingerprint == null || !FINGERPRINT_PATTERN.matcher(condFingerprint).matches()) {
            throw new AnalysisException("invalid hbo condition fingerprint, expect 64 hex chars: " + condFingerprint);
        }
        HboPlanStatisticsManager hboManager = Env.getCurrentEnv().getHboPlanStatisticsManager();
        switch (op) {
            case SET:
                if (expansion < 1) {
                    throw new AnalysisException("hbo join expansion must be greater than or equal to 1: " + expansion);
                }
                hboManager.putPinnedJoinExpansion(condFingerprint, expansion, condCanonical);
                break;
            case DELETE:
                hboManager.removePinnedJoinExpansion(condFingerprint);
                break;
            default:
                throw new IllegalStateException("unexpected hbo expansion op: " + op);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    public Op getOp() {
        return op;
    }

    public String getCondFingerprint() {
        return condFingerprint;
    }

    public double getExpansion() {
        return expansion;
    }

    public String getCondCanonical() {
        return condCanonical;
    }
}
