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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.ivm.IvmNormalizeDupKeysMTMV;
import org.apache.doris.mtmv.ivm.IvmNormalizeUniqKeysMTMV;
import org.apache.doris.mtmv.ivm.IvmPlanSignature;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.SupportChangelog;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Normalizes an IVM MV's query: settles which table model the MV is built with, and hands the query
 * to the normalizer that maintains one.
 *
 * <p>The table model decides what there is to normalize. A unique-key MV is maintained through the
 * hidden row-id, so its query is rewritten into the hidden layout that
 * {@link IvmNormalizeUniqKeysMTMV} builds. A DUP detail MV holds the query's own rows, so its query
 * is checked rather than rewritten, which is what {@link IvmNormalizeDupKeysMTMV} does.
 *
 * <p>The model is settled once, while the MV is created, and read back from the MV's own key model
 * on every later statement -- an MV's key model cannot be altered -- and it is recorded on the
 * statement's {@link IvmRewriteResult} so that the create paths build the MV to match what was
 * normalized here.
 */
public class IvmNormalizeMTMV implements CustomRewriter {
    private static final Logger LOG = LogManager.getLogger(IvmNormalizeMTMV.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        StatementContext statementContext = jobContext.getCascadesContext().getStatementContext();
        IvmRewriteContext rewriteContext = statementContext.getIvmRewriteContext().orElse(null);
        if (rewriteContext == null) {
            // Not an IVM rewrite: nothing to normalize.
            return plan;
        }
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getOrCreateIvmRewriteResult();
        if (rewriteResult.isNormalizeRewritten()) {
            return plan;
        }
        // Which normalizer builds the MV follows from the table model, so the model comes first: from
        // the query while the MV is being created, and from the MV's own key model again afterwards.
        KeysType keysType = resolveKeysType(plan, rewriteContext);
        rewriteResult.setKeysType(keysType);
        boolean detailModel = keysType == KeysType.DUP_KEYS;
        Plan result;
        if (detailModel) {
            // A detail MV holds the query's own rows, so its normalized plan is the query itself:
            // there is nothing to rewrite, only a query to check, and no row-id for a signature to
            // pin down.
            result = new IvmNormalizeDupKeysMTMV().normalize(plan, statementContext.getExcludedTriggerTables());
        } else {
            result = new IvmNormalizeUniqKeysMTMV().normalize(plan, jobContext);
        }
        // Whichever normalizer built it, the normalized plan is published here, and only here, so
        // that a statement downstream of this rewrite reads the plan of the model that was chosen.
        rewriteResult.setNormalizeRewritten(true);
        rewriteResult.setNormalizedPlan(result);
        if (rewriteContext.getMode() == IvmRewriteContext.Mode.CREATE) {
            // The normalized plan is what an incremental refresh is built against, so CREATE logs it
            // whole, together with the signature the unique-key model pins it down by. A detail MV has
            // no signature: it holds the query's own rows, and its refresh reads no layout.
            IvmPlanSignature planSignature = rewriteResult.getPlanSignature();
            LOG.info("IVM normalized plan, mtmvName={}, keysType={}, detailModel={}, inputRoot={}, plan={}, "
                            + "canonicalString={}, signature={}",
                    rewriteContext.getMtmvName(), keysType, detailModel, plan.getClass().getSimpleName(),
                    result.treeString(), planSignature == null ? null : planSignature.getCanonicalString(),
                    planSignature == null ? null : planSignature.getSha256());
        }
        return result;
    }

    /**
     * The key type, and with it the table model, this rewrite must build: the MV's own whenever
     * there is an MV to read it from, and the query's when there is none yet, which only CREATE does
     * without. An MV's key model cannot be altered, so what is read back is what CREATE decided.
     */
    private KeysType resolveKeysType(Plan plan, IvmRewriteContext rewriteContext) {
        MTMV mtmv = rewriteContext.getMtmv();
        if (mtmv != null) {
            return mtmv.isIvmAppendOnlyDupTable() ? KeysType.DUP_KEYS : KeysType.UNIQUE_KEYS;
        }
        Preconditions.checkState(rewriteContext.isCreate(),
                "an IVM rewrite without an MV can only be CREATE, but its mode is: %s",
                rewriteContext.getMode());
        return judgeKeysType(plan, rewriteContext);
    }

    /**
     * Decides which key type an MV being created must be built with, from the query, its base tables
     * and the MV's creation-time properties.
     *
     * <p>{@link KeysType#DUP_KEYS} means the MV is a detail table a refresh only ever appends to,
     * and it needs both halves of "no refresh can undo or rewrite a row the MV already holds": every
     * base table is an append-only detail table, whose changelog carries APPEND events only, and no
     * operator of the query corrects a row already materialized. Of the operators the normalize
     * supports, exactly two do correct one -- an aggregate, whose delta changes the row of a group
     * (DISTINCT included, and GROUPING SETS, which is a repeat of aggregates), and an outer join,
     * whose null side stops matching -- so those are what this asks for. Everything else that would
     * need the row-id path is outside what the normalize supports and is rejected there instead
     * (UNION DISTINCT, semi/anti joins, windows, limits), which is why this names the two rather
     * than every shape that is not.
     *
     * <p>A query that qualifies for the detail model is still built on the unique-key one when the MV
     * is created with {@code ivm_force_mow}, which is the user's way to ask for the model that
     * retracts: this is the one place creation reads it, and afterwards the model is what the MV's
     * own key type says.
     *
     * <p>When it does qualify, the detail model is the cheaper of the two, and building it is what makes
     * the judgment worth making at all: what it saves is in
     * {@link org.apache.doris.catalog.MTMV#isIvmAppendOnlyDupTable}.
     */
    private KeysType judgeKeysType(Plan plan, IvmRewriteContext rewriteContext) {
        if (rewriteContext.isForceMow()) {
            return KeysType.UNIQUE_KEYS;
        }
        boolean needsRowIdMaintenance = plan.anyMatch(node -> node instanceof LogicalAggregate
                || node instanceof LogicalRepeat
                || (node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getJoinType().isOuterJoin()));
        if (needsRowIdMaintenance) {
            return KeysType.UNIQUE_KEYS;
        }
        List<SupportChangelog> scans = plan.collectToList(SupportChangelog.class::isInstance);
        for (SupportChangelog scan : scans) {
            if (!scan.isChangelogAppendOnly()) {
                return KeysType.UNIQUE_KEYS;
            }
        }
        return KeysType.DUP_KEYS;
    }
}
