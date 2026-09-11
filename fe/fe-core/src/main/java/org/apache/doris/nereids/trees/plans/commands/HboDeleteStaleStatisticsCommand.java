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
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manual hbo statistics clean up:
 * <pre>
 *   HBO DELETE STALE STATISTICS;                     -- entries whose tables moved on
 *   HBO DELETE STALE STATISTICS OLDER_THAN 3600;     -- ... and unresolvable ones older than 1h
 * </pre>
 * A hbo fingerprint contains the visible version of every table of its struct info, so a new load
 * (or a dropped table) makes an entry unmatchable: it can never be applied again and is only kept
 * alive by the cache. Such entries are removed here, together with - when {@code OLDER_THAN} is
 * given - the entries whose struct info cannot be resolved any more (e.g. the entry was written by
 * another cluster). Pinned entries only: learned entries are keyed by an internally generated
 * fingerprint and carry no table version.
 */
public class HboDeleteStaleStatisticsCommand extends Command {
    private static final Logger LOG = LogManager.getLogger(HboDeleteStaleStatisticsCommand.class);

    private final Long olderThanSeconds;

    /**
     * HboDeleteStaleStatisticsCommand
     * @param olderThanSeconds when non null, also remove entries which cannot be resolved as soon as
     *                         they are older than this many seconds
     */
    public HboDeleteStaleStatisticsCommand(Long olderThanSeconds) {
        super(PlanType.HBO_STATISTICS_COMMAND);
        this.olderThanSeconds = olderThanSeconds;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied: HBO statistics management requires ADMIN privilege");
        }
        long olderThanMillis = olderThanSeconds == null ? -1 : olderThanSeconds * 1000;
        int removed = Env.getCurrentEnv().getHboPlanStatisticsManager()
                .deleteStalePinnedPlanStatistics(olderThanMillis);
        LOG.info("HBO DELETE STALE STATISTICS removed {} pinned entries (olderThanSeconds={})",
                removed, olderThanSeconds);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    public Long getOlderThanSeconds() {
        return olderThanSeconds;
    }
}
