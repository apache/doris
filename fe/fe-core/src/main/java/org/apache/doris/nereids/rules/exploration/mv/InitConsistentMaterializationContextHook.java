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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlannerHook;

import com.google.common.annotations.VisibleForTesting;

import java.util.Set;

/**
 * If enable query rewrite with mv in dml, should init consistent materialization context after analyze
 */
public class InitConsistentMaterializationContextHook extends InitMaterializationContextHook implements PlannerHook {

    public static final InitConsistentMaterializationContextHook INSTANCE =
            new InitConsistentMaterializationContextHook();

    @VisibleForTesting
    @Override
    public void initMaterializationContext(CascadesContext cascadesContext) {
        if (!cascadesContext.getConnectContext().getSessionVariable().isEnableDmlMaterializedViewRewrite()) {
            return;
        }
        super.doInitMaterializationContext(cascadesContext);
    }

    protected Set<MTMV> getAvailableMTMVs(Set<TableIf> usedTables, CascadesContext cascadesContext) {
        return Env.getCurrentEnv().getMtmvService().getRelationManager()
                .getAvailableMTMVs(cascadesContext.getStatementContext().getCandidateMTMVs(),
                        cascadesContext.getConnectContext(),
                        true, ((connectContext, mtmv) -> {
                            return MTMVUtil.mtmvContainsExternalTable(mtmv) && (!connectContext.getSessionVariable()
                                    .isEnableDmlMaterializedViewRewriteWhenBaseTableUnawareness());
                        }));
    }
}
