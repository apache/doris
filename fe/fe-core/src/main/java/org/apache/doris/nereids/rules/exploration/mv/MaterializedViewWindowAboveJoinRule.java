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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;

/**
 * MaterializedViewWindowAboveJoinRule this rule is used to rewrite a query with a
 * materialized view which has a window
 */
public class MaterializedViewWindowAboveJoinRule extends AbstractMaterializedViewJoinRule {
    @Override
    public List<Rule> buildRules() {
        return null;
    }

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping targetToSourceMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        return super.rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, targetToSourceMapping,
                tempRewritedPlan, materializationContext, cascadesContext);
    }

    @Override
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        return super.checkQueryPattern(structInfo, cascadesContext);
    }

    @Override
    protected boolean checkMaterializationPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        return super.checkMaterializationPattern(structInfo, cascadesContext);
    }
}
