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

import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.View;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maintain the context for query rewrite by materialized view
 */
public class MaterializationContext {

    // TODO add MaterializedView class
    private final Plan mvPlan;
    private final CascadesContext context;
    private final List<Table> baseTables;
    private final List<View> baseViews;
    // Group ids that are rewritten by this mv to reduce rewrite times
    private final Set<GroupId> matchedGroups = new HashSet<>();
    private final Plan scanPlan;

    public MaterializationContext(Plan mvPlan, CascadesContext context,
            List<Table> baseTables, List<View> baseViews, Plan scanPlan) {
        this.mvPlan = mvPlan;
        this.context = context;
        this.baseTables = baseTables;
        this.baseViews = baseViews;
        this.scanPlan = scanPlan;
    }

    public Set<GroupId> getMatchedGroups() {
        return matchedGroups;
    }

    public void addMatchedGroup(GroupId groupId) {
        matchedGroups.add(groupId);
    }

    public Plan getMvPlan() {
        return mvPlan;
    }

    public Plan getScanPlan() {
        return scanPlan;
    }
}
