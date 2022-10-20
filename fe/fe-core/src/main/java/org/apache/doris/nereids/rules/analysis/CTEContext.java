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

import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.HashMap;
import java.util.Map;

/**
 * Context used for CTE analysis and register
 */
public class CTEContext {

    // store CTE name and both initial and analyzed LogicalPlan of with query;
    // The initial LogicalPlan is used to inline a CTE if it is referenced by another CTE,
    // and the analyzed LogicalPlan will be  if it is referenced by the main query.
    private Map<String, LogicalPlan> initialCtePlans;
    private Map<String, LogicalPlan> analyzedCtePlans;

    public CTEContext() {
        initialCtePlans = new HashMap<>();
        analyzedCtePlans = new HashMap<>();
    }

    /**
     * check if cteName can be found in current order
     */
    public boolean containsCTE(String cteName) {
        return initialCtePlans.containsKey(cteName);
    }

    public LogicalPlan getInitialCTEPlan(String cteName) {
        return initialCtePlans.get(cteName);
    }

    public LogicalPlan getAnalyzedCTEPlan(String cteName) {
        return analyzedCtePlans.get(cteName);
    }

    public void putInitialPlan(String cteName, LogicalPlan plan) {
        initialCtePlans.put(cteName, plan);
    }

    public void putAnalyzedPlan(String cteName, LogicalPlan plan) {
        analyzedCtePlans.put(cteName, plan);
    }

}
