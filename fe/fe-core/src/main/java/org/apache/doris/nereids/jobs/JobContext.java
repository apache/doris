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

package org.apache.doris.nereids.jobs;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Context for one job in Nereids' cascades framework.
 */
public class JobContext {
    protected final CascadesContext cascadesContext;
    protected final PhysicalProperties requiredProperties;
    protected double costUpperBound;
    protected boolean rewritten = false;

    protected Map<RuleType, Integer> ruleInvokeTimes = Maps.newLinkedHashMap();

    public JobContext(CascadesContext cascadesContext, PhysicalProperties requiredProperties, double costUpperBound) {
        this.cascadesContext = cascadesContext;
        this.requiredProperties = requiredProperties;
        this.costUpperBound = costUpperBound;
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    public PhysicalProperties getRequiredProperties() {
        return requiredProperties;
    }

    public double getCostUpperBound() {
        return costUpperBound;
    }

    public void setCostUpperBound(double costUpperBound) {
        this.costUpperBound = costUpperBound;
    }

    public boolean isRewritten() {
        return rewritten;
    }

    public void setRewritten(boolean rewritten) {
        this.rewritten = rewritten;
    }

    public void onInvokeRule(RuleType ruleType) {
        addRuleInvokeTimes(ruleType);
    }

    private void addRuleInvokeTimes(RuleType ruleType) {
        Integer times = ruleInvokeTimes.get(ruleType);
        if (times == null) {
            times = 0;
        }
        ruleInvokeTimes.put(ruleType, times + 1);
    }
}
