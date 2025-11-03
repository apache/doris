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

package org.apache.doris.nereids.metrics.event;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.metrics.Event;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.Utils;

import java.util.List;

/**
 * transform event
 */
public class TransformEvent extends Event {
    private final GroupExpression groupExpression;
    private final Plan before;
    private final List<Plan> afters;
    private final RuleType ruleType;

    private TransformEvent(GroupExpression groupExpression, Plan before, List<Plan> afters, RuleType ruleType) {
        this.groupExpression = groupExpression;
        this.before = before;
        this.afters = afters;
        this.ruleType = ruleType;
    }

    public static TransformEvent of(GroupExpression groupExpression, Plan before, List<Plan> afters,
            RuleType ruleType) {
        return checkConnectContext(TransformEvent.class)
                ? new TransformEvent(groupExpression, before, afters, ruleType) : null;
    }

    public GroupExpression getGroupExpression() {
        return groupExpression;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("TransformEvent", "groupExpression", groupExpression,
                "before", before, "afters", afters, "ruleType", ruleType);
    }
}
