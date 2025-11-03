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
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.Utils;

/**
 * enforcer event
 */
public class EnforcerEvent extends Event {
    private final GroupExpression groupExpression;
    private final PhysicalPlan enforce;
    private final PhysicalProperties before;
    private final PhysicalProperties after;

    private EnforcerEvent(GroupExpression groupExpression, PhysicalPlan enforce, PhysicalProperties before,
            PhysicalProperties after) {
        this.groupExpression = groupExpression;
        this.enforce = enforce;
        this.before = before;
        this.after = after;
    }

    public static EnforcerEvent of(GroupExpression groupExpression, PhysicalPlan enforce, PhysicalProperties before,
            PhysicalProperties after) {
        return checkConnectContext(EnforcerEvent.class)
                ? new EnforcerEvent(groupExpression, enforce, before, after) : null;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("EnforcerEvent", "groupExpression", groupExpression,
                "enforce", enforce,
                "before", before,
                "after", after);
    }
}
