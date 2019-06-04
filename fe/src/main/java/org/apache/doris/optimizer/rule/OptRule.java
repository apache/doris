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

package org.apache.doris.optimizer.rule;

import org.apache.doris.optimizer.OptExpression;

import java.util.List;

// OptRule is used to match a MultiExpression which is under exploring.
// If pattern is matched, it means that this rule can be applied to that MultiExpression
// Transform will be called to transform a Expression to substituted Expresion.
// This is used in ExpressionExploreTask or ExpressionImplementTask to transform a
// Expression to another
public abstract class OptRule {
    private OptRuleType type;
    private OptExpression pattern;

    protected OptRule(OptRuleType type, OptExpression pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    // return rule type, used to
    public OptRuleType type() { return type; }
    public String name() { return type.getName(); }
    public boolean isExploration() { return false; }
    public boolean isImplementation() { return false; }
    public OptExpression getPattern() { return pattern; }
    public OptRuleType getType() { return type; }
    public boolean isApplyOnce() { return false; }
    public boolean isCompatible(OptRuleType type) { return true; }

    // Transform before Expression to other Expressions
    public abstract void transform(RuleCallContext call);

    public String debugString() {
        return name();
    }

}
