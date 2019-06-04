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

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptColumnRefFactory;

import java.util.List;

public final class RuleCallContext {

    private final OptExpression origin;
    private final List<OptExpression> newExprs;
    private final OptColumnRefFactory factory;

    public RuleCallContext(OptExpression origin, OptColumnRefFactory factory) {
        this.origin = origin;
        this.factory = factory;
        this.newExprs = Lists.newArrayList();
    }

    public OptExpression getOrigin() {
        return origin;
    }

    public void addNewExpr(OptExpression newExpr) {
        this.newExprs.add(newExpr);
    }
    public List<OptExpression> getNewExpr() { return this.newExprs; }
    public OptColumnRefFactory getColumnRefFactor() {
        return factory;
    }
}
