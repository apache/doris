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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.OperatorVisitor;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Used for parent property drive.
 */
public class ParentRequiredPropertyDeriver extends OperatorVisitor<Void, PlanContext> {

    PhysicalProperties requestPropertyFromParent;
    List<List<PhysicalProperties>> requiredPropertyListList;

    public ParentRequiredPropertyDeriver(JobContext context) {
        this.requestPropertyFromParent = context.getRequiredProperties();
    }

    public List<List<PhysicalProperties>> getRequiredPropertyListList(GroupExpression groupExpression) {
        requiredPropertyListList = Lists.newArrayList();
        groupExpression.getOperator().accept(this, new PlanContext(groupExpression));
        return requiredPropertyListList;
    }

    @Override
    public Void visitOperator(Operator operator, PlanContext context) {
        List<PhysicalProperties> requiredPropertyList = Lists.newArrayList();
        for (int i = 0; i < context.getGroupExpression().arity(); i++) {
            requiredPropertyList.add(new PhysicalProperties());
        }
        requiredPropertyListList.add(requiredPropertyList);
        return null;
    }

}
