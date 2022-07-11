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
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.OperatorVisitor;

import java.util.List;

/**
 * Used for property drive.
 */
public class ChildrenOutputPropertyDeriver extends OperatorVisitor<PhysicalProperties, PlanContext> {
    PhysicalProperties requirements;
    List<PhysicalProperties> childrenOutputProperties;

    public ChildrenOutputPropertyDeriver(PhysicalProperties requirements,
            List<PhysicalProperties> childrenOutputProperties) {
        this.childrenOutputProperties = childrenOutputProperties;
        this.requirements = requirements;
    }

    public static PhysicalProperties getProperties(
            PhysicalProperties requirements,
            List<PhysicalProperties> childrenOutputProperties,
            GroupExpression groupExpression) {

        ChildrenOutputPropertyDeriver childrenOutputPropertyDeriver = new ChildrenOutputPropertyDeriver(requirements,
                childrenOutputProperties);

        return groupExpression.getOperator().accept(childrenOutputPropertyDeriver, new PlanContext(groupExpression));
    }

    public PhysicalProperties getRequirements() {
        return requirements;
    }

    //    public List<List<PhysicalProperties>> getProperties(GroupExpression groupExpression) {
    //        properties = Lists.newArrayList();
    //        groupExpression.getOperator().accept(this, new PlanContext(groupExpression));
    //        return properties;
    //    }

    //    @Override
    //    public Void visitOperator(Operator operator, PlanContext context) {
    //        List<PhysicalProperties> props = Lists.newArrayList();
    //        for (int childIndex = 0; childIndex < context.getGroupExpression().arity(); ++childIndex) {
    //            props.add(new PhysicalProperties());
    //        }
    //        properties.add(props);
    //        return null;
    //    }
    @Override
    public PhysicalProperties visitOperator(Operator node, PlanContext context) {
        return new PhysicalProperties();
    }
}
