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

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.memo.GroupExpression;

import com.google.common.collect.Lists;

import java.util.List;

public class ChildPropertyDeriver {
    PhysicalProperties requirements;
    List<List<PhysicalProperties>> properties;

    public ChildPropertyDeriver(PlannerContext context, GroupExpression groupExpression) {
        requirements = context.getPhysicalProperties();

        properties = Lists.newArrayList();
        // set properties/.. for some kinds of operator
        // groupExpression.getOperator().accept()
    }

    public PhysicalProperties getRequirements() {
        return requirements;
    }

    public List<List<PhysicalProperties>> getProperties() {
        return properties;
    }

}
