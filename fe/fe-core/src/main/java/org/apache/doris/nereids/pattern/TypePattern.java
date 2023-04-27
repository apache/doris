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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/** pattern that used to match class type. */
public class TypePattern<TYPE extends Plan>
        extends Pattern<TYPE> {
    protected final Class<TYPE> type;

    public TypePattern(Class clazz, Pattern... children) {
        super(PlanType.UNKNOWN, children);
        this.type = Objects.requireNonNull(clazz, "class can not be null");
    }

    public TypePattern(Class<TYPE> clazz, List<Predicate<TYPE>> predicates, Pattern... children) {
        super(PlanType.UNKNOWN, predicates, children);
        this.type = Objects.requireNonNull(clazz, "class can not be null");
    }

    @Override
    public TypePattern<TYPE> withPredicates(List<Predicate<TYPE>> predicates) {
        return new TypePattern<>(type, predicates, children.toArray(new Pattern[0]));
    }

    @Override
    public boolean matchRoot(Plan plan) {
        return type.isInstance(plan);
    }
}
