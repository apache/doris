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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;

/**
 * LogicalPlan must compute and return non-null LogicalProperties without exception,
 * so UnboundRelation.computeLogicalProperties() return a UnboundLogicalProperties temporary.
 */
public class UnboundLogicalProperties extends LogicalProperties {
    public static final UnboundLogicalProperties INSTANCE = new UnboundLogicalProperties();

    private UnboundLogicalProperties() {
        super(ImmutableList::of, () -> FunctionalDependencies.EMPTY_FUNC_DEPS, ImmutableSet::of);
    }

    @Override
    public List<Slot> getOutput() {
        throw new UnboundException("getOutput");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof UnboundLogicalProperties;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
