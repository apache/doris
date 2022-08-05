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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Optional;

/**
 * The slot range required for expression analyze.
 */
public class Scope {
    private final Optional<Scope> outerScope;
    private final List<Slot> slots;

    public Scope(Optional<Scope> outerScope, List<Slot> slots) {
        this.outerScope = outerScope;
        this.slots = slots;
    }

    public Scope(List<Slot> slots) {
        this.outerScope = Optional.empty();
        this.slots = slots;
    }

    public List<Slot> getSlots() {
        return slots;
    }

    public Optional<Scope> getOuterScope() {
        return outerScope;
    }

    /**
     * generate scope link from inner to outer.
     */
    public List<Scope> toScopeLink() {
        Scope scope = this;
        Builder<Scope> builder = ImmutableList.<Scope>builder().add(scope);
        while (scope.getOuterScope().isPresent()) {
            scope = scope.getOuterScope().get();
            builder.add(scope);
        }
        return builder.build();
    }
}
