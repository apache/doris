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

import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * An interface provided some PatternDescriptor.
 * Child Interface(RuleFactory) can use to declare a pattern shape, then convert to a rule.
 * In the future, we will generate this interface by codegen.
 */
public interface Patterns {
    // need implement
    RulePromise defaultPromise();

    /* special pattern descriptors */

    default <T extends Plan> PatternDescriptor<T> any() {
        return new PatternDescriptor<>(Pattern.ANY, defaultPromise());
    }

    default <T extends Plan> PatternDescriptor<T> multi() {
        return new PatternDescriptor<>(Pattern.MULTI, defaultPromise());
    }

    default <T extends Plan> PatternDescriptor<T> subTree(Class<? extends Plan>... subTreeNodeTypes) {
        return new PatternDescriptor<>(new SubTreePattern(subTreeNodeTypes), defaultPromise());
    }
}
