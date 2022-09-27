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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Common interface for logical/physical project.
 */
public interface Project {
    List<NamedExpression> getProjects();

    /**
     * Generate a map that the key is the alias slot, corresponding value is the expression produces the slot.
     * For example:
     * <pre>
     * projects:
     * [a, alias(b as c), alias((d + e + 1) as f)]
     * result map:
     * c -> b
     * f -> d + e + 1
     * </pre>
     */
    default Map<Slot, Expression> getAliasToProducer() {
        return getProjects()
                .stream()
                .filter(Alias.class::isInstance)
                .collect(
                        Collectors.toMap(
                                NamedExpression::toSlot,
                                // Avoid cast to alias, retrieving the first child expression.
                                alias -> alias.child(0)
                        )
                );
    }
}
