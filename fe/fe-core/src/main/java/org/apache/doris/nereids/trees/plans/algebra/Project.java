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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Common interface for logical/physical project.
 */
public interface Project {
    List<NamedExpression> getProjects();

    /**
     * Generate a map that the key is the project output slot, corresponding value is the expression produces the slot.
     * Note that alias is striped off.
     */
    default Map<Expression, Expression> getSlotToProducer() {
        return getProjects()
                .stream()
                .collect(Collectors.toMap(
                        NamedExpression::toSlot,
                        namedExpr -> {
                            if (namedExpr instanceof Alias) {
                                return ((Alias) namedExpr).child();
                            } else {
                                return namedExpr;
                            }
                        })
                );
    }
}
