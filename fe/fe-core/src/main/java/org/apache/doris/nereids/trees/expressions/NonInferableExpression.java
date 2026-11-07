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

package org.apache.doris.nereids.trees.expressions;

/**
 * Marks expressions that must not be used to derive additional predicates by equivalent-expression
 * substitution or propagation to sibling plan branches. For example, a search predicate depends on
 * its source column's index and analyzer, which an equivalent column may not share.
 *
 * <p>This trait does not prevent normal predicate pushdown, alias substitution, or pruning.
 */
public interface NonInferableExpression {
}
