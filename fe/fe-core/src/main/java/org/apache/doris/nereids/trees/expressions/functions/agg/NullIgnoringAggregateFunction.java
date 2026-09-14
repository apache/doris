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

package org.apache.doris.nereids.trees.expressions.functions.agg;

/**
 * Marker for aggregate functions that ignore a row when any aggregate argument is SQL NULL.
 *
 * <p>Removing such rows does not change the aggregate result, so {@code InferAggNotNull} may add
 * inferred not-null predicates for their arguments. This describes input-row handling and is
 * independent of whether the aggregate result itself is nullable.
 *
 * <p>Implement this interface only when the contract holds for every signature and mode of the
 * aggregate function, including DISTINCT.
 */
public interface NullIgnoringAggregateFunction {
}
