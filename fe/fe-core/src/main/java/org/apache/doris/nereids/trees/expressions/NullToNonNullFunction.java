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
 * Marker interface for expressions that can convert NULL input into a non-NULL output.
 *
 * For example: COALESCE(NULL, 2) → 2, NVL(NULL, 0) → 0, NULL_OR_EMPTY(NULL) → true.
 *
 * This is significant for outer-join push-down safety: when an aggregate function contains
 * a NullToNonNull expression wrapping a column from the nullable side of an outer join,
 * the aggregation must NOT be pushed down. Null-extended rows (produced by the join for
 * unmatched rows) have NULL for all nullable-side columns. The NullToNonNull expression
 * would convert those NULLs to non-NULL values, and the pre-aggregation would miss those
 * contributions because null-extended rows do not exist in the base table.
 */
public interface NullToNonNullFunction {
}
