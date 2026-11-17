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

package org.apache.doris.nereids.trees.expressions.functions;

/**
 * Opt-in capability for moving a date interval across a comparison.
 *
 * <p>The opposite operation must be an exact, order-preserving inverse on the arithmetic's
 * valid input domain. This excludes month/year clamping even when the function is monotonic.
 * A bound signature with additional input-domain requirements must decline rearrangement when
 * those requirements cannot be proven. In particular, a valid UTC timestamp need not have a
 * representable session-local date, so TIMESTAMPTZ calendar DAY arithmetic cannot opt in.
 *
 * <p>This capability does not validate a particular comparison boundary. The caller must also
 * evaluate the inverse boundary using FE arithmetic and retain the original comparison if it
 * fails or does not produce a date literal. Monotonicity is a separate range-inference property.
 */
public interface DateAddSubRearrangeable extends ExpressionTrait {
    default boolean canRearrangeComparison() {
        return child(1).isConstant();
    }
}
