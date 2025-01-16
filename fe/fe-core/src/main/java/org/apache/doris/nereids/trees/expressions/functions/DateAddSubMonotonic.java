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

import org.apache.doris.nereids.trees.expressions.literal.Literal;

/** monotonicity for XX_ADD XX_SUB */
public interface DateAddSubMonotonic extends Monotonic {
    @Override
    default boolean isMonotonic(Literal lower, Literal upper) {
        return !(child(0) instanceof Literal) && child(1) instanceof Literal;
    }

    @Override
    default boolean isPositive() {
        return true;
    }

    @Override
    default int getMonotonicFunctionChildIndex() {
        return 0;
    }

    @Override
    default boolean isStrictlyMonotonic() {
        return isMonotonic(null, null);
    }
}
