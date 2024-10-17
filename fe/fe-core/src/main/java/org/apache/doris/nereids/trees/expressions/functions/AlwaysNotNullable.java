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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;

/**
 * nullable is always false.
 *
 * e.g. `count(*)`, the output column is always not nullable
 */
public interface AlwaysNotNullable extends ComputeNullable {
    @Override
    default boolean nullable() {
        return false;
    }

    // return value of this function if the input data is empty.
    // for example, count(*) of empty table is 0;
    default Expression resultForEmptyInput() {
        throw new AnalysisException("should implement resultForEmptyInput() for " + this.getClass());
    }
}
