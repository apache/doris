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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;

/**
 * Roll up trait, which identify an function could be rolled up if a function appear in query
 * which can be represented by aggregate function in view.
 * Acquire the rolled up function by constructRollUp method.
 */
public interface RollUpTrait {

    /**
     * Construct the roll up function with custom param
     */
    Function constructRollUp(Expression param, Expression... varParams);

    /**
     * identify the function itself can be rolled up
     * Such as Sum can be rolled up directly, but BitmapUnionCount can not
     */
    boolean canRollUp();
}
