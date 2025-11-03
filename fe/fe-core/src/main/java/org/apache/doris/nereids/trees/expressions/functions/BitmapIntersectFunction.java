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
import org.apache.doris.nereids.types.DataType;

/** BitmapIntersectFunction */
public interface BitmapIntersectFunction extends FunctionTrait {
    @Override
    default void checkLegalityBeforeTypeCoercion() {
        String functionName = getName();
        if (arity() <= 2) {
            throw new AnalysisException(functionName + "(bitmap_column, column_to_filter, filter_values) "
                    + "function requires at least three parameters");
        }

        DataType inputType = getArgumentType(0);
        if (!inputType.isBitmapType()) {
            throw new AnalysisException(
                    functionName + "function first argument should be of BITMAP type, but was " + inputType);
        }

        for (int i = 2; i < arity(); i++) {
            if (!getArgument(i).isConstant()) {
                throw new AnalysisException(functionName + " function filter_values arg must be constant");
            }
        }
    }
}
