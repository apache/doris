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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.FunctionTrait;
import org.apache.doris.nereids.types.DataType;

/** BitmapFunction */
public interface BitmapFunction extends FunctionTrait {
    @Override
    default void checkLegalityBeforeTypeCoercion() {
        DataType argumentType = getArgumentType(0);
        if (!argumentType.isBitmapType()) {
            throw new AnalysisException(getName()
                    + " function's argument should be of BITMAP type, but was " + argumentType);
        }
    }
}
