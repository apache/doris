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

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.List;

/**
 * interface for udf
 */
public interface Udf extends ComputeNullable {
    @Override
    default boolean nullable() {
        NullableMode mode = getNullableMode();
        if (mode == NullableMode.ALWAYS_NOT_NULLABLE) {
            return false;
        } else if (mode == NullableMode.ALWAYS_NULLABLE) {
            return true;
        } else if (mode == NullableMode.DEPEND_ON_ARGUMENT) {
            return children().stream().anyMatch(ExpressionTrait::nullable);
        }
        throw new AnalysisException("unsupported nullable mode for udf in Nereids");
    }

    Function getCatalogFunction() throws org.apache.doris.common.AnalysisException;

    NullableMode getNullableMode();

    List<Expression> children();
}
