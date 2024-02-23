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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;

/**
 * Window functions, as known as analytic functions.
 */
public abstract class WindowFunction extends BoundFunction implements SupportWindowAnalytic {

    public WindowFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public WindowFunction(String name, List<Expression> children) {
        super(name, children);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowFunction that = (WindowFunction) o;
        return Objects.equals(getName(), that.getName())
                && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), children);
    }

    /**
     * LAG/LEAD param must be const, and offset must be number
     */
    protected void checkValidParams(Expression param, boolean isOffset) {
        DataType type = param.getDataType();
        if (isOffset == true && !type.isNumericType()) {
            throw new AnalysisException("The offset of LAG/LEAD must be a number: " + this.toSql());
        }
        if (!param.isConstant()) {
            throw new AnalysisException(
                    "The parameter 2 or parameter 3 of LAG/LEAD must be a constant value: " + this.toSql());
        }
    }
}
