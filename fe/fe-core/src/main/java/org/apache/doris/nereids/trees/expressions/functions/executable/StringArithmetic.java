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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

/**
 * executable functions:
 * concat
 */
public class StringArithmetic {
    /**
     * Executable arithmetic functions concat
     */
    @ExecFunction(name = "concat", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression concatVarcharVarchar(VarcharLiteral first, VarcharLiteral second) {
        String result = first.getValue() + second.getValue();
        return new VarcharLiteral(result);
    }

    /**
     * Executable arithmetic functions substring
     */
    // substring constructor would add
    @ExecFunction(name = "substring", argTypes = {"VARCHAR", "INT", "INT"}, returnType = "VARCHAR")
    public static Expression substringVarcharIntInt(VarcharLiteral first, IntegerLiteral second, IntegerLiteral third) {
        String result = null;
        if (third.getValue() > first.getValue().length()) {
            result = first.getValue().substring(second.getValue());
        } else {
            result = first.getValue().substring(second.getValue(), third.getValue());
        }
        return new VarcharLiteral(result);
    }

}
