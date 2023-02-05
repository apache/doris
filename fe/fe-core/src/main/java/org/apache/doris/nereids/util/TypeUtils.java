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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Subtract;

/**
 * Judgment expression type.
 */
public class TypeUtils {

    public static boolean isAddOrSubtract(Expression expr) {
        return isAdd(expr) || isSubtract(expr);
    }

    public static boolean isAdd(Expression expr) {
        return expr instanceof Add;
    }

    public static boolean isSubtract(Expression expr) {
        return expr instanceof Subtract;
    }

    public static boolean isMultiplyOrDivide(Expression expr) {
        return isMultiply(expr) || isDivide(expr);
    }

    public static boolean isDivide(Expression expr) {
        return expr instanceof Divide;
    }

    public static boolean isMultiply(Expression expr) {
        return expr instanceof Multiply;
    }

}
