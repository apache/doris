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

package org.apache.doris.plsql.util;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.plsql.Var;


public class TypeUtil {
    /*
     * convert plsql type to nereids type
     */
    public static DataType convertForCatalogType(Var var) {
        switch (var.type) {
            case BIGINT:
                return BigIntType.INSTANCE;
            case STRING:
                return StringType.INSTANCE;
            case DECIMAL:
                return DecimalV3Type.INSTANCE;
            case DOUBLE:
                return DoubleType.INSTANCE;
            case DATE:
                return DateType.INSTANCE;
            case TIMESTAMP:
                return TimeV2Type.INSTANCE;
            case BOOL:
                return BooleanType.INSTANCE;
            default:
                return NullType.INSTANCE;
        }
    }

    /*
     * cast plsql value to doris expr
     */
    public static Expression castForDorisExpr(Var var) {
        Literal literal = Literal.of(var.value);
        return TypeCoercionUtils.castIfNotMatchType(literal, convertForCatalogType(var));
    }
}



