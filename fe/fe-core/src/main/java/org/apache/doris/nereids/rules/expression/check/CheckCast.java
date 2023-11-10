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

package org.apache.doris.nereids.rules.expression.check;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import java.util.List;

/**
 * check cast valid
 */
public class CheckCast extends AbstractExpressionRewriteRule {

    public static final CheckCast INSTANCE = new CheckCast();

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        rewrite(cast.child(), context);
        DataType originalType = cast.child().getDataType();
        DataType targetType = cast.getDataType();
        if (!check(originalType, targetType)) {
            throw new AnalysisException("cannot cast " + originalType.toSql() + " to " + targetType.toSql());
        }
        return cast;
    }

    private boolean check(DataType originalType, DataType targetType) {
        if (originalType.isNullType()) {
            return true;
        }
        if (originalType.equals(targetType)) {
            return true;
        }
        if (originalType instanceof CharacterType && !(targetType instanceof PrimitiveType)) {
            return true;
        }
        if (originalType instanceof ArrayType && targetType instanceof ArrayType) {
            return check(((ArrayType) originalType).getItemType(), ((ArrayType) targetType).getItemType());
        } else if (originalType instanceof MapType && targetType instanceof MapType) {
            return check(((MapType) originalType).getKeyType(), ((MapType) targetType).getKeyType())
                    && check(((MapType) originalType).getValueType(), ((MapType) targetType).getValueType());
        } else if (originalType instanceof StructType && targetType instanceof StructType) {
            List<StructField> targetFields = ((StructType) targetType).getFields();
            List<StructField> originalFields = ((StructType) originalType).getFields();
            if (targetFields.size() != originalFields.size()) {
                return false;
            }
            for (int i = 0; i < targetFields.size(); i++) {
                if (originalFields.get(i).isNullable() != targetFields.get(i).isNullable()) {
                    return false;
                }
                if (!check(originalFields.get(i).getDataType(), targetFields.get(i).getDataType())) {
                    return false;
                }
            }
            return true;
        } else if (originalType instanceof JsonType || targetType instanceof JsonType) {
            return true;
        } else {
            return checkPrimitiveType(originalType, targetType);
        }
    }

    /**
     * forbid this original and target type
     *   1. original type is object type
     *   2. target type is object type
     *   3. original type is same with target type
     *   4. target type is null type
     */
    private boolean checkPrimitiveType(DataType originalType, DataType targetType) {
        if (!originalType.isPrimitive() || !targetType.isPrimitive()) {
            return false;
        }
        if (originalType.equals(targetType)) {
            return false;
        }
        if (originalType.isNullType()) {
            return true;
        }
        if (originalType.isObjectType() || targetType.isObjectType()) {
            return false;
        }
        if (targetType.isNullType()) {
            return false;
        }
        if (targetType.isTimeLikeType() && !(originalType.isIntegralType()
                || originalType.isStringLikeType() || originalType.isFloatLikeType())) {
            return false;
        }
        return true;
    }
}
