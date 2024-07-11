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

package org.apache.doris.nereids.cost;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * expression cost is calculated by
 * 1. non-leaf tree node count: N
 * 2. expression which contains input of stringType or complexType(array/json/struct...), add cost
 */
public class ExpressionCostEvaluator extends ExpressionVisitor<Double, Void> {
    private static Map<Class, Double> dataTypeCost = Maps.newHashMap();

    static {
        dataTypeCost.put(DecimalV2Type.class, 1.5);
        dataTypeCost.put(DecimalV3Type.class, 1.5);
        dataTypeCost.put(StringType.class, 2.0);
        dataTypeCost.put(CharType.class, 2.0);
        dataTypeCost.put(VarcharType.class, 2.0);
        dataTypeCost.put(ArrayType.class, 3.0);
        dataTypeCost.put(MapType.class, 3.0);
        dataTypeCost.put(StructType.class, 3.0);
    }

    @Override
    public Double visit(Expression expr, Void context) {
        double cost = 0.0;
        for (Expression child : expr.children()) {
            cost += child.accept(this, context);
            // the more children, the more computing cost
            cost += dataTypeCost.getOrDefault(child.getDataType().getClass(), 0.1);
        }
        return cost;
    }

    @Override
    public Double visitSlotReference(SlotReference slot, Void context) {
        return 0.0;
    }

    @Override
    public Double visitLiteral(Literal literal, Void context) {
        return 0.0;
    }

    @Override
    public Double visitAlias(Alias alias, Void context) {
        Expression child = alias.child();
        if (child instanceof SlotReference) {
            return 0.0;
        }
        return alias.child().accept(this, context);
    }
}
