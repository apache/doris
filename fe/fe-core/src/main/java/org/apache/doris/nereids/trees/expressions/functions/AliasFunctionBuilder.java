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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.PlaceholderSlot;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

/**
 * specific function builder for alias function.
 */
public class AliasFunctionBuilder extends FunctionBuilder {
    private final BoundFunction originalFunction;
    private final List<DataType> argTypes;
    private final List<PlaceholderSlot> placeholders;

    public AliasFunctionBuilder(BoundFunction originalFunction, List<DataType> argTypes,
            List<PlaceholderSlot> placeholders, Constructor<BoundFunction> builderMethod) {
        super(builderMethod);
        this.originalFunction = originalFunction;
        this.argTypes = argTypes;
        this.placeholders = placeholders;
    }

    @Override
    public BoundFunction build(String name, List<?> arguments) {
        Map<PlaceholderSlot, Object> replaceMap = Maps.newHashMap();
        for (int i = 0; i < placeholders.size(); ++i) {
            replaceMap.put(placeholders.get(i), arguments.get(i));
        }
        return PlaceholderBounder.INSTANCE.replace(originalFunction, replaceMap);
    }

    private static class PlaceholderBounder extends DefaultExpressionRewriter<Map<PlaceholderSlot, Object>> {
        public static final PlaceholderBounder INSTANCE = new PlaceholderBounder();

        public BoundFunction replace(Expression expression, Map<PlaceholderSlot, Object> context) {
            return ((BoundFunction) expression.accept(this, context));
        }

        @Override
        public Expression visitPlaceholderSlot(PlaceholderSlot slot, Map<PlaceholderSlot, Object> context) {
            return ((Expression) context.get(slot));
        }
    }
}
