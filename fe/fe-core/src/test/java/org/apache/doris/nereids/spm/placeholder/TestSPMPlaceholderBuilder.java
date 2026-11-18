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

package org.apache.doris.nereids.spm.placeholder;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Test-side expression-list driver and inspection of the production occurrence index. */
public class TestSPMPlaceholderBuilder extends SPMPlaceholderBuilder {
    public List<Expression> parameterizeExpressions(List<Expression> expressions) {
        return parameterizeExpressions(this, expressions);
    }

    public static List<Expression> parameterizeExpressions(SPMPlaceholderBuilder builder,
            List<Expression> expressions) {
        builder.startNewTree();
        List<Expression> result = new ArrayList<>(expressions.size());
        for (Expression expression : expressions) {
            result.add(builder.apply(expression));
        }
        return result;
    }

    public List<PlaceholderSnapshot> getPlaceholderExprs() {
        try {
            Field field = SPMPlaceholderBuilder.class.getDeclaredField("placeholderIndex");
            field.setAccessible(true);
            Map<PlaceholderKey, Expression> index = (Map<PlaceholderKey, Expression>) field.get(this);
            List<PlaceholderSnapshot> result = new ArrayList<>(index.size());
            index.forEach((key, value) -> result.add(new PlaceholderSnapshot(key.expression(), value,
                    key.parent(), key.childIndex(), key.blockId(), key.rootOrdinal(), key.occurrencePath())));
            result.sort(Comparator.comparingLong(snapshot -> markerId(snapshot.placeholderExpr())));
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static long markerId(Expression marker) {
        return marker instanceof SpmConstVar ? ((SpmConstVar) marker).getId()
                : ((SpmConstList) ((InPredicate) marker).getOptions().get(0)).getId();
    }
}
