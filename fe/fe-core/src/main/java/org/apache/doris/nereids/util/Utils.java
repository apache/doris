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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utils for Nereids.
 */
public class Utils {
    /**
     * Quoted string if it contains special character or all characters are digit.
     *
     * @param part string to be quoted
     * @return quoted string
     */
    public static String quoteIfNeeded(String part) {
        // We quote strings except the ones which consist of digits only.
        return part.matches("\\w*[\\w&&[^\\d]]+\\w*")
                ? part : part.replace("`", "``");
    }

    /**
     * Helper function to eliminate unnecessary checked exception caught requirement from the main logic of translator.
     *
     * @param f function which would invoke the logic of
     *        stale code from old optimizer that could throw
     *        a checked exception.
     */
    public static void execWithUncheckedException(FuncWrapper f) {
        try {
            f.exec();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Helper function to eliminate unnecessary checked exception caught requirement from the main logic of translator.
     */
    @SuppressWarnings("unchecked")
    public static <R> R execWithReturnVal(Supplier<R> f) {
        final Object[] ans = new Object[] {null};
        try {
            ans[0] = f.get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return (R) ans[0];
    }

    /**
     * Wrapper to a function without return value.
     */
    public interface FuncWrapper {
        void exec() throws Exception;
    }

    /**
     * Wrapper to a funciton with return value.
     */
    public interface Supplier<R> {
        R get() throws Exception;
    }

    /**
     * Fully qualified identifier name parts, i.e., concat qualifier and name into a list.
     */
    public static List<String> qualifiedNameParts(List<String> qualifier, String name) {
        return new ImmutableList.Builder<String>().addAll(qualifier).add(name).build();
    }

    /**
     * Fully qualified identifier name, concat qualifier and name with `.` as separator.
     */
    public static String qualifiedName(List<String> qualifier, String name) {
        return StringUtils.join(qualifiedNameParts(qualifier, name), ".");
    }

    /**
     * equals for List but ignore order.
     */
    public static <E> boolean equalsIgnoreOrder(List<E> one, List<E> other) {
        if (one.size() != other.size()) {
            return false;
        }
        return new HashSet<>(one).containsAll(other) && new HashSet<>(other).containsAll(one);
    }

    /**
     * Get sql string for plan.
     *
     * @param planName name of plan, like LogicalJoin.
     * @param variables variable needed to add into sqlString.
     * @return the string of PlanNode.
     */
    public static String toSqlString(String planName, Object... variables) {
        Preconditions.checkState(variables.length % 2 == 0);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(planName).append(" ( ");

        if (variables.length == 0) {
            return stringBuilder.append(" )").toString();
        }

        for (int i = 0; i < variables.length - 1; i += 2) {
            stringBuilder.append(variables[i]).append("=").append(variables[i + 1]);
            if (i < variables.length - 2) {
                stringBuilder.append(", ");
            }
        }

        return stringBuilder.append(" )").toString();
    }

    /**
     * See if there are correlated columns in a subquery expression.
     */
    public static boolean containCorrelatedSlot(List<Expression> correlatedSlots, Expression expr) {
        if (correlatedSlots.isEmpty() || expr == null) {
            return false;
        }
        if (expr instanceof SlotReference) {
            return correlatedSlots.contains(expr);
        }
        for (Expression child : expr.children()) {
            if (containCorrelatedSlot(correlatedSlots, child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the correlated columns that belong to the subquery,
     * that is, the correlated columns that can be resolved within the subquery.
     * eg:
     * select * from t1 where t1.a = (select sum(t2.b) from t2 where t1.c = t2.d));
     * correlatedPredicates : t1.c = t2.d
     * correlatedSlots : t1.c
     * return t2.d
     */
    public static List<Expression> getCorrelatedSlots(List<Expression> correlatedPredicates,
            List<Expression> correlatedSlots) {
        List<Expression> slots = new ArrayList<>();
        correlatedPredicates.forEach(predicate -> {
            if (!(predicate instanceof BinaryExpression)) {
                throw new AnalysisException("UnSupported expr type: " + correlatedPredicates);
            }
            BinaryExpression binaryExpression = (BinaryExpression) predicate;
            if (binaryExpression.left().anyMatch(correlatedSlots::contains)) {
                if (binaryExpression.right() instanceof SlotReference) {
                    slots.add(binaryExpression.right());
                }
            } else {
                if (binaryExpression.left() instanceof SlotReference) {
                    slots.add(binaryExpression.left());
                }
            }
        });
        return slots;
    }

    public static Map<Boolean, List<Expression>> splitCorrelatedConjuncts(
            List<Expression> conjuncts, List<Expression> slots) {
        return conjuncts.stream().collect(Collectors.partitioningBy(
                expr -> expr.anyMatch(slots::contains)));
    }

    public static LocalDateTime getLocalDatetimeFromLong(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(dateTime), ZoneId.systemDefault());
    }

    public static <T> void replaceList(List<T> list, T oldItem, T newItem) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == oldItem) {
                list.set(i, newItem);
            }
        }
    }
}
