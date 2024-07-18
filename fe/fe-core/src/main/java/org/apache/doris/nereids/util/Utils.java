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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     *         stale code from old optimizer that could throw
     *         a checked exception.
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
     * Check whether lhs and rhs are intersecting.
     */
    public static <T> boolean isIntersecting(Set<T> lhs, Collection<T> rhs) {
        for (T rh : rhs) {
            if (lhs.contains(rh)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Wrapper to a function without return value.
     */
    public interface FuncWrapper {
        void exec() throws Exception;
    }

    /**
     * Wrapper to a function with return value.
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
     * get qualified name with Backtick
     */
    public static String qualifiedNameWithBackquote(List<String> qualifiers, String name) {
        List<String> fullName = new ArrayList<>(qualifiers);
        fullName.add(name);
        return qualifiedNameWithBackquote(fullName);
    }

    /**
     * get qualified name with Backtick
     */
    public static String qualifiedNameWithBackquote(List<String> qualifiers) {
        List<String> qualifierWithBackquote = Lists.newArrayListWithCapacity(qualifiers.size());
        for (String qualifier : qualifiers) {
            String escapeQualifier = qualifier.replace("`", "``");
            qualifierWithBackquote.add('`' + escapeQualifier + '`');
        }
        return StringUtils.join(qualifierWithBackquote, ".");
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
            if (!"".equals(toStringOrNull(variables[i + 1]))) {
                if (i != 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append(toStringOrNull(variables[i])).append("=").append(toStringOrNull(variables[i + 1]));
            }
        }

        return stringBuilder.append(" )").toString();
    }

    private static String toStringOrNull(Object obj) {
        return obj == null ? "null" : obj.toString();
    }

    /**
     * Get the unCorrelated exprs that belong to the subquery,
     * that is, the unCorrelated exprs that can be resolved within the subquery.
     * eg:
     * select * from t1 where t1.a = (select sum(t2.b) from t2 where t1.c = abs(t2.d));
     * correlatedPredicates : t1.c = abs(t2.d)
     * unCorrelatedExprs : abs(t2.d)
     * return abs(t2.d)
     */
    public static List<Expression> getUnCorrelatedExprs(List<Expression> correlatedPredicates,
                                                        List<Expression> correlatedSlots) {
        List<Expression> unCorrelatedExprs = new ArrayList<>();
        correlatedPredicates.forEach(predicate -> {
            if (!(predicate instanceof BinaryExpression) && (!(predicate instanceof Not)
                    || !(predicate.child(0) instanceof BinaryExpression))) {
                throw new AnalysisException(
                        "Unsupported correlated subquery with correlated predicate "
                                + predicate.toString());
            }

            BinaryExpression binaryExpression;
            if (predicate instanceof Not) {
                binaryExpression = (BinaryExpression) ((Not) predicate).child();
            } else {
                binaryExpression = (BinaryExpression) predicate;
            }
            Expression left = binaryExpression.left();
            Expression right = binaryExpression.right();
            Set<Slot> leftInputSlots = left.getInputSlots();
            Set<Slot> rightInputSlots = right.getInputSlots();
            boolean correlatedToLeft = !leftInputSlots.isEmpty()
                    && leftInputSlots.stream().allMatch(correlatedSlots::contains)
                    && rightInputSlots.stream().noneMatch(correlatedSlots::contains);
            boolean correlatedToRight = !rightInputSlots.isEmpty()
                    && rightInputSlots.stream().allMatch(correlatedSlots::contains)
                    && leftInputSlots.stream().noneMatch(correlatedSlots::contains);
            if (!correlatedToLeft && !correlatedToRight) {
                throw new AnalysisException(
                        "Unsupported correlated subquery with correlated predicate " + predicate);
            } else if (correlatedToLeft && !rightInputSlots.isEmpty()) {
                unCorrelatedExprs.add(right);
            } else if (correlatedToRight && !leftInputSlots.isEmpty()) {
                unCorrelatedExprs.add(left);
            }
        });
        return unCorrelatedExprs;
    }

    private static List<Expression> collectCorrelatedSlotsFromChildren(
            BinaryExpression binaryExpression, List<Expression> correlatedSlots) {
        List<Expression> slots = new ArrayList<>();
        if (binaryExpression.left().anyMatch(correlatedSlots::contains)) {
            if (binaryExpression.right() instanceof SlotReference) {
                slots.add(binaryExpression.right());
            } else if (binaryExpression.right() instanceof Cast) {
                slots.add(((Cast) binaryExpression.right()).child());
            }
        } else {
            if (binaryExpression.left() instanceof SlotReference) {
                slots.add(binaryExpression.left());
            } else if (binaryExpression.left() instanceof Cast) {
                slots.add(((Cast) binaryExpression.left()).child());
            }
        }
        return slots;
    }

    public static Map<Boolean, List<Expression>> splitCorrelatedConjuncts(
            Set<Expression> conjuncts, List<Expression> slots) {
        return conjuncts.stream().collect(Collectors.partitioningBy(
                expr -> expr.anyMatch(slots::contains)));
    }

    /**
     * Replace one item in a list with another item.
     */
    public static <T> void replaceList(List<T> list, T oldItem, T newItem) {
        boolean result = false;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equals(oldItem)) {
                list.set(i, newItem);
                result = true;
            }
        }
        Preconditions.checkState(result);
    }

    /**
     * Remove item from a list without equals method.
     */
    public static <T> void identityRemove(List<T> list, T item) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == item) {
                list.remove(i);
                i--;
                return;
            }
        }
        Preconditions.checkState(false, "item not found in list");
    }

    /**
     * allCombinations
     */
    public static <T> List<List<T>> allCombinations(List<List<T>> lists) {
        if (lists.size() == 1) {
            List<T> first = lists.get(0);
            if (first.size() == 1) {
                return lists;
            }
            List<List<T>> result = Lists.newArrayListWithCapacity(lists.size());
            for (T item : first) {
                result.add(ImmutableList.of(item));
            }
            return result;
        } else {
            return doAllCombinations(lists);
        }
    }

    private static <T> List<List<T>> doAllCombinations(List<List<T>> lists) {
        int size = lists.size();
        if (size == 0) {
            return ImmutableList.of();
        }
        List<T> first = lists.get(0);
        if (size == 1) {
            return first
                    .stream()
                    .map(ImmutableList::of)
                    .collect(ImmutableList.toImmutableList());
        }
        List<List<T>> rest = lists.subList(1, size);
        List<List<T>> combinationWithoutFirst = allCombinations(rest);
        return first.stream()
                .flatMap(firstValue -> combinationWithoutFirst.stream()
                        .map(restList ->
                                Stream.concat(Stream.of(firstValue), restList.stream())
                                        .collect(ImmutableList.toImmutableList())
                        )
                ).collect(ImmutableList.toImmutableList());
    }

    public static <T> List<T> copyRequiredList(List<T> list) {
        return ImmutableList.copyOf(Objects.requireNonNull(list, "non-null list is required"));
    }

    public static <T> List<T> copyRequiredMutableList(List<T> list) {
        return Lists.newArrayList(Objects.requireNonNull(list, "non-null list is required"));
    }

    /**
     * Normalize the name to lower underscore style, return default name if the name is empty.
     */
    public static String normalizeName(String name, String defaultName) {
        if (StringUtils.isEmpty(name)) {
            return defaultName;
        }
        if (name.contains("$")) {
            name = name.replace("$", "_");
        }
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
    }

    /**
     * Check the content if contains chinese or not, if true when contains chinese or false
     */
    public static boolean containChinese(String text) {
        for (char textChar : text.toCharArray()) {
            if (Character.UnicodeScript.of(textChar) == Character.UnicodeScript.HAN) {
                return true;
            }
        }
        return false;
    }

    public static <I, O> List<O> fastMapList(List<I> list, int additionSize, Function<I, O> transformer) {
        List<O> newList = Lists.newArrayListWithCapacity(list.size() + additionSize);
        for (I input : list) {
            newList.add(transformer.apply(input));
        }
        return newList;
    }

    /**
     * fastToImmutableList
     */
    public static <E> ImmutableList<E> fastToImmutableList(E[] array) {
        switch (array.length) {
            case 0:
                return ImmutableList.of();
            case 1:
                return ImmutableList.of(array[0]);
            default:
                // NOTE: ImmutableList.copyOf(array) has additional clone of the array, so here we
                //       direct generate a ImmutableList
                Builder<E> copyChildren = ImmutableList.builderWithExpectedSize(array.length);
                for (E child : array) {
                    copyChildren.add(child);
                }
                return copyChildren.build();
        }
    }

    /**
     * fastToImmutableList
     */
    public static <E> ImmutableList<E> fastToImmutableList(Collection<? extends E> collection) {
        if (collection instanceof ImmutableList) {
            return (ImmutableList<E>) collection;
        }

        switch (collection.size()) {
            case 0:
                return ImmutableList.of();
            case 1:
                return collection instanceof List
                        ? ImmutableList.of(((List<E>) collection).get(0))
                        : ImmutableList.of(collection.iterator().next());
            default: {
                // NOTE: ImmutableList.copyOf(list) has additional clone of the list, so here we
                //       direct generate a ImmutableList
                Builder<E> copyChildren = ImmutableList.builderWithExpectedSize(collection.size());
                copyChildren.addAll(collection);
                return copyChildren.build();
            }
        }
    }

    /**
     * fastToImmutableSet
     */
    public static <E> ImmutableSet<E> fastToImmutableSet(Collection<? extends E> collection) {
        if (collection instanceof ImmutableSet) {
            return (ImmutableSet<E>) collection;
        }
        switch (collection.size()) {
            case 0:
                return ImmutableSet.of();
            case 1:
                return collection instanceof List
                        ? ImmutableSet.of(((List<E>) collection).get(0))
                        : ImmutableSet.of(collection.iterator().next());
            default:
                // NOTE: ImmutableList.copyOf(array) has additional clone of the array, so here we
                //       direct generate a ImmutableList
                ImmutableSet.Builder<E> copyChildren = ImmutableSet.builderWithExpectedSize(collection.size());
                for (E child : collection) {
                    copyChildren.add(child);
                }
                return copyChildren.build();
        }
    }

    /**
     * reverseImmutableList
     */
    public static <E> ImmutableList<E> reverseImmutableList(List<? extends E> list) {
        Builder<E> reverseList = ImmutableList.builderWithExpectedSize(list.size());
        for (int i = list.size() - 1; i >= 0; i--) {
            reverseList.add(list.get(i));
        }
        return reverseList.build();
    }

    /**
     * filterImmutableList
     */
    public static <E> ImmutableList<E> filterImmutableList(List<? extends E> list, Predicate<E> filter) {
        Builder<E> newList = ImmutableList.builderWithExpectedSize(list.size());
        for (int i = 0; i < list.size(); i++) {
            E item = list.get(i);
            if (filter.test(item)) {
                newList.add(item);
            }
        }
        return newList.build();
    }

    /**
     * concatToSet
     */
    public static <E> Set<E> concatToSet(Collection<? extends E> left, Collection<? extends E> right) {
        ImmutableSet.Builder<E> required = ImmutableSet.builderWithExpectedSize(
                left.size() + right.size()
        );
        required.addAll(left);
        required.addAll(right);
        return required.build();
    }

    /**
     * fastReduce
     */
    public static <M, T extends M> Optional<M> fastReduce(List<T> list, BiFunction<M, T, M> reduceOp) {
        if (list.isEmpty()) {
            return Optional.empty();
        }
        M merge = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            merge = reduceOp.apply(merge, list.get(i));
        }
        return Optional.of(merge);
    }

    /** If the first character of the string is uppercase, replace the first character with lowercase*/
    public static String convertFirstChar(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        char firstChar = input.charAt(0);
        if (Character.isUpperCase(firstChar)) {
            firstChar = Character.toLowerCase(firstChar);
        } else {
            return input;
        }
        return firstChar + input.substring(1);
    }

    /** addLinePrefix */
    public static String addLinePrefix(String str, String prefix) {
        StringBuilder newStr = new StringBuilder((int) (str.length() * 1.2));
        String[] lines = str.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            newStr.append(prefix).append(line);
            if (i + 1 < lines.length) {
                newStr.append("\n");
            }
        }
        return newStr.toString();
    }
}
