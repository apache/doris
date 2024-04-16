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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.pattern.TypeMappings.TypeMapping;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/** ExpressionPatternMappings */
public abstract class TypeMappings<K, T extends TypeMapping<K>> {
    protected final ListMultimap<Class<? extends K>, T> singleMappings;
    protected final List<T> multiMappings;

    /** ExpressionPatternMappings */
    public TypeMappings(List<T> typeMappings) {
        this.singleMappings = ArrayListMultimap.create();
        this.multiMappings = Lists.newArrayList();

        for (T mapping : typeMappings) {
            Set<Class<? extends K>> childrenClasses = getChildrenClasses(mapping.getType());
            if (childrenClasses == null || childrenClasses.isEmpty()) {
                // add some expressions which no child class
                // e.g. LessThanEqual
                addSimpleMapping(mapping);
            } else if (childrenClasses.size() <= 100) {
                // add some expressions which have children classes
                // e.g. ComparisonPredicate will be expanded to
                //      ruleMappings.put(LessThanEqual.class, rule);
                //      ruleMappings.put(LessThan.class, rule);
                //      ruleMappings.put(GreaterThan.class, rule);
                //      ruleMappings.put(GreaterThanEquals.class, rule);
                //      ...
                addThisAndChildrenMapping(mapping, childrenClasses);
            } else {
                // some expressions have lots of children classes, e.g. Expression, ExpressionTrait, BinaryExpression,
                // we will not expand this types to child class, but also add this rules to other type matching.
                // for example, if we have three rules to matches this types: LessThanEqual, Abs and Expression,
                // then the ruleMappings would be:
                //   {
                //      LessThanEqual.class: [rule_of_LessThanEqual, rule_of_Expression],
                //      Abs.class: [rule_of_Abs, rule_of_Expression]
                //   }
                //
                // and the multiMatchRules would be: [rule_of_Expression]
                //
                // if we matches `a <= 1`, there have two rules would be applied because
                // ruleMappings.get(LessThanEqual.class) return two rules;
                // if we matches `a = 1`, ruleMappings.get(EqualTo.class) will return empty rules, so we use
                // all the rules in multiMatchRules to matches and apply, the rule_of_Expression will be applied.
                addMultiMapping(mapping);
            }
        }
    }

    public @Nullable List<T> get(Class<? extends K> clazz) {
        return singleMappings.get(clazz);
    }

    private void addSimpleMapping(T typeMapping) {
        Class<? extends K> clazz = typeMapping.getType();
        int modifiers = clazz.getModifiers();
        if (!Modifier.isAbstract(modifiers)) {
            addSingleMapping(clazz, typeMapping);
        }
    }

    private void addThisAndChildrenMapping(
            T typeMapping, Set<Class<? extends K>> childrenClasses) {
        Class<? extends K> clazz = typeMapping.getType();
        if (!Modifier.isAbstract(clazz.getModifiers())) {
            addSingleMapping(clazz, typeMapping);
        }

        for (Class<? extends K> childrenClass : childrenClasses) {
            if (!Modifier.isAbstract(childrenClass.getModifiers())) {
                addSingleMapping(childrenClass, typeMapping);
            }
        }
    }

    private void addMultiMapping(T multiMapping) {
        multiMappings.add(multiMapping);

        Set<Class<? extends K>> existSingleMappingTypes = Utils.fastToImmutableSet(singleMappings.keySet());
        for (Class<? extends K> existSingleType : existSingleMappingTypes) {
            Class<? extends K> type = multiMapping.getType();
            if (type.isAssignableFrom(existSingleType)) {
                singleMappings.put(existSingleType, multiMapping);
            }
        }
    }

    private void addSingleMapping(Class<? extends K> clazz, T singleMapping) {
        if (!singleMappings.containsKey(clazz) && !multiMappings.isEmpty()) {
            for (T multiMapping : multiMappings) {
                if (multiMapping.getType().isAssignableFrom(clazz)) {
                    singleMappings.put(clazz, multiMapping);
                }
            }
        }
        singleMappings.put(clazz, singleMapping);
    }

    protected abstract Set<Class<? extends K>> getChildrenClasses(Class<? extends K> clazz);

    /** TypeMapping */
    public interface TypeMapping<K> {
        Class<? extends K> getType();
    }
}
