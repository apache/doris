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

import org.apache.doris.nereids.trees.expressions.LessThanEqual;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** ParentTypeIdMapping */
public class ParentTypeIdMapping {

    private final AtomicInteger idGenerator = new AtomicInteger();
    private final Map<Class<?>, Integer> classId = new ConcurrentHashMap<>(8192);

    /** getId */
    public int getId(Class<?> clazz) {
        Integer id = classId.get(clazz);
        if (id != null) {
            return id;
        }
        return ensureClassHasId(clazz);
    }

    private int ensureClassHasId(Class<?> clazz) {
        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null) {
            ensureClassHasId(superClass);
        }

        for (Class<?> interfaceClass : clazz.getInterfaces()) {
            ensureClassHasId(interfaceClass);
        }

        return classId.computeIfAbsent(clazz, c -> idGenerator.incrementAndGet());
    }

    public static void main(String[] args) {
        ParentTypeIdMapping mapping = new ParentTypeIdMapping();
        int id = mapping.getId(LessThanEqual.class);
        System.out.println(id);
    }
}
