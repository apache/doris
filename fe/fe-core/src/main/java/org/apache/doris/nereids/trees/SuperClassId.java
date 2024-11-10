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

package org.apache.doris.nereids.trees;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** SuperClassId */
// NOTE: static method let jvm do more aggressive inline, so we not make the instance
public class SuperClassId {
    private static final AtomicInteger idGenerator = new AtomicInteger();
    private static final Map<Class<?>, Integer> classIdCache = new ConcurrentHashMap<>(1000);
    private static final Map<Class<?>, BitSet> superClassIdsCache = new ConcurrentHashMap<>(1000);

    private SuperClassId() {}

    /** getSuperClassIds */
    public static BitSet getSuperClassIds(Class<?> clazz) {
        // get is more efficiency than computeIfAbsent, is lots of cases, we not need to put into the map
        BitSet ids = superClassIdsCache.get(clazz);
        if (ids != null) {
            return ids;
        }
        return superClassIdsCache.computeIfAbsent(clazz, c -> {
            BitSet bitSet = new BitSet();
            fillSuperClassIds(bitSet, clazz, true);
            return bitSet;
        });
    }

    /** getClassId */
    public static int getClassId(Class<?> clazz) {
        // get is more efficiency than computeIfAbsent, is lots of cases, we not need to put into the map
        Integer id = classIdCache.get(clazz);
        if (id != null) {
            return id;
        }
        return classIdCache.computeIfAbsent(clazz, c -> idGenerator.incrementAndGet());
    }

    private static void fillSuperClassIds(BitSet superClassIds, Class<?> clazz, boolean isTop) {
        BitSet cache = isTop ? null : superClassIdsCache.get(clazz);
        if (cache != null) {
            superClassIds.or(cache);
            return;
        }

        Class<?> superclass = clazz.getSuperclass();
        if (superclass != null) {
            fillSuperClassIds(superClassIds, superclass, false);
        }

        for (Class<?> trait : clazz.getInterfaces()) {
            fillSuperClassIds(superClassIds, trait, false);
        }

        superClassIds.set(getClassId(clazz));
    }
}
