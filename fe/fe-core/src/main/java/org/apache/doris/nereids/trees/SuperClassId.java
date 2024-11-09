package org.apache.doris.nereids.trees;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// NOTE: static method let jvm do more aggressive inline, so we not make the instance
public class SuperClassId {
    private static final AtomicInteger idGenerator = new AtomicInteger();
    private static final Map<Class<?>, Integer> classIdCache = new ConcurrentHashMap<>(1000);
    private static final Map<Class<?>, BitSet> superClassIdsCache = new ConcurrentHashMap<>(1000);

    private SuperClassId() {}

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
