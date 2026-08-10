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

package org.apache.doris.connector.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReflectiveObjectSizeEstimatorTest {

    @Test
    public void usesActualRuntimeCollectionSize() {
        Holder small = new Holder(strings(1));
        Holder large = new Holder(strings(100));

        long smallBytes = ReflectiveObjectSizeEstimator.estimate(small);
        long largeBytes = ReflectiveObjectSizeEstimator.estimate(large);

        Assertions.assertTrue(largeBytes > smallBytes);
    }

    @Test
    public void usesActualRuntimeMapSizeWithCachedClassPlan() {
        Map<String, String> values = new HashMap<>();
        values.put("key-0", "value-0");
        MapHolder holder = new MapHolder(values);
        long smallBytes = ReflectiveObjectSizeEstimator.estimate(holder);

        for (int i = 1; i < 100; i++) {
            values.put("key-" + i, "value-" + i);
        }

        Assertions.assertTrue(ReflectiveObjectSizeEstimator.estimate(holder) > smallBytes);
    }

    @Test
    public void countsSharedReferenceOnceAndStopsCycles() {
        String shared = new String("shared-value");
        SharedHolder holder = new SharedHolder(shared, shared);
        long expected = JvmSizeUtils.instanceSize(SharedHolder.class) + JvmSizeUtils.stringSize(shared);

        Assertions.assertEquals(expected, ReflectiveObjectSizeEstimator.estimate(holder));

        Cycle cycle = new Cycle();
        cycle.next = cycle;
        Assertions.assertEquals(
                JvmSizeUtils.instanceSize(Cycle.class),
                ReflectiveObjectSizeEstimator.estimate(cycle));
    }

    @Test
    public void usesExactPrimitiveArrayLayout() {
        int[] values = new int[37];
        Assertions.assertEquals(
                JvmSizeUtils.intArraySize(values.length),
                ReflectiveObjectSizeEstimator.estimate(values));
    }

    @Test
    public void validatesBounds() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ReflectiveObjectSizeEstimator.estimate(new Object(), 0, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ReflectiveObjectSizeEstimator.estimate(new Object(), 1, -1));
    }

    private static List<String> strings(int size) {
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(new String("value-" + i));
        }
        return values;
    }

    private static final class Holder {
        private final List<String> values;

        private Holder(List<String> values) {
            this.values = values;
        }
    }

    private static final class SharedHolder {
        private final String left;
        private final String right;

        private SharedHolder(String left, String right) {
            this.left = left;
            this.right = right;
        }
    }

    private static final class MapHolder {
        private final Map<String, String> values;

        private MapHolder(Map<String, String> values) {
            this.values = values;
        }
    }

    private static final class Cycle {
        private Cycle next;
    }
}
