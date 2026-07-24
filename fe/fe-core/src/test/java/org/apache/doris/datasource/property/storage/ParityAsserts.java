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

package org.apache.doris.datasource.property.storage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Assertion helpers for the Phase A5 parity (characterization) tests that freeze
 * the exact runtime outputs of the fe-core StorageProperties family before the
 * fe-filesystem SPI migration. Every expected value in these tests is an oracle
 * for the future StorageAdapter facade.
 */
final class ParityAsserts {

    private ParityAsserts() {
    }

    /** Asserts the actual map contains exactly the expected entries: no more, no less. */
    static void assertExactMap(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    /** Asserts every expected (key, value) pair is present in the Configuration. */
    static void assertConfContains(Configuration conf, Map<String, String> expectedSubset) {
        expectedSubset.forEach((k, v) -> Assertions.assertEquals(v, conf.get(k), "config key: " + k));
    }

    /** Asserts none of the keys is present in the Configuration. */
    static void assertConfLacks(Configuration conf, String... keys) {
        for (String k : keys) {
            Assertions.assertNull(conf.get(k), "config key should be absent: " + k);
        }
    }

    /** Builds a Map from alternating key/value varargs. */
    static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }
}
