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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class InvertedIndexUtilTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static void check(Map<String, String> properties) throws AnalysisException {
        InvertedIndexUtil.checkInvertedIndexParser(
                "idx", PrimitiveType.STRING, properties, TInvertedIndexFileStorageFormat.V2);
    }

    // The BE writer gates building the "tbf" sub-file on this index property; without it on the FE
    // allow-list, CREATE INDEX ... PROPERTIES("token_bloom_filter"="true") would be rejected and the
    // feature would be unreachable from SQL. These tests pin the property to the allow-list + the
    // boolean validation.
    @Test
    public void testTokenBloomFilterAccepted() throws AnalysisException {
        check(props("parser", "english", "token_bloom_filter", "true"));
        check(props("parser", "english", "token_bloom_filter", "false"));
    }

    @Test
    public void testTokenBloomFilterInvalidValueRejected() {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> check(props("parser", "english", "token_bloom_filter", "maybe")));
        Assertions.assertTrue(e.getMessage().contains("token_bloom_filter must be true or false"),
                e.getMessage());
    }

    @Test
    public void testUnknownPropertyStillRejected() {
        // Sanity: the allow-list is still enforced; only the intended key was added.
        Assertions.assertThrows(AnalysisException.class,
                () -> check(props("parser", "english", "not_a_real_key", "x")));
    }
}
