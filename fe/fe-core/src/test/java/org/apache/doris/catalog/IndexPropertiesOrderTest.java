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

package org.apache.doris.catalog;

import org.apache.doris.analysis.IndexDef;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IndexPropertiesOrderTest {

    @Test
    public void testIndexPropertiesConsistentOrder() {
        // Create properties with multiple key-value pairs to test ordering
        Map<String, String> properties = new HashMap<>();
        properties.put("parser", "english");
        properties.put("lower_case", "true");
        properties.put("support_phrase", "true");

        List<String> columns = Arrays.asList("description");

        // Create multiple Index objects with the same properties
        Index index1 = new Index(1L, "test_idx", columns, IndexDef.IndexType.INVERTED,
                                 new HashMap<>(properties), "test comment");
        Index index2 = new Index(2L, "test_idx", columns, IndexDef.IndexType.INVERTED,
                                 new HashMap<>(properties), "test comment");
        Index index3 = new Index(3L, "test_idx", columns, IndexDef.IndexType.INVERTED,
                                 new HashMap<>(properties), "test comment");

        // The properties part should be consistent across all instances
        String props1 = index1.getPropertiesString();
        String props2 = index2.getPropertiesString();
        String props3 = index3.getPropertiesString();

        // Assert that all properties strings are identical
        Assertions.assertEquals(props1, props2, "Properties order should be consistent between index1 and index2");
        Assertions.assertEquals(props2, props3, "Properties order should be consistent between index2 and index3");
        Assertions.assertEquals(props1, props3, "Properties order should be consistent between index1 and index3");

        // Verify the properties are in alphabetical order
        Assertions.assertTrue(props1.contains("lower_case"), "Properties should contain lower_case");
        Assertions.assertTrue(props1.contains("parser"), "Properties should contain parser");
        Assertions.assertTrue(props1.contains("support_phrase"), "Properties should contain support_phrase");

        // Test with different orderings of the same properties
        Map<String, String> properties2 = new LinkedHashMap<>();
        properties2.put("support_phrase", "true");
        properties2.put("parser", "english");
        properties2.put("lower_case", "true");
        Index index4 = new Index(4L, "test_idx", columns, IndexDef.IndexType.INVERTED,
                                 properties2, "test comment");
        String props4 = index4.getPropertiesString();

        // Should still be the same as the others due to TreeMap sorting
        Assertions.assertEquals(props1, props4, "Properties order should be consistent regardless of input order");
    }
}
