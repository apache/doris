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

import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class InvertedIndexUtilTest {
    @Test
    public void testCheckInvertedIndexProperties() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "true");

        InvertedIndexUtil.checkInvertedIndexProperties(properties);

        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "invalid_value");
        try {
            InvertedIndexUtil.checkInvertedIndexProperties(properties);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertEquals(
                    "errCode = 2, detailMessage = Invalid inverted index 'dict_compression' value: invalid_value, "
                            + "dict_compression must be true or false",
                    e.getMessage());
        }
    }
}
