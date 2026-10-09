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

public class InvertedIndexKuromojiValidationTest {

    @Test
    public void acceptsKuromojiParserWithSearchMode() throws AnalysisException {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "kuromoji");
        props.put("parser_mode", "search");
        InvertedIndexUtil.checkInvertedIndexParser(
                "content", PrimitiveType.STRING, props, TInvertedIndexFileStorageFormat.V2);
    }

    @Test
    public void rejectsInvalidKuromojiMode() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "kuromoji");
        props.put("parser_mode", "bogus");
        Assertions.assertThrows(AnalysisException.class, () ->
                InvertedIndexUtil.checkInvertedIndexParser(
                        "content", PrimitiveType.STRING, props, TInvertedIndexFileStorageFormat.V2));
    }
}
