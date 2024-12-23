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
    @Test
    public void testCheckInvertedIndexProperties() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "true");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat = TInvertedIndexFileStorageFormat.V3;
        InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, invertedIndexFileStorageFormat);

        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "invalid_value");
        try {
            InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, invertedIndexFileStorageFormat);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertEquals(
                    "errCode = 2, detailMessage = Invalid inverted index 'dict_compression' value: invalid_value, "
                            + "dict_compression must be true or false",
                    e.getMessage());
        }
    }

    @Test
    public void testDictCompressionValidTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "true");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V3;

        InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
    }

    @Test
    public void testDictCompressionValidFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "false");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V3;

        InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
    }

    @Test
    public void testDictCompressionNonStringType() {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "true");

        PrimitiveType colType = PrimitiveType.INT;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V3;

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, () -> {
            InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
        });

        Assertions.assertEquals(
                "errCode = 2, detailMessage = dict_compression can only be set for StringType columns. type: INT",
                thrown.getMessage()
        );
    }

    @Test
    public void testDictCompressionInvalidStorageFormat() {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "true");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V2;

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, () -> {
            InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
        });

        Assertions.assertEquals(
                "errCode = 2, detailMessage = dict_compression can only be set when storage format is V3",
                thrown.getMessage()
        );
    }

    @Test
    public void testDictCompressionInvalidValue() {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "invalid_value");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V3;

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, () -> {
            InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
        });

        Assertions.assertEquals(
                "errCode = 2, detailMessage = Invalid inverted index 'dict_compression' value: invalid_value, "
                        + "dict_compression must be true or false",
                thrown.getMessage()
        );
    }

    @Test
    public void testDictCompressionCaseSensitivity() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "True");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V3;

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, () -> {
            InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
        });

        Assertions.assertEquals(
                "errCode = 2, detailMessage = Invalid inverted index 'dict_compression' value: True, "
                        + "dict_compression must be true or false",
                thrown.getMessage()
        );
    }

    @Test
    public void testDictCompressionNullValue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V3;

        InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
    }

    @Test
    public void testDictCompressionWhenStorageFormatIsNull() {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "true");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = null;

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, () -> {
            InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
        });

        Assertions.assertEquals(
                "errCode = 2, detailMessage = dict_compression can only be set when storage format is V3",
                thrown.getMessage()
        );
    }

    @Test
    public void testDictCompressionWithNonV3AndValidValue() {
        Map<String, String> properties = new HashMap<>();
        properties.put(InvertedIndexUtil.INVERTED_INDEX_DICT_COMPRESSION_KEY, "false");

        PrimitiveType colType = PrimitiveType.STRING;
        TInvertedIndexFileStorageFormat storageFormat = TInvertedIndexFileStorageFormat.V2;

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, () -> {
            InvertedIndexUtil.checkInvertedIndexProperties(properties, colType, storageFormat);
        });

        Assertions.assertEquals(
                "errCode = 2, detailMessage = dict_compression can only be set when storage format is V3",
                thrown.getMessage()
        );
    }
}
