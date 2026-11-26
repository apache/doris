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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StringDefaultValueLengthTest {
    // CHAR/VARCHAR length is in bytes, "中" is 3 bytes in UTF-8.
    @Test
    void testVarcharDefaultValueLengthInBytes() {
        Assertions.assertDoesNotThrow(
                () -> ColumnDef.validateDefaultValue(ScalarType.createVarcharType(3), "中", null));
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(ScalarType.createVarcharType(2), "中", null));
        Assertions.assertTrue(e.getMessage().contains("Default value is too long"), e.getMessage());
    }

    @Test
    void testCharDefaultValueLengthInBytes() {
        Assertions.assertDoesNotThrow(
                () -> ColumnDef.validateDefaultValue(ScalarType.createCharType(3), "中", null));
        Assertions.assertDoesNotThrow(
                () -> ColumnDef.validateDefaultValue(ScalarType.createCharType(3), "abc", null));
        Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(ScalarType.createCharType(1), "中", null));
        Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(ScalarType.createCharType(3), "abcd", null));
    }
}
