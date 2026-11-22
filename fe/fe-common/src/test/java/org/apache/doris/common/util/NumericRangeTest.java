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

package org.apache.doris.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NumericRangeTest {
    @Test
    public void testExactBoundsAndFiniteValues() {
        // Check the original decimal value. A long/double conversion would lose precision.
        for (String value : new String[] {"-0.1", "-1", "NaN", "Infinity", "-Infinity"}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> NumericRange.check("limit", value, "0", ""), value);
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> NumericRange.check("limit", "9223372036854775808", "0", "9223372036854775807"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> NumericRange.check("ratio", "1e309", "0", "1"));
        Assertions.assertDoesNotThrow(() -> NumericRange.check("limit", "0", "0", ""));
        Assertions.assertDoesNotThrow(() -> NumericRange.check("limit", "100", "1", "100"));
        // An unbounded declaration keeps its existing parser and sentinel rules.
        Assertions.assertDoesNotThrow(() -> NumericRange.check("automatic", "-1", "", ""));
    }
}
