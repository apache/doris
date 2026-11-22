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

import java.math.BigDecimal;

/** Checks explicit numeric parameter bounds before assignment. */
public final class NumericRange {
    private NumericRange() {
    }

    public static void check(String name, String value, String minimum, String maximum) {
        if (minimum.isEmpty() && maximum.isEmpty()) {
            return;
        }
        // Keep the input precision. For example, -0.1 must not become the valid integer 0.
        BigDecimal number;
        try {
            number = new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a finite number, but was " + value, e);
        }
        if (!minimum.isEmpty() && number.compareTo(new BigDecimal(minimum)) < 0) {
            throw new IllegalArgumentException(name + " must be >= " + minimum + ", but was " + value);
        }
        if (!maximum.isEmpty() && number.compareTo(new BigDecimal(maximum)) > 0) {
            throw new IllegalArgumentException(name + " must be <= " + maximum + ", but was " + value);
        }
    }
}
