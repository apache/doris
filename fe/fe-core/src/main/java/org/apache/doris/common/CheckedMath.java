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

package org.apache.doris.common;

import com.google.common.math.LongMath;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CheckedMath {

    private final static Logger LOG = LogManager.getLogger(CheckedMath.class);

    /**
     * Computes and returns the multiply of two longs. If an overflow occurs,
     * the maximum Long value is returned (Long.MAX_VALUE).
     */
    public static long checkedMultiply(long a, long b) {
        try {
            return LongMath.checkedMultiply(a, b);
        } catch (ArithmeticException e) {
            LOG.warn("overflow when multiplying longs: " + a + ", " + b);
            return Long.MAX_VALUE;
        }
    }

    /**
     * Computes and returns the sum of two longs. If an overflow occurs,
     * the maximum Long value is returned (Long.MAX_VALUE).
     */
    public static long checkedAdd(long a, long b) {
        try {
            return LongMath.checkedAdd(a, b);
        } catch (ArithmeticException e) {
            LOG.warn("overflow when adding longs: " + a + ", " + b);
            return Long.MAX_VALUE;
        }
    }
}
