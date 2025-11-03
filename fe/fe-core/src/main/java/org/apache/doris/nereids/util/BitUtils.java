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

package org.apache.doris.nereids.util;

import java.util.List;

/**
 * Bit Util tools.
 */
public class BitUtils {
    /**
     * parse the bits array of big ending to long value.
     */
    public static long bigEndianBitsToLong(List<Boolean> bits) {
        long value = 0;
        for (int i = 0; i < bits.size(); i++) {
            long bit = bits.get(i) ? 1 : 0;
            // e.g. the 1 in [1, 0, 0] should shift left 2 length and become to 4.
            int leftShiftLength = bits.size() - 1 - i;
            value |= (bit << leftShiftLength);
        }
        return value;
    }
}
