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

package org.apache.doris.foundation.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BitUtilTest {

    @Test
    public void testLog2Ceiling() {
        Assertions.assertEquals(0, BitUtil.log2Ceiling(1));
        Assertions.assertEquals(1, BitUtil.log2Ceiling(2));
        Assertions.assertEquals(2, BitUtil.log2Ceiling(3));
        Assertions.assertEquals(3, BitUtil.log2Ceiling(8));
    }

    @Test
    public void testRoundUpToPowerOf2() {
        Assertions.assertEquals(1, BitUtil.roundUpToPowerOf2(1));
        Assertions.assertEquals(4, BitUtil.roundUpToPowerOf2(3));
        Assertions.assertEquals(8, BitUtil.roundUpToPowerOf2(8));
        Assertions.assertEquals(16, BitUtil.roundUpToPowerOf2(9));
    }

    @Test
    public void testRoundUpToPowerOf2Factor() {
        Assertions.assertEquals(16, BitUtil.roundUpToPowerOf2Factor(13, 8));
        Assertions.assertEquals(16, BitUtil.roundUpToPowerOf2Factor(16, 8));
        Assertions.assertEquals(24, BitUtil.roundUpToPowerOf2Factor(17, 8));
    }
}
