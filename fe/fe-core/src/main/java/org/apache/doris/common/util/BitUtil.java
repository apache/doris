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

public class BitUtil {

    // Returns the log2 of 'val'. 'val' must be > 0.
    public static int log2Ceiling(long val) {
        // Formula is based on the Long.numberOfLeadingZeros() javadoc comment.
        return 64 - Long.numberOfLeadingZeros(val - 1);
    }

    // Round up 'val' to the nearest power of two. 'val' must be > 0.
    public static long roundUpToPowerOf2(long val) {
        return 1L << log2Ceiling(val);
    }

    // Round up 'val' to the nearest multiple of a power-of-two 'factor'.
    // 'val' must be > 0.
    public static long roundUpToPowerOf2Factor(long val, long factor) {
        return (val + (factor - 1)) & ~(factor - 1);
    }
}

