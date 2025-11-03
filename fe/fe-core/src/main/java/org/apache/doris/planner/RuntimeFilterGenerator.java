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

package org.apache.doris.planner;

import org.apache.doris.common.util.BitUtil;
import org.apache.doris.qe.SessionVariable;

/**
 * Class used for generating and assigning runtime filters to a query plan using
 * runtime filter propagation. Runtime filter propagation is an optimization technique
 * used to filter scanned tuples or scan ranges based on information collected at
 * runtime. A runtime filter is constructed during the build phase of a join node, and is
 * applied at, potentially, multiple scan nodes on the probe side of that join node.
 * Runtime filters are generated from equal-join predicates but they do not replace the
 * original predicates.
 *
 * MinMax filters are of a fixed size (except for those used for string type) and
 * therefore only sizes for bloom filters need to be calculated. These calculations are
 * based on the NDV estimates of the associated table columns, the min buffer size that
 * can be allocated by the bufferpool, and the query options. Moreover, it is also bound
 * by the MIN/MAX_BLOOM_FILTER_SIZE limits which are enforced on the query options before
 * this phase of planning.
 *
 * Example: select * from T1, T2 where T1.a = T2.b and T2.c = '1';
 * Assuming that T1 is a fact table and T2 is a significantly smaller dimension table, a
 * runtime filter is constructed at the join node between tables T1 and T2 while building
 * the hash table on the values of T2.b (rhs of the join condition) from the tuples of T2
 * that satisfy predicate T2.c = '1'. The runtime filter is subsequently sent to the
 * scan node of table T1 and is applied on the values of T1.a (lhs of the join condition)
 * to prune tuples of T2 that cannot be part of the join result.
 */
public final class RuntimeFilterGenerator {
    /**
     * Internal class that encapsulates the max, min and default sizes used for creating
     * bloom filter objects.
     */
    public static class FilterSizeLimits {
        // Maximum filter size, in bytes, rounded up to a power of two.
        public final long maxVal;

        // Minimum filter size, in bytes, rounded up to a power of two.
        public final long minVal;

        // Pre-computed default filter size, in bytes, rounded up to a power of two.
        public final long defaultVal;

        public FilterSizeLimits(SessionVariable sessionVariable) {
            // Round up all limits to a power of two
            long maxLimit = sessionVariable.getRuntimeBloomFilterMaxSize();
            maxVal = BitUtil.roundUpToPowerOf2(maxLimit);

            long minLimit = sessionVariable.getRuntimeBloomFilterMinSize();
            // Make sure minVal <= defaultVal <= maxVal
            minVal = BitUtil.roundUpToPowerOf2(Math.min(minLimit, maxVal));

            long defaultValue = sessionVariable.getRuntimeBloomFilterSize();
            defaultValue = Math.max(defaultValue, minVal);
            defaultVal = BitUtil.roundUpToPowerOf2(Math.min(defaultValue, maxVal));
        }
    }
}
