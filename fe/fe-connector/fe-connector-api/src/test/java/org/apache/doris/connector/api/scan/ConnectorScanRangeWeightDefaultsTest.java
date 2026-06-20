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

package org.apache.doris.connector.api.scan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * FIX-A1: a {@link ConnectorScanRange} that does not override the split-weight getters must inherit the
 * {@code -1} "not provided" sentinel, so the engine ({@code PluginDrivenSplit}) leaves the FileSplit
 * scheduling fields null and keeps {@code SplitWeight.standard()} (the no-regression guarantee for
 * connectors with no size-based weight model: jdbc / es / trino / maxcompute).
 */
public class ConnectorScanRangeWeightDefaultsTest {

    @Test
    public void defaultWeightGettersReturnSentinel() {
        ConnectorScanRange range = new ConnectorScanRange() {
            @Override
            public ConnectorScanRangeType getRangeType() {
                return ConnectorScanRangeType.FILE_SCAN;
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }
        };

        // MUTATION: a 0 default would pass PluginDrivenSplit's weight>=0 gate and (with a target) flip
        // these connectors to proportional weighting -> a behavior change for every non-weighting connector.
        Assertions.assertEquals(-1L, range.getSelfSplitWeight(),
                "getSelfSplitWeight() default must be the -1 sentinel, not 0");
        Assertions.assertEquals(-1L, range.getTargetSplitSize(),
                "getTargetSplitSize() default must be the -1 sentinel, not 0");
    }
}
