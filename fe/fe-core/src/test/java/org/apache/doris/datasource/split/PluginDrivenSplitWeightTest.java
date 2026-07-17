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

package org.apache.doris.datasource.split;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * FIX-A1: {@link PluginDrivenSplit} must thread the connector's proportional split weight
 * ({@link ConnectorScanRange#getSelfSplitWeight()} / {@link ConnectorScanRange#getTargetSplitSize()}) into
 * the {@link FileSplit} scheduling fields so {@code FederationBackendPolicy} distributes splits by size
 * (legacy paimon parity), and must fall back to the uniform {@link SplitWeight#standard()} when the
 * connector provides no weight (the {@code -1} SPI sentinel — no-regression for other connectors).
 *
 * <p>Assertions pin the EXACT {@code getRawValue()} so the RED state (fields unset → standard, rawValue
 * 100) is always distinguishable from the GREEN proportional value (50 / 1). {@code fromProportion} uses
 * {@code ceil(weight * 100)} (SplitWeight:74), and FileSplit clamps the proportion to [0.01, 1.0]
 * (FileSplit:106-112).
 */
public class PluginDrivenSplitWeightTest {

    /** Minimal fake range exposing only the two weight getters (+ the two required methods). */
    private static ConnectorScanRange range(long selfWeight, long targetSize) {
        return new ConnectorScanRange() {
            @Override
            public ConnectorScanRangeType getRangeType() {
                return ConnectorScanRangeType.FILE_SCAN;
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public long getSelfSplitWeight() {
                return selfWeight;
            }

            @Override
            public long getTargetSplitSize() {
                return targetSize;
            }
        };
    }

    @Test
    public void proportionalWeightWhenConnectorProvidesBoth() {
        // W=50 / T=100 -> proportion 0.5 -> fromProportion rawValue = ceil(50) = 50 (NOT standard's 100).
        // WHY: legacy paimon set selfSplitWeight + targetSplitSize so FederationBackendPolicy weighted
        // splits by size; the SPI must reproduce that. MUTATION: the ctor not threading the fields ->
        // getSplitWeight() == standard() (rawValue 100) -> red.
        PluginDrivenSplit split = new PluginDrivenSplit(range(50L, 100L));
        Assertions.assertEquals(50L, split.getSplitWeight().getRawValue(),
                "a weighted range must yield a proportional (non-standard) split weight");
    }

    @Test
    public void proportionalWeightClampsToLowerBound() {
        // W=1 / T=100 -> 0.01 floor clamp -> fromProportion(0.01) rawValue = ceil(1) = 1 (NOT 0, NOT 100).
        PluginDrivenSplit split = new PluginDrivenSplit(range(1L, 100L));
        Assertions.assertEquals(1L, split.getSplitWeight().getRawValue(),
                "a tiny weight must clamp to the 0.01 lower bound (rawValue 1), not collapse to 0");
    }

    @Test
    public void zeroWeightIsValidAndProportional() {
        // W=0 (empty file / 0-row sys table) is a legitimate weight, not "unset": 0/100 -> clamp 0.01 ->
        // rawValue 1. The gate is weight>=0 (NOT >0), so a genuine 0 still produces a clamped proportional
        // weight. MUTATION: a weight>0 gate would drop 0-weight splits back to standard() (rawValue 100).
        PluginDrivenSplit split = new PluginDrivenSplit(range(0L, 100L));
        Assertions.assertEquals(1L, split.getSplitWeight().getRawValue(),
                "weight 0 is valid (>=0 gate) and clamps to 0.01, matching legacy");
    }

    @Test
    public void standardWeightWhenConnectorProvidesNoWeight() {
        // The -1 SPI sentinel (a connector with no weight model: jdbc/es/trino/maxcompute) -> both FileSplit
        // fields stay null -> standard() (rawValue 100). The no-regression guarantee.
        PluginDrivenSplit split = new PluginDrivenSplit(range(-1L, -1L));
        Assertions.assertEquals(100L, split.getSplitWeight().getRawValue(),
                "no connector weight (-1 sentinel) must keep the uniform standard() weight");
        Assertions.assertSame(SplitWeight.standard(), split.getSplitWeight());
    }

    @Test
    public void standardWeightWhenOnlyOneFieldProvided() {
        // Proportional weight needs BOTH a weight and a POSITIVE denominator (target>0 guards div-by-zero).
        Assertions.assertEquals(100L,
                new PluginDrivenSplit(range(50L, -1L)).getSplitWeight().getRawValue(),
                "a weight with no target denominator must stay standard()");
        Assertions.assertEquals(100L,
                new PluginDrivenSplit(range(-1L, 100L)).getSplitWeight().getRawValue(),
                "a target with no weight must stay standard()");
    }
}
