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

package org.apache.doris.connector.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Covers the additive {@code isAutoInc} (P2-8 FIX-AUTOINC-REJECT) and {@code isAggregated}
 * (G5 FIX-AGG-COLUMN-REJECT) fields added to {@link ConnectorColumn}.
 *
 * <p><b>WHY this matters:</b> each such flag is now a semantic discriminator that the
 * connector validation rejects on. equals/hashCode must include it (else a set/map deduping
 * {@code ConnectorColumn}s could collapse an auto-inc column onto a plain one, silently dropping
 * the flag), and the legacy arities (5/6-arg) must keep {@code isAutoInc=false} so the other six
 * connectors and all read-path producers are zero behavior change.</p>
 */
public class ConnectorColumnTest {

    @Test
    public void equalsAndHashCodeDistinguishAutoInc() {
        ConnectorColumn plain = new ConnectorColumn(
                "id", ConnectorType.of("INT"), "", false, null, false, false);
        ConnectorColumn autoInc = new ConnectorColumn(
                "id", ConnectorType.of("INT"), "", false, null, false, true);

        // WHY (Rule 9): two columns differing ONLY by auto-inc are genuinely different; if
        // equals/hashCode ignored the field, dedup could re-drop the flag downstream.
        // MUTATION: removing `&& isAutoInc == that.isAutoInc` from equals makes this red.
        Assertions.assertNotEquals(plain, autoInc,
                "columns differing only by isAutoInc must not be equal");
        Assertions.assertNotEquals(plain.hashCode(), autoInc.hashCode(),
                "hashCode must reflect isAutoInc");
    }

    @Test
    public void defaultCtorsLeaveAutoIncFalse() {
        // WHY: locks the additive-default contract -- the 5-arg and 6-arg ctors (used by the other
        // six connectors and read-path producers) must keep isAutoInc=false, i.e. zero behavior
        // change. MUTATION: changing a delegation default to true makes this red.
        ConnectorColumn fiveArg = new ConnectorColumn(
                "c", ConnectorType.of("INT"), "", true, null);
        ConnectorColumn sixArg = new ConnectorColumn(
                "c", ConnectorType.of("INT"), "", true, null, true);

        Assertions.assertFalse(fiveArg.isAutoInc(), "5-arg ctor must default isAutoInc=false");
        Assertions.assertFalse(sixArg.isAutoInc(), "6-arg ctor must default isAutoInc=false");
        Assertions.assertTrue(sixArg.isKey(), "6-arg ctor must still honor isKey=true");
    }

    @Test
    public void equalsAndHashCodeDistinguishAggregated() {
        ConnectorColumn plain = new ConnectorColumn(
                "c", ConnectorType.of("INT"), "", false, null, false, false, false);
        ConnectorColumn aggregated = new ConnectorColumn(
                "c", ConnectorType.of("INT"), "", false, null, false, false, true);

        // WHY (Rule 9): two columns differing ONLY by isAggregated are genuinely different; if
        // equals/hashCode ignored the field, dedup could re-drop the aggregate flag downstream.
        // MUTATION: removing `&& isAggregated == that.isAggregated` from equals makes this red.
        Assertions.assertNotEquals(plain, aggregated,
                "columns differing only by isAggregated must not be equal");
        Assertions.assertNotEquals(plain.hashCode(), aggregated.hashCode(),
                "hashCode must reflect isAggregated");
    }

    @Test
    public void defaultCtorsLeaveAggregatedFalse() {
        // WHY: locks the additive-default contract -- the 5/6/7-arg ctors (used by the other six
        // connectors and read-path producers) must keep isAggregated=false, i.e. zero behavior
        // change. MUTATION: changing the 7-arg delegation default to true makes this red.
        ConnectorColumn fiveArg = new ConnectorColumn(
                "c", ConnectorType.of("INT"), "", true, null);
        ConnectorColumn sixArg = new ConnectorColumn(
                "c", ConnectorType.of("INT"), "", true, null, true);
        ConnectorColumn sevenArg = new ConnectorColumn(
                "c", ConnectorType.of("INT"), "", true, null, false, true);

        Assertions.assertFalse(fiveArg.isAggregated(), "5-arg ctor must default isAggregated=false");
        Assertions.assertFalse(sixArg.isAggregated(), "6-arg ctor must default isAggregated=false");
        Assertions.assertFalse(sevenArg.isAggregated(), "7-arg ctor must default isAggregated=false");
        Assertions.assertTrue(sevenArg.isAutoInc(), "7-arg ctor must still honor isAutoInc=true");
    }
}
