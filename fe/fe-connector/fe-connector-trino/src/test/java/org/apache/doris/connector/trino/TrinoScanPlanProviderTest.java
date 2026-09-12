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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/** Tests correctness gates shared by Trino metadata and scan planning. */
public class TrinoScanPlanProviderTest {

    @Test
    public void testFilterPreventsSourceLimitPushdown() {
        Optional<ConnectorExpression> filter = Optional.of(ConnectorLiteral.ofBoolean(true));

        Assertions.assertFalse(TrinoScanPlanProvider.shouldApplyLimit(10L, filter));
    }

    @Test
    public void testUnfilteredScanCanPushLimit() {
        Assertions.assertTrue(TrinoScanPlanProvider.shouldApplyLimit(10L, Optional.empty()));
        Assertions.assertFalse(TrinoScanPlanProvider.shouldApplyLimit(-1L, Optional.empty()));
    }

    @Test
    public void testTrinoRejectsCastPredicatePushdown() {
        TrinoConnectorDorisMetadata metadata = new TrinoConnectorDorisMetadata(null, null, null);

        Assertions.assertFalse(metadata.supportsCastPredicatePushdown(null));
    }

    @Test
    public void testMetadataDefersFilteringUntilAfterCastGate() {
        TrinoConnectorDorisMetadata metadata = new TrinoConnectorDorisMetadata(null, null, null);
        ConnectorFilterConstraint constraint = new ConnectorFilterConstraint(ConnectorLiteral.ofBoolean(true));

        Assertions.assertFalse(metadata.applyFilter(null, null, constraint).isPresent());
    }
}
