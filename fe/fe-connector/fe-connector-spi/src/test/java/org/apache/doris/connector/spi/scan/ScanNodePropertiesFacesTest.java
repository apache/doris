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

package org.apache.doris.connector.spi.scan;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Guards the relation between the two scan-node-property return faces of {@link ConnectorScanPlanProvider},
 * which the interface javadoc now states as a rule: the engine calls ONLY
 * {@code getScanNodePropertiesResult}, whose default implementation delegates to the {@code Map} face
 * without conjunct tracking. A connector overrides one face or the other, never both.
 *
 * <p><b>Why this matters:</b> the two behaviours the javadoc promises are both silent when wrong. A
 * connector that overrides only the {@code Map} face must still reach the engine (otherwise its scan
 * properties vanish and, for instance, its storage credentials never reach BE); and the delegation must
 * NOT claim conjunct tracking, because "tracking with an empty not-pushed set" means "every conjunct was
 * pushed exactly, prune them all" — asserting that for a connector which never reported anything would
 * drop filters and return extra rows.</p>
 */
public class ScanNodePropertiesFacesTest {

    private static final ConnectorTableHandle HANDLE = new ConnectorTableHandle() {
    };

    /** Overrides only the Map face — the shape 5 of the shipped connectors rely on. */
    private static final class MapFaceOnlyProvider implements ConnectorScanPlanProvider {
        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
            return Collections.emptyList();
        }

        @Override
        public Map<String, String> getScanNodeProperties(ConnectorSession session,
                ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
                Optional<ConnectorExpression> filter) {
            return Collections.singletonMap(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "parquet");
        }
    }

    /** Overrides only the result face, reporting fine-grained pushdown — the es shape. */
    private static final class TrackingFaceProvider implements ConnectorScanPlanProvider {
        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
            return Collections.emptyList();
        }

        @Override
        public ScanNodePropertiesResult getScanNodePropertiesResult(ConnectorSession session,
                ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
                Optional<ConnectorExpression> filter) {
            return ScanNodePropertiesResult.withPushdownTracking(
                    Collections.singletonMap(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "es_http"),
                    Collections.singleton(1));
        }
    }

    @Test
    public void mapFaceReachesEngineWithoutClaimingConjunctTracking() {
        ScanNodePropertiesResult result = new MapFaceOnlyProvider().getScanNodePropertiesResult(
                null, HANDLE, Collections.emptyList(), Optional.empty());

        // The map a Map-face-only connector returns is what the engine consumes. MUTATION: making the
        // default return an empty map -> red (and in production the connector's location.* credentials
        // would never reach BE).
        Assertions.assertEquals("parquet", result.getProperties().get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));

        // Not "everything was pushed": the engine must keep every conjunct. MUTATION: switching the
        // default to withPushdownTracking(props, emptySet()) -> red, and in production every conjunct
        // would be pruned unevaluated -> extra rows.
        Assertions.assertFalse(result.hasConjunctTracking());
        Assertions.assertTrue(result.getNotPushedConjunctIndices().isEmpty());
    }

    @Test
    public void resultFaceCarriesTheReportedNotPushedIndices() {
        ScanNodePropertiesResult result = new TrackingFaceProvider().getScanNodePropertiesResult(
                null, HANDLE, Collections.emptyList(), Optional.empty());

        Assertions.assertTrue(result.hasConjunctTracking());
        Assertions.assertEquals(Collections.singleton(1), result.getNotPushedConjunctIndices());
        Assertions.assertEquals("es_http", result.getProperties().get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
    }

    @Test
    public void trackingWithAnEmptySetMeansPruneEverything() {
        Set<Integer> nothingKept = Collections.emptySet();
        ScanNodePropertiesResult result =
                ScanNodePropertiesResult.withPushdownTracking(Collections.emptyMap(), nothingKept);

        // The two factories are the only way to pick this bit now; it used to be encoded by which
        // constructor overload the caller happened to choose.
        Assertions.assertTrue(result.hasConjunctTracking());
        Assertions.assertTrue(result.getNotPushedConjunctIndices().isEmpty());
        Assertions.assertFalse(ScanNodePropertiesResult.of(Collections.emptyMap()).hasConjunctTracking());
    }
}
