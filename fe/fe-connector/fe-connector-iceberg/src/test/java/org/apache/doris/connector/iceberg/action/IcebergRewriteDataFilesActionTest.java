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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.iceberg.rewrite.RewriteDataFilePlanner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins the argument spec + planning-parameter build of the connector {@link IcebergRewriteDataFilesAction}
 * (WS-REWRITE R3). The arguments and the min/max-file-size defaulting are ported verbatim from the engine
 * action; the validation messages must stay byte-identical (the connector reuses the shared fe-foundation
 * {@code NamedArguments}). The whole path is dormant until the P6.6 cutover.
 */
public class IcebergRewriteDataFilesActionTest {

    private static IcebergRewriteDataFilesAction action(java.util.Map<String, String> props,
            java.util.List<String> partitionNames) {
        return new IcebergRewriteDataFilesAction(props, partitionNames, null);
    }

    @Test
    public void defaultsMinMaxFileSizeFromTarget() {
        IcebergRewriteDataFilesAction a = action(Collections.emptyMap(), Collections.emptyList());
        a.validate();
        RewriteDataFilePlanner.Parameters p = a.buildRewriteParameters();
        // WHY: min/max-file-size are not plain defaults — when unset (0) they derive from target (75% / 180%),
        // the rule that decides which files are "outside the desired size range" and thus rewritten. MUTATION:
        // dropping the 0.75/1.8 defaulting -> min/max stay 0 -> nothing is ever too-small/too-large -> red.
        Assertions.assertEquals(536870912L, p.getTargetFileSizeBytes());
        Assertions.assertEquals((long) (536870912L * 0.75), p.getMinFileSizeBytes());
        Assertions.assertEquals((long) (536870912L * 1.8), p.getMaxFileSizeBytes());
        Assertions.assertEquals(5, p.getMinInputFiles());
        Assertions.assertFalse(p.isRewriteAll());
    }

    @Test
    public void rejectsMinFileSizeAboveMax() {
        IcebergRewriteDataFilesAction a = action(
                ImmutableMap.of("min-file-size-bytes", "100", "max-file-size-bytes", "50"),
                Collections.emptyList());
        // WHY: an inverted min/max range would silently rewrite nothing (or everything); the action must reject
        // it with the legacy byte-identical message. MUTATION: dropping the min<=max check -> no throw -> red.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals(
                "min-file-size-bytes must be less than or equal to max-file-size-bytes", e.getMessage());
    }

    @Test
    public void rejectsPartitionSpecification() {
        IcebergRewriteDataFilesAction a = action(Collections.emptyMap(), ImmutableList.of("p1"));
        // WHY: rewrite_data_files does not accept a PARTITION (...) clause (only WHERE); it must reject one.
        // MUTATION: dropping validateNoPartitions() -> no throw -> red.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertTrue(e.getMessage().contains("does not support partition specification"), e.getMessage());
    }

    @Test
    public void buildRewriteParametersCarriesExplicitArgs() {
        IcebergRewriteDataFilesAction a = action(
                ImmutableMap.<String, String>builder()
                        .put("target-file-size-bytes", "1000")
                        .put("min-input-files", "3")
                        .put("rewrite-all", "true")
                        .put("delete-file-threshold", "7")
                        .put("delete-ratio-threshold", "0.5")
                        .build(),
                Collections.emptyList());
        a.validate();
        RewriteDataFilePlanner.Parameters p = a.buildRewriteParameters();
        // WHY: every explicit argument must reach the planner unchanged (each drives a real selection rule).
        // MUTATION: buildRewriteParameters reading the wrong namedArgument -> a mismatch below -> red.
        Assertions.assertEquals(1000L, p.getTargetFileSizeBytes());
        Assertions.assertEquals(3, p.getMinInputFiles());
        Assertions.assertTrue(p.isRewriteAll());
        Assertions.assertEquals(7, p.getDeleteFileThreshold());
        Assertions.assertEquals(0.5, p.getDeleteRatioThreshold());
        // min/max still derive off the explicit target (750 / 1800) since they were not given.
        Assertions.assertEquals(750L, p.getMinFileSizeBytes());
        Assertions.assertEquals(1800L, p.getMaxFileSizeBytes());
    }

    @Test
    public void executeActionThrowsBecauseDistributed() {
        IcebergRewriteDataFilesAction a = action(Collections.emptyMap(), Collections.emptyList());
        // WHY: rewrite_data_files has no synchronous single-call body (it is the DISTRIBUTED procedure); the
        // execute() path must fail loud rather than silently no-op if a future caller wires it wrong. MUTATION:
        // executeAction returning a row instead of throwing -> no throw -> red.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> a.execute(null, null));
        Assertions.assertTrue(e.getMessage().contains("distributed procedure"), e.getMessage());
    }
}
