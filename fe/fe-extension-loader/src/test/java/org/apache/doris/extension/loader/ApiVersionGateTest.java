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

package org.apache.doris.extension.loader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The decision rule itself: major must match and minor and patch never matter. Strict gates refuse missing
 * declarations; an explicitly configured legacy gate accepts only a genuinely absent declaration for the
 * documented legacy major.
 *
 * <p>Each case here encodes a promise made to plugin authors, not just current behavior. The two-way minor
 * cases are the load-bearing ones: they are only sound because "major" is defined to cover <em>additions</em>
 * to the SPI surface as well as removals, so a compatible minor can never change the set of API elements a
 * plugin can reach. If that definition is ever weakened, these two tests are the ones that must change first.
 *
 * <p>The gate under test is built from {@code src/test/resources/META-INF/doris/test-plugin-api-version
 * .properties} through the same {@code forFamily} path the four production families use — so a broken
 * resource convention fails here before it can reach a family.
 */
class ApiVersionGateTest {

    private static final ApiVersionGate GATE =
            ApiVersionGate.forFamily("test", ApiVersionGateTest.class);
    private static final ApiVersionGate LEGACY_GATE =
            ApiVersionGate.forFamilyAllowingUnversioned("test", ApiVersionGateTest.class, "7.0");

    @Test
    void testKernelVersionComesFromTheFilteredResource() {
        Assertions.assertEquals("7.2", GATE.getExpectedVersion());
        Assertions.assertEquals(7, GATE.getExpectedMajor());
        Assertions.assertTrue(GATE.rejectionReason(null).contains("TEST"),
                "the rejection text must name the family so a log line identifies which contract failed");
    }

    @Test
    void testManifestAttributeFollowsTheNamingConvention() {
        // The pom side writes this element name literally; nothing can machine-check that the two agree, so
        // the derived name is pinned here and quoted in the family wiring tests.
        Assertions.assertEquals("Doris-Test-Plugin-Api-Version", GATE.getManifestAttribute());
        Assertions.assertEquals("Doris-Connector-Plugin-Api-Version",
                ApiVersionGate.manifestAttributeOf("connector"));
        Assertions.assertEquals("/META-INF/doris/connector-plugin-api-version.properties",
                ApiVersionGate.versionResourceOf("connector"));
    }

    @Test
    void testExactVersionIsAccepted() {
        Assertions.assertNull(GATE.rejectionReason("7.2"));
    }

    @Test
    void testOlderMinorIsAccepted() {
        // A plugin built against 7.0 running on a 7.2 FE: 7.2 added nothing to the surface (that would have
        // been 8.0), so every API element the plugin was compiled against is still here.
        Assertions.assertNull(GATE.rejectionReason("7.0"));
    }

    @Test
    void testNewerMinorIsAccepted() {
        // The other direction, and the reason minor is not compared: a plugin built against 7.9 cannot be
        // calling anything 7.2 lacks, because a new API element would have forced major 8.
        Assertions.assertNull(GATE.rejectionReason("7.9"));
    }

    @Test
    void testPatchSegmentIsIgnoredAndOptional() {
        Assertions.assertNull(GATE.rejectionReason("7.2.13"));
        Assertions.assertNull(GATE.rejectionReason("7.0.0"));
    }

    @Test
    void testBareMajorIsAccepted() {
        Assertions.assertNull(GATE.rejectionReason("7"));
    }

    @Test
    void testSurroundingWhitespaceIsTolerated() {
        // Manifest values survive line folding and hand editing; a stray space is not a compatibility signal.
        Assertions.assertNull(GATE.rejectionReason("  7.2  "));
    }

    @Test
    void testDifferentMajorIsRejectedInBothDirections() {
        String older = GATE.rejectionReason("6.9");
        Assertions.assertNotNull(older);
        Assertions.assertTrue(older.contains("6.9") && older.contains("7.2"),
                "the diagnostic must print both the declared and the expected version: " + older);

        Assertions.assertNotNull(GATE.rejectionReason("8.0"),
                "a plugin built against a newer major may call methods this FE does not have");
    }

    @Test
    void testMissingDeclarationIsRejectedFailClosed() {
        // The whole point of the redesign: a plugin that says nothing must not pass. If this ever returns
        // null, the check has degenerated into the always-true one it replaced.
        String reason = GATE.rejectionReason(null);
        Assertions.assertNotNull(reason);
        Assertions.assertTrue(reason.contains("Doris-Test-Plugin-Api-Version"),
                "the diagnostic must name the attribute the plugin has to add: " + reason);
        Assertions.assertNotNull(GATE.rejectionReason(""));
        Assertions.assertNotNull(GATE.rejectionReason("   "));
    }

    @Test
    void testDocumentedLegacyUnversionedPluginIsAcceptedWithWarning() {
        Assertions.assertNull(LEGACY_GATE.rejectionReason(null));
        String warning = LEGACY_GATE.acceptanceWarning(null);
        Assertions.assertNotNull(warning);
        Assertions.assertTrue(warning.contains("legacy TEST plugin API 7.0"), warning);

        Assertions.assertNotNull(LEGACY_GATE.rejectionReason(""),
                "an explicit empty declaration is malformed, not a legacy artifact");
        Assertions.assertNull(LEGACY_GATE.acceptanceWarning("7.0"),
                "a versioned plugin must not be mislabeled as legacy");
    }

    @Test
    void testLegacyUnversionedPluginIsRejectedAfterKernelMajorUpgrade() {
        ApiVersionGate oldLegacy =
                ApiVersionGate.forFamilyAllowingUnversioned("test", ApiVersionGateTest.class, "6.9");
        String reason = oldLegacy.rejectionReason(null);
        Assertions.assertNotNull(reason);
        Assertions.assertTrue(reason.contains("6.9") && reason.contains("7.2"), reason);
        Assertions.assertNull(oldLegacy.acceptanceWarning(null));
    }

    @Test
    void testMalformedLegacyVersionConfigurationFailsLoudly() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ApiVersionGate.forFamilyAllowingUnversioned(
                        "test", ApiVersionGateTest.class, "legacy-v7"));
    }

    @Test
    void testMalformedDeclarationIsRejectedRatherThanTruncated() {
        // "7.x" must not be read as major 7. Silently salvaging a major out of a typo would admit a plugin
        // whose real build version nobody knows.
        for (String malformed : new String[] {"7.x", "v7.2", "7,2", "7.", ".2", "7.2.3.4", "seven", "-1"}) {
            Assertions.assertNotNull(GATE.rejectionReason(malformed),
                    "must be rejected as malformed: " + malformed);
        }
    }

    @Test
    void testMissingKernelResourceFailsLoud() {
        // A build that forgot to ship the filtered resource is a build defect. Degrading to "accept
        // everything" would hide it until a genuinely incompatible plugin corrupted something at runtime.
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> ApiVersionGate.forFamily("nosuchfamily", ApiVersionGateTest.class));
        Assertions.assertTrue(e.getMessage().contains("nosuchfamily-plugin-api-version.properties"),
                e.getMessage());
    }

    @Test
    void testUnfilteredKernelResourceFailsLoud() {
        // The realistic build defect: the resource shipped, but maven filtering was not enabled for its
        // directory, so the value is still a literal ${...} placeholder.
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> ApiVersionGate.forFamily("unfiltered", ApiVersionGateTest.class));
        Assertions.assertTrue(e.getMessage().contains("build defect"),
                "the diagnostic must say this is a build problem, not a deployment one: " + e.getMessage());
    }
}
