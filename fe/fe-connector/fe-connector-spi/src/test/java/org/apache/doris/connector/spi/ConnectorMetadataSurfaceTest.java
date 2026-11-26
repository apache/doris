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

package org.apache.doris.connector.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/**
 * Freezes the public method surface of {@link ConnectorMetadata} and pins the minimum implementation set.
 *
 * <p><b>WHY this matters:</b> every method of {@link ConnectorMetadata} and its six {@code Ops}
 * sub-interfaces has a default body, so the compiler forces nothing on an implementor and no existing test
 * fails when a method silently disappears, gains a parameter, or is moved between interfaces. That is exactly
 * what happened when {@code ConnectorTableOps} was split into per-domain super-interfaces: the split is only
 * safe if the resulting method set is IDENTICAL, and "identical" was previously unverifiable. This test makes
 * the surface a reviewed artifact — {@code src/test/resources/connector-metadata-methods.txt} is the recorded
 * set, and any change to it has to be made deliberately.</p>
 *
 * <p><b>When this test goes red on purpose:</b> a batch that intentionally deletes or adds SPI methods must
 * regenerate the baseline in the same commit. That is the point — it is a speed bump on the public connector
 * surface, not an obstacle. Regenerate by running this test and copying the "actual" set from the failure
 * message.</p>
 */
public class ConnectorMetadataSurfaceTest {

    private static final String BASELINE_RESOURCE = "/connector-metadata-methods.txt";

    /** The six domain interfaces {@link ConnectorTableOps} aggregates. Each states its own minimum set. */
    private static final List<Class<?>> TABLE_DOMAINS = Arrays.asList(
            ConnectorTableMetadataOps.class,
            ConnectorViewOps.class,
            ConnectorTableDdlOps.class,
            ConnectorColumnEvolutionOps.class,
            ConnectorSnapshotRefOps.class,
            ConnectorPartitionListingOps.class);

    /**
     * The methods every connector must override, no matter which capabilities it declares. All eight shipped
     * connectors override the first four; seven of eight override the fifth. Promoting a sixth method into
     * this set is a real decision about what "a working connector" means, so it must be made here first.
     */
    private static final List<String> UNCONDITIONAL = Arrays.asList(
            "buildTableDescriptor",
            "getColumnHandles",
            "getTableHandle",
            "getTableSchema",
            "listTableNames");

    @Test
    public void methodSurfaceMatchesRecordedBaseline() throws IOException {
        TreeSet<String> actual = renderSurface(ConnectorMetadata.class);
        TreeSet<String> expected = readBaseline();

        TreeSet<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        TreeSet<String> added = new TreeSet<>(actual);
        added.removeAll(expected);

        Assertions.assertTrue(missing.isEmpty() && added.isEmpty(),
                "ConnectorMetadata's method surface changed.\n"
                        + "  gone from the baseline (deleted, renamed, or signature changed): " + missing + "\n"
                        + "  new since the baseline: " + added + "\n"
                        + "If the change is intended, update src/test/resources"
                        + BASELINE_RESOURCE + " in the same commit. Full actual surface:\n"
                        + String.join("\n", actual));
    }

    @Test
    public void everyMustImplementMarkerSitsOnItsOwnDomain() {
        List<String> misplaced = new ArrayList<>();
        for (Method m : ConnectorMetadata.class.getMethods()) {
            if (m.getAnnotation(ConnectorMustImplement.class) == null) {
                continue;
            }
            if (!TABLE_DOMAINS.contains(m.getDeclaringClass())) {
                misplaced.add(m.getDeclaringClass().getSimpleName() + "#" + m.getName());
            }
        }
        Assertions.assertEquals(Collections.emptyList(), misplaced,
                "@ConnectorMustImplement must be declared on one of the six table domain interfaces, so that "
                        + "'which methods are required' stays attached to the domain that documents why. "
                        + "Markers found elsewhere: " + misplaced);
    }

    @Test
    public void everyDomainDeclaresItsRequiredMethods() {
        for (Class<?> domain : TABLE_DOMAINS) {
            boolean marked = false;
            for (Method m : domain.getDeclaredMethods()) {
                if (m.getAnnotation(ConnectorMustImplement.class) != null) {
                    marked = true;
                    break;
                }
            }
            Assertions.assertTrue(marked, domain.getSimpleName()
                    + " declares no @ConnectorMustImplement method. Every domain must state what makes it "
                    + "mandatory — either unconditionally or under a stated precondition — otherwise a "
                    + "connector author has no way to tell 'nothing is required here' from 'nobody wrote it "
                    + "down'.");
        }
    }

    @Test
    public void unconditionallyRequiredMethodsAreExactlyTheKnownFive() {
        TreeSet<String> unconditional = new TreeSet<>();
        for (Method m : ConnectorMetadata.class.getMethods()) {
            ConnectorMustImplement marker = m.getAnnotation(ConnectorMustImplement.class);
            if (marker != null && marker.when().isEmpty()) {
                unconditional.add(m.getName());
            }
        }
        Assertions.assertEquals(new TreeSet<>(UNCONDITIONAL), unconditional,
                "The set of unconditionally required SPI methods changed. Each entry claims 'a connector that "
                        + "does not override this cannot serve a single query'; adding one silently makes every "
                        + "existing connector retroactively incomplete. Update UNCONDITIONAL here, with the "
                        + "override evidence, in the same commit as the annotation change.");
    }

    private static TreeSet<String> renderSurface(Class<?> iface) {
        TreeSet<String> rendered = new TreeSet<>();
        for (Method m : iface.getMethods()) {
            if (m.isSynthetic()) {
                continue;
            }
            StringBuilder sb = new StringBuilder(m.getName()).append('(');
            Class<?>[] params = m.getParameterTypes();
            for (int i = 0; i < params.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(params[i].getTypeName());
            }
            rendered.add(sb.append(')').toString());
        }
        return rendered;
    }

    private static TreeSet<String> readBaseline() throws IOException {
        TreeSet<String> baseline = new TreeSet<>();
        try (InputStream in = ConnectorMetadataSurfaceTest.class.getResourceAsStream(BASELINE_RESOURCE)) {
            Assertions.assertNotNull(in, "missing test resource " + BASELINE_RESOURCE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    baseline.add(line.trim());
                }
            }
        }
        return baseline;
    }
}
