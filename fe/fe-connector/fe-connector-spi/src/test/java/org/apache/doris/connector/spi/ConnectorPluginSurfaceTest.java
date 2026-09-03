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

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

/**
 * Freezes the CONNECTOR plugin API surface, so that changing it cannot happen without also deciding the
 * version consequence.
 *
 * <p><b>Why this exists.</b> Every method here has a default body or is implemented by eight shipped connectors, so the compiler forces nothing on a plugin author and nothing fails when a method quietly appears, disappears, or changes shape. The plugin API version in
 * {@code <connector.plugin.api.version>} is the contract that says which FE a given plugin may load into,
 * and the rule attached to it is blunt: <em>any</em> change to the surface below — adding a type or a method
 * just as much as removing or re-signing one — is a MAJOR change. No unit test can prove somebody actually
 * bumped the property (a test sees only the current state, never the delta), so this is a speed bump, not a
 * gate: it makes the change visible in review, in the same commit, with the reason spelled out in the
 * failure message.
 *
 * <p><b>Regenerating.</b> Run this test, copy the "actual" block out of the failure message into
 * {@code src/test/resources/connector-plugin-surface.txt}, and bump the major of {@code connector.plugin.api.version} in
 * {@code fe/fe-connector/pom.xml} in the SAME commit.
 *
 * <p>{@code Plugin} / {@code PluginFactory} / {@code PluginContext} from fe-extension-spi are frozen here
 * too, and identically in the other three families' baselines. They are loaded parent-first for every family
 * (see {@code ChildFirstClassLoader.DEFAULT_PARENT_FIRST_PACKAGES}), so a change to them breaks all four
 * plugin kinds at once — and turns all four baselines red at once, each asking for its own bump.
 *
 * <p>Signatures are recorded with their return type, unlike the older
 * {@code connector-metadata-methods.txt} baseline: a changed return type is a MAJOR change by the same
 * definition, and a name-and-parameters-only record cannot see it.
 */
public class ConnectorPluginSurfaceTest {

    private static final String BASELINE_RESOURCE = "/connector-plugin-surface.txt";

    @Test
    public void connectorApiMajorTracksTheRecordedSurfaceChange() throws IOException {
        Properties version = new Properties();
        try (InputStream in = ConnectorProvider.class.getResourceAsStream(
                "/META-INF/doris/connector-plugin-api-version.properties")) {
            Assertions.assertNotNull(in, "missing connector plugin API version resource");
            version.load(in);
        }
        // Major 6: write binding gained execution-capability methods.
        // Major 7 adds ConnectorProvider#driverUrlsToValidate: the FE asks the provider which properties
        // name a jar it will load into the FE JVM, and applies the operator's jdbc_driver_secure_path /
        // jdbc_driver_url_white_list policy to them on ALTER CATALOG (which never reaches
        // Connector#preCreateValidation). An older plugin answers "no jars" by inheriting the default, so a
        // repointed driver_url on such a plugin would skip the operator's policy silently - the FE must
        // refuse it at load time instead.
        Assertions.assertEquals("7.0", version.getProperty("api.version"));
    }

    /** Root entry points plus provider/handle types returned to connector plugins. */
    private static final List<Class<?>> FROZEN_TYPES = Arrays.asList(
            ConnectorProvider.class,
            ConnectorContext.class,
            Connector.class,
            ConnectorColumnHandle.class,
            ConnectorTableSchema.class,
            ConnectorScanPlanProvider.class,
            ConnectorWriteHandle.class,
            ConnectorWritePlanProvider.class,
            org.apache.doris.extension.spi.Plugin.class,
            org.apache.doris.extension.spi.PluginFactory.class,
            org.apache.doris.extension.spi.PluginContext.class);

    @Test
    public void pluginApiSurfaceMatchesRecordedBaseline() throws IOException {
        TreeSet<String> actual = renderSurface();
        TreeSet<String> expected = readBaseline();

        TreeSet<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        TreeSet<String> added = new TreeSet<>(actual);
        added.removeAll(expected);

        Assertions.assertTrue(missing.isEmpty() && added.isEmpty(),
                "The CONNECTOR plugin API surface changed.\n"
                        + "  gone from the baseline (removed, renamed, or re-signed): " + missing + "\n"
                        + "  new since the baseline: " + added + "\n"
                        + "THIS IS A MAJOR CHANGE - the same commit that refreshes src/test/resources"
                        + BASELINE_RESOURCE + " must increment the major of <connector.plugin.api.version>"
                        + " in fe/fe-connector/pom.xml (and zero its minor).\n"
                        + "Full actual surface:\n" + String.join("\n", actual));
    }

    /**
     * One line per method reachable on a frozen type, keyed by that type rather than by the interface that
     * happens to declare it: what matters is what a plugin can call on the type it was handed, so moving a
     * default method up or down a super-interface chain is not by itself a surface change.
     */
    private static TreeSet<String> renderSurface() {
        TreeSet<String> rendered = new TreeSet<>();
        for (Class<?> frozen : FROZEN_TYPES) {
            for (Method m : frozen.getMethods()) {
                if (m.isSynthetic() || m.getDeclaringClass() == Object.class) {
                    continue;
                }
                StringBuilder sb = new StringBuilder(frozen.getName()).append('#')
                        .append(m.getName()).append('(');
                Class<?>[] params = m.getParameterTypes();
                for (int i = 0; i < params.length; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(params[i].getTypeName());
                }
                rendered.add(sb.append("):").append(m.getReturnType().getTypeName()).toString());
            }
        }
        return rendered;
    }

    private static TreeSet<String> readBaseline() throws IOException {
        TreeSet<String> baseline = new TreeSet<>();
        try (InputStream in = ConnectorPluginSurfaceTest.class.getResourceAsStream(BASELINE_RESOURCE)) {
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
