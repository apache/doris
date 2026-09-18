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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.jar.JarFile;

/**
 * Decides whether a directory-loaded plugin was built against an API this FE can serve.
 *
 * <p><b>Why a manifest attribute and not a method on the SPI.</b> The plugin's declared version must
 * physically travel with the plugin artifact. A {@code default} method on an SPI interface cannot do that: the
 * SPI interface is always loaded parent-first from the FE kernel (see
 * {@link ChildFirstClassLoader#DEFAULT_PARENT_FIRST_PACKAGES} and each family's parent-first prefixes), and
 * every plugin zip excludes the SPI jar, so the default body executed on a "plugin's" behalf is the kernel's
 * own code. Such a check is true by construction and can never reject anything. Reading a MANIFEST attribute
 * of the jar that defines the plugin's factory class reads a value that was stamped when the plugin was built.
 *
 * <h2>One number, distributed to two places</h2>
 *
 * <p>Each family declares its API version once, as a maven property in its parent pom. That single property
 * flows to both sides of the comparison in the same build:
 * <ol>
 *   <li>into a filtered resource inside the family's SPI module, which this class reads to learn what the
 *       kernel expects;</li>
 *   <li>into the {@code <manifestEntries>} of every plugin jar built from that pom, which
 *       {@link DirectoryPluginRuntimeManager} reads to learn what a plugin declares.</li>
 * </ol>
 * There is no second number to keep in sync, so no synchronization test is needed.
 *
 * <h2>Naming convention (families are derived, not enumerated)</h2>
 *
 * <p>A family is named by a single lower-case token, e.g. {@code connector}. Everything else follows from it:
 * <ul>
 *   <li>kernel resource: {@code /META-INF/doris/connector-plugin-api-version.properties}, holding
 *       {@code api.version=<major.minor[.patch]>};</li>
 *   <li>plugin MANIFEST attribute: {@code Doris-Connector-Plugin-Api-Version}.</li>
 * </ul>
 * A new family only has to follow the convention on both the pom side and the manager side; this class stays
 * family-neutral.
 *
 * <h2>Decision rule</h2>
 *
 * <p>Major must match exactly; minor and patch are ignored in both directions. That is only sound because
 * "major" is defined as <em>any</em> change to the SPI surface — including additions — so a compatible minor
 * can never change the set of API elements a plugin can see.
 */
public final class ApiVersionGate {

    /** The single key inside every family's kernel-side version resource. */
    static final String VERSION_KEY = "api.version";

    private final String familyLabel;
    private final String manifestAttribute;
    private final String expectedVersion;
    private final int expectedMajor;
    private final ClassLoader kernelClassLoader;

    private ApiVersionGate(String familyLabel, String manifestAttribute, String expectedVersion,
            int expectedMajor, ClassLoader kernelClassLoader) {
        this.familyLabel = familyLabel;
        this.manifestAttribute = manifestAttribute;
        this.expectedVersion = expectedVersion;
        this.expectedMajor = expectedMajor;
        this.kernelClassLoader = kernelClassLoader;
    }

    /**
     * Builds the gate for one plugin family by reading the version the kernel was built with.
     *
     * <p>The resource is looked up through {@code spiAnchor} so that it is read from the very artifact that
     * carries the family's SPI types — pass an interface the family's plugins implement (for example
     * {@code ConnectorProvider}), not a class from the calling module.
     *
     * @param family     lower-case single-token family name, e.g. {@code connector}
     * @param spiAnchor  a type from the family's SPI artifact, used to locate the kernel version resource
     * @throws IllegalStateException if the resource is missing, unreadable, or holds no parseable version.
     *         That is a build defect, not a deployment problem, so it fails the FE loudly instead of
     *         degrading into a check that admits everything.
     */
    public static ApiVersionGate forFamily(String family, Class<?> spiAnchor) {
        Objects.requireNonNull(family, "family");
        Objects.requireNonNull(spiAnchor, "spiAnchor");
        String resource = versionResourceOf(family);
        String declared = readKernelVersion(family, spiAnchor, resource);
        OptionalInt major = parseMajor(declared);
        if (!major.isPresent()) {
            throw new IllegalStateException("Malformed " + VERSION_KEY + "='" + declared + "' in " + resource
                    + " (from " + spiAnchor.getName() + "); expected major[.minor[.patch]]. This is a build"
                    + " defect: the maven property feeding this resource is missing or was not filtered.");
        }
        return new ApiVersionGate(family.toUpperCase(Locale.ROOT), manifestAttributeOf(family),
                declared.trim(), major.getAsInt(), spiAnchor.getClassLoader());
    }

    /** Kernel-side resource path for a family, by convention. */
    static String versionResourceOf(String family) {
        return "/META-INF/doris/" + family + "-plugin-api-version.properties";
    }

    /** Plugin-side MANIFEST attribute name for a family, by convention. */
    static String manifestAttributeOf(String family) {
        return "Doris-" + Character.toUpperCase(family.charAt(0)) + family.substring(1) + "-Plugin-Api-Version";
    }

    private static String readKernelVersion(String family, Class<?> spiAnchor, String resource) {
        try (InputStream in = spiAnchor.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Missing " + resource + " on the classpath of "
                        + spiAnchor.getName() + ". The " + family + " SPI artifact must carry the filtered"
                        + " resource that records which plugin API version this FE was built with.");
            }
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty(VERSION_KEY);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + resource + " from "
                    + spiAnchor.getName(), e);
        }
    }

    public String getManifestAttribute() {
        return manifestAttribute;
    }

    public String getExpectedVersion() {
        return expectedVersion;
    }

    public int getExpectedMajor() {
        return expectedMajor;
    }

    /**
     * Judges what a plugin jar declared.
     *
     * <p>Fail-closed: a jar that declares nothing is rejected, so that a plugin built without any awareness of
     * this contract cannot slip through. A third party can still get in by declaring a version it was not
     * built against, but that is an active false claim rather than an omission.
     *
     * @param declaredVersion the value of {@link #getManifestAttribute()} in the plugin jar, or null when absent
     * @return null when the plugin may load, otherwise a diagnostic naming the declared and expected values
     */
    public String rejectionReason(String declaredVersion) {
        if (declaredVersion == null || declaredVersion.trim().isEmpty()) {
            return "no " + manifestAttribute + " in the plugin jar MANIFEST; this FE serves " + familyLabel
                    + " plugin API " + expectedVersion + ". Declare the attribute and rebuild the plugin"
                    + " against this Doris release.";
        }
        OptionalInt major = parseMajor(declaredVersion);
        if (!major.isPresent()) {
            return "malformed " + manifestAttribute + "='" + declaredVersion.trim()
                    + "' in the plugin jar MANIFEST (expected major[.minor[.patch]]); this FE serves "
                    + familyLabel + " plugin API " + expectedVersion + ".";
        }
        if (major.getAsInt() != expectedMajor) {
            return "incompatible " + manifestAttribute + "='" + declaredVersion.trim() + "': major "
                    + major.getAsInt() + " but this FE serves " + familyLabel + " plugin API "
                    + expectedVersion + " (major " + expectedMajor + "). Rebuild the plugin against this"
                    + " Doris release.";
        }
        return null;
    }

    /** Applies the same manifest-major contract to a classpath-discovered provider. */
    public String rejectionReasonForClass(Class<?> pluginClass) {
        Path definingJar = ManifestVersions.jarOf(pluginClass);
        if (definingJar == null) {
            // Maven/IDE runs expose kernel-built providers as class directories, but an independently
            // loaded exploded provider has no immutable declaration and must remain fail-closed.
            return pluginClass.getClassLoader() == kernelClassLoader ? null : rejectionReason((String) null);
        }
        try (JarFile jar = new JarFile(definingJar.toFile())) {
            return rejectionReason(ManifestVersions.mainAttribute(jar, manifestAttribute));
        } catch (IOException e) {
            return rejectionReason((String) null);
        }
    }

    /**
     * Major segment of {@code major[.minor[.patch]]}, or empty when the value is not that shape.
     *
     * <p>Minor and patch are recorded in the manifest for operators (they surface nowhere else) but take no
     * part in the decision; they are still required to be numeric so that a typo is reported as malformed
     * rather than silently reduced to its major.
     */
    private static OptionalInt parseMajor(String version) {
        if (version == null) {
            return OptionalInt.empty();
        }
        String trimmed = version.trim();
        if (trimmed.isEmpty()) {
            return OptionalInt.empty();
        }
        String[] segments = trimmed.split("\\.", -1);
        if (segments.length > 3) {
            return OptionalInt.empty();
        }
        for (String segment : segments) {
            if (segment.isEmpty()) {
                return OptionalInt.empty();
            }
            for (int i = 0; i < segment.length(); i++) {
                if (!Character.isDigit(segment.charAt(i))) {
                    return OptionalInt.empty();
                }
            }
        }
        try {
            return OptionalInt.of(Integer.parseInt(segments[0]));
        } catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
    }
}
