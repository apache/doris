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

package org.apache.doris.jni.spi;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The JNI plugin API version this BE build serves.
 *
 * <h2>One number, two places</h2>
 *
 * <p>{@code jni.plugin.api.version} is declared once, in {@code fe/be-java-extensions/pom.xml}, and
 * the same build sends it to both sides of the comparison:
 * <ol>
 *   <li>filtered into {@link #VERSION_RESOURCE} inside this SPI jar — what BE expects, read here;</li>
 *   <li>stamped into every plugin jar's MANIFEST as {@link #MANIFEST_ATTRIBUTE} — what a plugin
 *       declares, read by the plugin loader from the jar that defines the plugin's
 *       {@link DorisPlugin}.</li>
 * </ol>
 * There is no second number that could drift, so no test has to keep them in sync.
 *
 * <p><b>The plugin side must be read from the MANIFEST, never from a method on the SPI.</b> A
 * {@code default} method on {@link DorisPlugin} would look like it reports the plugin's version but
 * could not: the SPI is always loaded from BE's own jar and never packaged into a plugin, so such a
 * method executes BE's code and is true by construction. The MANIFEST attribute is stamped when the
 * plugin is built and physically travels with it. This mirrors the FE plugin loader, which reached
 * the same conclusion for the same reason.
 *
 * <p>Comparison rule: major must match exactly; minor is ignored in both directions. That is sound
 * only because "major" means <em>any</em> change to the SPI surface, additions included — so a
 * differing minor can never change the set of API elements a plugin can see. Bump the major, and
 * zero the minor, in the same commit as any SPI change.
 */
public final class SpiVersion {

    /** Resource inside this jar carrying the version BE serves. */
    public static final String VERSION_RESOURCE = "/META-INF/doris/jni-plugin-api-version.properties";

    /** MANIFEST attribute carrying the version a plugin was built against. */
    public static final String MANIFEST_ATTRIBUTE = "Doris-Jni-Plugin-Api-Version";

    private static final String VERSION_KEY = "api.version";

    private static final String VERSION = readVersion();

    private SpiVersion() {
    }

    /** The full version string BE serves, for example {@code "1.0"}. */
    public static String version() {
        return VERSION;
    }

    /** Major component of {@link #version()}; the only part the compatibility rule compares. */
    public static int major() {
        return majorOf(VERSION);
    }

    /**
     * Major component of an arbitrary declared version.
     *
     * @return the major, or -1 when the string is absent or not of the form {@code major[.minor]}
     */
    public static int majorOf(String declared) {
        if (declared == null) {
            return -1;
        }
        String trimmed = declared.trim();
        int dot = trimmed.indexOf('.');
        String head = dot < 0 ? trimmed : trimmed.substring(0, dot);
        if (head.isEmpty()) {
            return -1;
        }
        for (int i = 0; i < head.length(); i++) {
            if (head.charAt(i) < '0' || head.charAt(i) > '9') {
                return -1;
            }
        }
        try {
            return Integer.parseInt(head);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static String readVersion() {
        try (InputStream in = SpiVersion.class.getResourceAsStream(VERSION_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Missing " + VERSION_RESOURCE + " in the jni-spi jar."
                        + " This is a build defect: the filtered resource that records which plugin API"
                        + " version this BE was built with was not packaged.");
            }
            Properties properties = new Properties();
            properties.load(in);
            String declared = properties.getProperty(VERSION_KEY);
            if (majorOf(declared) < 0) {
                throw new IllegalStateException("Malformed " + VERSION_KEY + "='" + declared + "' in "
                        + VERSION_RESOURCE + "; expected major[.minor]. This is a build defect: the maven"
                        + " property feeding this resource is missing or was not filtered.");
            }
            return declared.trim();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + VERSION_RESOURCE + " from the jni-spi jar", e);
        }
    }
}
