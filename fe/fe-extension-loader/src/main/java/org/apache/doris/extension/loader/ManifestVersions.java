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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Reads plugin release versions from jar MANIFEST Implementation-Version.
 *
 * <p>Always resolves the jar that actually defined the class (its code
 * source) instead of {@link Package#getImplementationVersion()}: Package
 * metadata is fixed per package name and classloader by whichever jar defines
 * the package first, so with two same-package factories from different jars
 * the later one would report the earlier jar's version.
 */
final class ManifestVersions {

    private ManifestVersions() {
    }

    /**
     * Implementation-Version from the manifest of one jar, honoring the class's
     * package section first.
     *
     * <p>Per the jar spec a package section ("Name: com/acme/plugin/") overrides
     * the main attributes for classes in that package, mirroring
     * {@code Package.getImplementationVersion()}.
     *
     * @param packagePath manifest section name from {@link #packagePathOf}, may be null
     * @return the version, or null when the manifest or the attribute is absent
     */
    static String fromManifest(JarFile jarFile, String packagePath) throws IOException {
        Manifest manifest = jarFile.getManifest();
        if (manifest == null) {
            return null;
        }
        if (packagePath != null) {
            Attributes packageAttributes = manifest.getAttributes(packagePath);
            if (packageAttributes != null) {
                String version = packageAttributes.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
                if (version != null) {
                    return version;
                }
            }
        }
        return manifest.getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VERSION);
    }

    /** Manifest section name for the class's package ("com/acme/plugin/"), or null. */
    static String packagePathOf(Class<?> clazz) {
        String className = clazz.getName();
        int lastDot = className.lastIndexOf('.');
        return lastDot < 0 ? null : className.substring(0, lastDot).replace('.', '/') + "/";
    }

    /** Jar file that defined the class per its protection domain, or null. */
    static Path jarOf(Class<?> clazz) {
        try {
            CodeSource codeSource = clazz.getProtectionDomain().getCodeSource();
            if (codeSource == null || codeSource.getLocation() == null) {
                return null;
            }
            Path path = Paths.get(codeSource.getLocation().toURI());
            return Files.isRegularFile(path) ? path : null;
        } catch (URISyntaxException | RuntimeException e) {
            return null;
        }
    }
}
