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

package org.apache.doris.common.classloader;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

public class JniScannerClassLoaderTest {
    private static final String FILE_IO = "org.apache.iceberg.io.FileIO";
    private static final List<String> FACTORIES = Arrays.asList(
            "org.apache.iceberg.CatalogUtil", "org.apache.iceberg.io.ResolvingFileIO");
    private static final List<String> SHARED_TYPES = Arrays.asList(
            FILE_IO,
            "org.apache.iceberg.io.DelegateFileIO",
            "org.apache.iceberg.FileScanTask",
            "org.apache.iceberg.StaticDataTask",
            "org.apache.iceberg.aws.s3.S3FileIO",
            "org.apache.iceberg.hadoop.HadoopFileIO",
            "org.apache.hadoop.conf.Configuration",
            "org.apache.doris.common.jni.JniScanner",
            "org.apache.iceberg.CatalogUtilSupport",
            "org.apache.iceberg.io.ResolvingFileIOHelpers");

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void icebergFactoriesAndTheirNestedClassesAreChildFirst() throws Exception {
        Fixture fixture = fixture(true);
        try (URLClassLoader parent = fixture.parent();
                JniScannerClassLoader scanner = fixture.scanner("iceberg-metadata-scanner", parent)) {
            for (String name : FACTORIES) {
                Class<?> factory = assertLoadedBy(scanner, name, scanner, "child");
                Class<?> nested = assertLoadedBy(scanner, name + "$Nested", scanner, "child");
                Assert.assertNotSame(parent.loadClass(name), factory);
                Assert.assertSame(nested, factory.getMethod("nestedType").invoke(null));
                Assert.assertSame("factory references must use the shared FileIO identity",
                        parent.loadClass(FILE_IO), factory.getMethod("fileIoType").invoke(null));
            }
        }
    }

    @Test
    public void icebergSharedContractsAndOtherFileIoImplementationsStayParentFirst() throws Exception {
        Fixture fixture = fixture(true);
        try (URLClassLoader parent = fixture.parent();
                JniScannerClassLoader scanner = fixture.scanner("iceberg-metadata-scanner", parent)) {
            for (String name : SHARED_TYPES) {
                Assert.assertSame(parent.loadClass(name), assertLoadedBy(scanner, name, parent, "parent"));
            }
        }
    }

    @Test
    public void otherScannerNamesKeepTheIcebergFactoriesParentFirst() throws Exception {
        Fixture fixture = fixture(true);
        try (URLClassLoader parent = fixture.parent()) {
            for (String scannerName : Arrays.asList("paimon-scanner", "iceberg", "iceberg-metadata-scanner-extra")) {
                try (JniScannerClassLoader scanner = fixture.scanner(scannerName, parent)) {
                    for (String name : FACTORIES) {
                        Assert.assertSame(parent.loadClass(name), assertLoadedBy(scanner, name, parent, "parent"));
                        Assert.assertSame(parent.loadClass(name + "$Nested"),
                                assertLoadedBy(scanner, name + "$Nested", parent, "parent"));
                    }
                }
            }
        }
    }

    @Test
    public void icebergFactoriesFallBackToParentWhenAbsentFromTheScanner() throws Exception {
        Fixture fixture = fixture(false);
        try (URLClassLoader parent = fixture.parent();
                JniScannerClassLoader scanner = fixture.scanner("iceberg-metadata-scanner", parent)) {
            for (String name : FACTORIES) {
                Assert.assertSame(parent.loadClass(name), assertLoadedBy(scanner, name, parent, "parent"));
                Assert.assertSame(parent.loadClass(name + "$Nested"),
                        assertLoadedBy(scanner, name + "$Nested", parent, "parent"));
            }
        }
    }

    private static Class<?> assertLoadedBy(ClassLoader scanner, String name, ClassLoader definingLoader, String marker)
            throws ReflectiveOperationException {
        Class<?> loaded = scanner.loadClass(name);
        Assert.assertSame(name, definingLoader, loaded.getClassLoader());
        Assert.assertEquals(name, marker, loaded.getMethod("origin").invoke(null));
        Assert.assertSame("repeat loads must retain class identity: " + name, loaded, scanner.loadClass(name));
        return loaded;
    }

    private Fixture fixture(boolean includeChildFactories) throws IOException {
        List<String> allTypes = new ArrayList<>(SHARED_TYPES);
        allTypes.addAll(FACTORIES);
        URL parent = compileMarkers("parent", allTypes);
        URL child = compileMarkers("child", includeChildFactories ? allTypes : SHARED_TYPES);
        return new Fixture(parent, child);
    }

    private URL compileMarkers(String marker, List<String> classNames) throws IOException {
        // The test has no SDK dependency: only the exact binary names determine the loader policy.
        Path directory = temporaryFolder.newFolder(marker).toPath();
        Path classes = Files.createDirectory(directory.resolve("classes"));
        Path emptyClasspath = Files.createDirectory(directory.resolve("empty-classpath"));
        List<File> sources = new ArrayList<>();
        for (String name : classNames) {
            int packageSeparator = name.lastIndexOf('.');
            String packageName = name.substring(0, packageSeparator);
            String simpleName = name.substring(packageSeparator + 1);
            String source = "package " + packageName + "; public class " + simpleName + " {"
                    + " public static String origin() { return \"" + marker + "\"; }"
                    + " public static Class<?> fileIoType() { return " + FILE_IO + ".class; }"
                    + " public static Class<?> nestedType() { return Nested.class; }"
                    + " public static class Nested {"
                    + " public static String origin() { return \"" + marker + "\"; } } }";
            Path sourceFile = directory.resolve("sources").resolve(name.replace('.', '/') + ".java");
            Files.createDirectories(sourceFile.getParent());
            Files.write(sourceFile, source.getBytes(StandardCharsets.UTF_8));
            sources.add(sourceFile.toFile());
        }
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assert.assertNotNull("loader fixtures require the JDK compiler", compiler);
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(
                diagnostics, null, StandardCharsets.UTF_8)) {
            List<String> options = Arrays.asList("-proc:none", "-d", classes.toString(),
                    "-classpath", emptyClasspath.toString());
            boolean compiled = compiler.getTask(null, fileManager, diagnostics, options, null,
                    fileManager.getJavaFileObjectsFromFiles(sources)).call();
            Assert.assertTrue("fixture compilation failed: " + diagnostics.getDiagnostics(), compiled);
        }
        return classes.toUri().toURL();
    }

    private static final class Fixture {
        private final URL parentClasses;
        private final URL scannerClasses;

        private Fixture(URL parentClasses, URL scannerClasses) {
            this.parentClasses = parentClasses;
            this.scannerClasses = scannerClasses;
        }

        private URLClassLoader parent() {
            // Exclude the test/application loader so real SDK classes cannot satisfy fixture lookups.
            return new URLClassLoader(new URL[] {parentClasses}, ClassLoader.getPlatformClassLoader());
        }

        private JniScannerClassLoader scanner(String name, ClassLoader parent) {
            return new JniScannerClassLoader(name, Collections.singletonList(scannerClasses), parent);
        }
    }
}
