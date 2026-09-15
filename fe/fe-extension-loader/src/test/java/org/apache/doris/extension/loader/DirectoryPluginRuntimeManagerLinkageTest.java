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

import org.apache.doris.extension.loader.testplugins.AbsentDependencyProbe;
import org.apache.doris.extension.loader.testplugins.AbsentStaticInitTestPluginFactory;
import org.apache.doris.extension.loader.testplugins.AbsentSuperclassTestPluginFactory;
import org.apache.doris.extension.loader.testplugins.MetadataTestPluginFactory;
import org.apache.doris.extension.loader.testplugins.NotAFactory;
import org.apache.doris.extension.loader.testplugins.ThrowingStaticInitTestPluginFactory;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * A plugin whose dependency is absent must fail alone.
 *
 * <p>This is the shape of "the plugin is installed but the library bundle it needs is not": the
 * missing class is reported by the JVM as {@code NoClassDefFoundError}, an {@link Error}, which is
 * not a {@link ReflectiveOperationException} and so used to walk straight out of {@code loadAll} and
 * out of the FE's startup path with it. One uninstalled bundle must cost the plugins that need it,
 * not the FE.
 *
 * <p>The two cases are the two places the loader touches plugin bytecode, and they fail one step
 * apart: resolving the class (a supertype the jar does not carry) and first initializing it (a
 * static initializer that reaches outside the jar).
 *
 * <p>The advice sentence itself is asserted separately, against fabricated throwables, including the
 * shapes it must not misread: the JVM's wording for a class whose initializer had already failed (which
 * a parent-first class can carry in from an earlier plugin), a plugin's own sentence-shaped wording, and
 * the "wrong name" form that means a broken jar rather than a missing dependency.
 */
class DirectoryPluginRuntimeManagerLinkageTest {

    @TempDir
    Path tempDir;

    @Test
    void testAbsentSuperclassIsAFailureNotAThrow() throws IOException {
        Path root = tempDir.resolve("plugins");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("absent-superclass").resolve("absent-superclass.jar"),
                AbsentSuperclassTestPluginFactory.class, "1.0");

        assertReportsMissingDependency(load(root));
    }

    @Test
    void testAbsentStaticInitDependencyIsAFailureNotAThrow() throws IOException {
        Path root = tempDir.resolve("plugins");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("absent-static-init").resolve("absent-static-init.jar"),
                AbsentStaticInitTestPluginFactory.class, "1.0");

        assertReportsMissingDependency(load(root));
    }

    @Test
    void testAlreadyFailedInitializationIsNotReportedAsAMissingClass() {
        // "Could not initialize class X" means X was found and its initializer threw - a broken plugin,
        // not an uninstalled bundle. Read as a missing class, its first token would advertise a class
        // named "Could" and send the reader off to look at the shared library root.
        String advice = DirectoryPluginRuntimeManager.missingClassAdvice(
                new NoClassDefFoundError("Could not initialize class org.example.BrokenFactory"));

        Assertions.assertEquals("", advice,
                () -> "an initializer that already failed is not a missing dependency: " + advice);
    }

    @Test
    void testAMissingClassIsNamedInEveryFormTheJvmReportsItIn() {
        Assertions.assertTrue(adviceFor(new NoClassDefFoundError("org/example/Probe"))
                        .contains("The class org/example/Probe is"),
                "the plain form names the class directly");
        Assertions.assertTrue(
                adviceFor(new ExceptionInInitializerError(new ClassNotFoundException("org.example.Probe")))
                        .contains("The class org.example.Probe is"),
                "a static initializer's first failure wraps the real miss");

        // An initializer failure on top of a genuine miss: skipping the wording must not stop the walk.
        NoClassDefFoundError alreadyFailed =
                new NoClassDefFoundError("Could not initialize class org.example.BrokenFactory");
        alreadyFailed.initCause(new NoClassDefFoundError("org/example/Probe"));
        Assertions.assertTrue(adviceFor(alreadyFailed).contains("The class org/example/Probe is"),
                "the miss below the initializer failure is what the reader needs");
    }

    @Test
    void testAnInitializerThatThrowsIsAFailureThatNamesTheReason() throws IOException {
        // A non-Error thrown by <clinit> arrives wrapped in ExceptionInInitializerError, whose own
        // message is null: logged as toString() it says nothing. The reason has to be carried into
        // the failure message itself, because that message is all some consumers record.
        Path root = tempDir.resolve("plugins");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("throwing-static-init").resolve("throwing-static-init.jar"),
                ThrowingStaticInitTestPluginFactory.class, "1.0");

        LoadReport<PluginFactory> report = load(root);
        Assertions.assertTrue(report.getSuccesses().isEmpty(), "the plugin cannot have loaded");
        Assertions.assertEquals(1, report.getFailures().size());
        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_INSTANTIATE, failure.getStage());
        Assertions.assertInstanceOf(ExceptionInInitializerError.class, failure.getCause(),
                () -> "a non-Error from <clinit> is wrapped by the JVM, got " + failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains(ThrowingStaticInitTestPluginFactory.REASON),
                () -> "the initializer's own reason must reach the message: " + failure.getMessage());
        Assertions.assertFalse(failure.getMessage().contains("shared library bundle"),
                () -> "nothing is missing here, so no bundle advice: " + failure.getMessage());
    }

    @Test
    void testAServiceFileNamingANonFactoryIsAFailureNotAThrow() throws IOException {
        // asSubclass() answers a service entry that names a class of the wrong type with a
        // ClassCastException: a RuntimeException, so neither of the two other catch families sees it.
        Path root = tempDir.resolve("plugins");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("not-a-factory").resolve("not-a-factory.jar"),
                NotAFactory.class, "1.0", DirectoryPluginRuntimeManagerMetadataTest.TEST_GATE.getExpectedVersion());

        LoadReport<PluginFactory> report = load(root);
        Assertions.assertTrue(report.getSuccesses().isEmpty(), "the plugin cannot have loaded");
        Assertions.assertEquals(1, report.getFailures().size());
        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_INSTANTIATE, failure.getStage());
        Assertions.assertInstanceOf(ClassCastException.class, failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains("does not implement " + PluginFactory.class.getName()),
                () -> "the message must name the factory type the class was expected to implement: "
                        + failure.getMessage());
    }

    @Test
    void testACyclicCauseChainIsWalkedOnce() {
        // Legal Java: a's cause is still the sentinel when initCause runs, so the chain is a -> b -> a.
        // A plugin's constructor can throw this, and the walk runs on the FE's startup thread under
        // the loader's lifecycle lock, before any port is open.
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);

        String advice = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> DirectoryPluginRuntimeManager.missingClassAdvice(new InvocationTargetException(a)));
        Assertions.assertEquals("", advice, "no node of the cycle names a missing class");

        String summary = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> DirectoryPluginRuntimeManager.rootCauseSummary(new InvocationTargetException(a)));
        Assertions.assertTrue(summary.contains("java.lang.RuntimeException: b"),
                () -> "the last node reached before the chain repeats is the summary: " + summary);
    }

    @Test
    void testOnlyAClassNameShapedMessageIsReadAsAMissingClass() {
        // Plugin code may wrap a lookup failure in a sentence; its first word is not a class name.
        Assertions.assertEquals("", DirectoryPluginRuntimeManager.missingClassAdvice(
                        new ClassNotFoundException("Cannot load driver class com.example.Driver")),
                "a sentence-shaped message names no class the reader could act on");
        // ...but the JDK's own node below it does, and the walk has to reach it.
        Assertions.assertTrue(adviceFor(new ClassNotFoundException(
                        "SPI class 'org.example.Probe' was not found in BE's classloader",
                        new ClassNotFoundException("org.example.Probe")))
                        .contains("The class org.example.Probe is"),
                "the bare name one node down is the one to report");
        // A trailing dot or a path is plugin wording too, never the class.
        Assertions.assertEquals("", DirectoryPluginRuntimeManager.missingClassAdvice(
                new ClassNotFoundException("com.example.Driver. Bundle this class into that jar.")));
        // Binary and internal spellings, with nested-class and digit characters, are class names.
        Assertions.assertTrue(adviceFor(new NoClassDefFoundError("org/example/Outer$Inner2"))
                .contains("The class org/example/Outer$Inner2 is"));
    }

    @Test
    void testTheWrongNameFormIsABrokenJarNotAMissingDependency() throws IOException {
        // HotSpot's wording is "<name in the bytecode> (wrong name: <name requested>)": the class file
        // WAS found, under the requested name's path, and its bytecode says otherwise. Sending the
        // reader to the shared bundle would be exactly wrong. Driven through loadAll with a real jar
        // whose entry path and bytecode name disagree, so the orientation of the sentence is checked
        // against the JVM rather than against a fabricated string.
        String requested = NotAFactory.class.getName().replace("NotAFactory", "Renamed");
        String entry = requested.replace('.', '/') + ".class";
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put(entry, classBytes(NotAFactory.class));
        Path root = tempDir.resolve("plugins");
        writeJar(root.resolve("wrong-name").resolve("wrong-name.jar"), requested, entries);

        LoadReport<PluginFactory> report = load(root);
        Assertions.assertEquals(1, report.getFailures().size(), () -> "" + report.getSuccesses());
        String message = report.getFailures().get(0).getMessage();
        String bytecodeName = NotAFactory.class.getName().replace('.', '/');
        String requestedName = requested.replace('.', '/');
        Assertions.assertTrue(message.contains("The class " + requestedName + " was found in this plugin's jars"),
                message);
        Assertions.assertTrue(message.contains("names it " + bytecodeName + ":"), message);
        Assertions.assertTrue(message.contains("wrong path"), message);
        Assertions.assertFalse(message.contains("shared library bundle"), message);
    }

    @Test
    void testALongClassNameIsDiagnosedWithoutOverflowingTheStack() throws IOException {
        // The message is plugin-controlled text: a service file naming a class of the longest legal
        // length arrives as a ClassNotFoundException whose message is that name. A regex with one
        // recursion per token overflowed the FE's default 1 MB stack here - inside the catch block,
        // which turned a tolerated failure into an FE exit.
        String longName = String.join(".", Collections.nCopies(6000, "abcdefghij"));
        Path root = tempDir.resolve("plugins");
        writeJar(root.resolve("long-name").resolve("long-name.jar"), longName, Collections.emptyMap());

        LoadReport<PluginFactory> report = load(root);
        Assertions.assertEquals(1, report.getFailures().size());
        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_INSTANTIATE, failure.getStage());
        Assertions.assertTrue(failure.getMessage().contains("The class " + longName + " is"),
                "the name is read as a class name, however long");

        // The direct forms too, on both diagnostics.
        Assertions.assertDoesNotThrow(() -> DirectoryPluginRuntimeManager.missingClassAdvice(
                new NoClassDefFoundError(longName.replace('.', '/'))));
        Assertions.assertDoesNotThrow(() -> DirectoryPluginRuntimeManager.missingClassAdvice(
                new NoClassDefFoundError(longName.replace('.', '/') + " (wrong name: " + longName + ")")));
    }

    @Test
    void testACauselessLinkageErrorIsSummarisedByItself() throws IOException {
        // A class file compiled for a newer JDK fails with UnsupportedClassVersionError, which has no
        // cause: the failure itself is the whole story, and the message-only consumers must get it.
        byte[] bytes = classBytes(MetadataTestPluginFactory.class);
        bytes[6] = (byte) 0xFF;
        bytes[7] = (byte) 0xFF;
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put(MetadataTestPluginFactory.class.getName().replace('.', '/') + ".class", bytes);
        Path root = tempDir.resolve("plugins");
        writeJar(root.resolve("newer-jdk").resolve("newer-jdk.jar"), MetadataTestPluginFactory.class.getName(),
                entries);

        LoadReport<PluginFactory> report = load(root);
        Assertions.assertEquals(1, report.getFailures().size());
        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertInstanceOf(UnsupportedClassVersionError.class, failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains("; caused by java.lang.UnsupportedClassVersionError"),
                failure.getMessage());
    }

    @Test
    void testARuntimeExceptionWhileResolvingTheFactoryClassIsAFailureNotAThrow() throws IOException {
        // Site A: defining the factory class resolves its supertypes through the parent, and a parent
        // may answer with a SecurityException (a signed package the plugin also ships unsigned classes
        // into). A RuntimeException, so neither ReflectiveOperationException nor LinkageError.
        Path root = tempDir.resolve("plugins");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("refused-supertype").resolve("refused-supertype.jar"),
                AbsentSuperclassTestPluginFactory.class, "1.0");
        ClassLoader refusingParent = new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (AbsentDependencyProbe.class.getName().equals(name)) {
                    throw new SecurityException("signer information does not match: " + name);
                }
                return super.loadClass(name, resolve);
            }
        };

        LoadReport<PluginFactory> report = load(root, refusingParent);
        Assertions.assertEquals(1, report.getFailures().size());
        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_INSTANTIATE, failure.getStage());
        Assertions.assertInstanceOf(SecurityException.class, failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains("; caused by java.lang.SecurityException: signer"),
                failure.getMessage());
    }

    @Test
    void testTheJdk17SecondAttemptShapeIsSummarised() {
        // JDK 17's second attempt at a class whose initializer already failed: the miss is named only in
        // the ExceptionInInitializerError's message, one node down. No advice (nothing new is missing),
        // but the summary must carry that text.
        NoClassDefFoundError secondAttempt =
                new NoClassDefFoundError("Could not initialize class org.example.BrokenFactory");
        secondAttempt.initCause(new ExceptionInInitializerError(
                "Exception java.lang.NoClassDefFoundError: org/example/Probe [in thread \"main\"]"));
        Assertions.assertEquals("", DirectoryPluginRuntimeManager.missingClassAdvice(secondAttempt));
        Assertions.assertTrue(DirectoryPluginRuntimeManager.rootCauseSummary(secondAttempt)
                .contains("java.lang.ExceptionInInitializerError: Exception java.lang.NoClassDefFoundError:"
                        + " org/example/Probe"));
    }

    @Test
    void testOnlyAClassNameShapedTextIsAClassName() {
        Assertions.assertTrue(DirectoryPluginRuntimeManager.isClassName("org.example.Probe$Inner2"));
        Assertions.assertTrue(DirectoryPluginRuntimeManager.isClassName("org/example/Probe$Inner2"));
        Assertions.assertTrue(DirectoryPluginRuntimeManager.isClassName("Probe"));
        Assertions.assertTrue(DirectoryPluginRuntimeManager.isClassName("org.example." + new String(
                        new int[] {0xdc, 'n', 0xef, 'c', 'o', 'd', 'e'}, 0, 7)),
                "a Unicode identifier is a class name");
        Assertions.assertTrue(DirectoryPluginRuntimeManager.isClassName("org.example.Mangled-Name"),
                "a compiler-mangled name is a class name");
        Assertions.assertFalse(DirectoryPluginRuntimeManager.isClassName(""));
        Assertions.assertFalse(DirectoryPluginRuntimeManager.isClassName("com.example.Driver."));
        Assertions.assertFalse(DirectoryPluginRuntimeManager.isClassName(".com.example.Driver"));
        Assertions.assertFalse(DirectoryPluginRuntimeManager.isClassName("com..example.Driver"));
        Assertions.assertFalse(DirectoryPluginRuntimeManager.isClassName("Cannot load driver class com.x.Y"));
        Assertions.assertFalse(DirectoryPluginRuntimeManager.isClassName("[Lorg/example/Probe;"));
        Assertions.assertFalse(DirectoryPluginRuntimeManager.isClassName("org/example/Probe (wrong name: x/Y)"));
    }

    private static byte[] classBytes(Class<?> clazz) throws IOException {
        String entry = clazz.getName().replace('.', '/') + ".class";
        try (InputStream in = clazz.getClassLoader().getResourceAsStream(entry)) {
            Assertions.assertNotNull(in, "class bytes not found: " + entry);
            return in.readAllBytes();
        }
    }

    /**
     * Writes a plugin jar whose service file names {@code serviceClassName} and whose class entries are
     * exactly {@code entries} - which lets a test disagree with itself on purpose: an entry under one
     * path carrying another class's bytes, a class file with a patched version, or no class at all.
     */
    private static void writeJar(Path jarPath, String serviceClassName, Map<String, byte[]> entries)
            throws IOException {
        Files.createDirectories(jarPath.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue(
                DirectoryPluginRuntimeManagerMetadataTest.TEST_GATE.getManifestAttribute(),
                DirectoryPluginRuntimeManagerMetadataTest.TEST_GATE.getExpectedVersion());
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                jar.putNextEntry(new JarEntry(entry.getKey()));
                jar.write(entry.getValue());
                jar.closeEntry();
            }
            jar.putNextEntry(new JarEntry("META-INF/services/" + PluginFactory.class.getName()));
            jar.write((serviceClassName + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
    }

    private static String adviceFor(Throwable failure) {
        String advice = DirectoryPluginRuntimeManager.missingClassAdvice(failure);
        Assertions.assertTrue(advice.contains("shared library bundle"),
                () -> "the advice must say where a missing dependency is expected to come from: " + advice);
        return advice;
    }

    /**
     * Loads {@code root} against a parent that refuses {@link AbsentDependencyProbe}. The fabricated
     * jars leave that class out, so the plugin classloader cannot find it in its own jars either -
     * exactly the reachability a plugin has when the shared bundle carrying its dependency is not
     * installed.
     */
    private LoadReport<PluginFactory> load(Path root) {
        ClassLoader hidingParent = new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (AbsentDependencyProbe.class.getName().equals(name)) {
                    throw new ClassNotFoundException(name + " is withheld by this test");
                }
                return super.loadClass(name, resolve);
            }
        };
        return load(root, hidingParent);
    }

    private static LoadReport<PluginFactory> load(Path root, ClassLoader parent) {
        // Assertions.assertDoesNotThrow is the point of the test: before the loader caught LinkageError
        // this call threw, and nothing between here and FE startup would have caught it.
        return Assertions.assertDoesNotThrow(() -> new DirectoryPluginRuntimeManager<PluginFactory>().loadAll(
                Collections.singletonList(root),
                parent,
                PluginFactory.class,
                null,
                DirectoryPluginRuntimeManagerMetadataTest.TEST_GATE));
    }

    private static void assertReportsMissingDependency(LoadReport<PluginFactory> report) {
        Assertions.assertTrue(report.getSuccesses().isEmpty(), "the plugin cannot have loaded");
        Assertions.assertEquals(1, report.getFailures().size());

        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_INSTANTIATE, failure.getStage());
        Assertions.assertInstanceOf(LinkageError.class, failure.getCause(),
                () -> "expected the linkage failure to be preserved as the cause, got " + failure.getCause());
        Assertions.assertTrue(
                failure.getMessage().contains(AbsentDependencyProbe.class.getName().replace('.', '/')),
                () -> "the message must name the class that is missing: " + failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("shared library bundle"),
                () -> "the message must say where a missing dependency is expected to come from: "
                        + failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("; caused by java.lang.ClassNotFoundException: "),
                () -> "the message must carry the innermost cause: " + failure.getMessage());
    }
}
