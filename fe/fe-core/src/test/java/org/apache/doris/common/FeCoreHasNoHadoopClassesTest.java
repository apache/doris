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

package org.apache.doris.common;

import org.apache.doris.catalog.Env;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The FE kernel must carry no Hadoop.
 *
 * <p>Hadoop belongs to the filesystem and connector plugins, which load it through their own
 * classloaders; fe-core itself must not reference a single {@code org.apache.hadoop} type. Removing
 * the jars from {@code fe/lib} is only half of that — the other half is that no fe-core class names
 * one, because javac needs nothing but the class on the compile classpath and a reference that
 * compiles today keeps compiling after the jar leaves, failing instead at runtime with
 * {@code NoClassDefFoundError} on whichever code path first reaches it.
 *
 * <p>This scans fe-core's own compiled output rather than its source: a constant-pool entry is the
 * form a dependency actually takes, so it catches the reference no import reveals — a supertype
 * inherited from another module, a type that only appears in a method descriptor, a synthetic
 * bridge.
 *
 * <p>Both spellings are needles. JVM internal form ({@code org/apache/hadoop/}) is how class
 * references and field/method descriptors are written, and covers everything javac links. The dotted
 * form is how a class name reaches {@code Class.forName} or {@code loadClass}, and a reflective load
 * compiles against nothing at all: it would pass an internal-form-only scan and then fail at runtime,
 * on whichever code path first reaches it, once the kernel jars are gone. fe-core has reflective
 * loading paths, so the dotted spelling has to be a needle too.
 *
 * <p>{@link #POLICY_OWNERS} is the exception, and the only one: two classes hold
 * {@code "org.apache.hadoop."} as a parent-first classloader-policy string, which is a statement
 * about where plugins load Hadoop from rather than a use of it. They are allowlisted by name and the
 * test asserts each of them still carries the string, so an allowlist entry cannot outlive the reason
 * for it.
 *
 * <p>{@code org/apache/doris/kerberos/} is a needle too. That module splits into a Hadoop half
 * (UGI logins, {@code AuthenticationConfig(Configuration)}) and a Hadoop-free remainder, and its
 * Hadoop half would sit in {@code fe/lib} as classes that can never link there. fe-core takes the
 * one interface it needs, {@code ExecutionAuthenticator}, from fe-foundation instead, and
 * {@code fe-core/pom.xml} no longer depends on fe-kerberos at all.
 */
public class FeCoreHasNoHadoopClassesTest {

    /** JVM internal form: how class references and descriptors are spelled in the constant pool. */
    private static final List<String> FORBIDDEN = Arrays.asList(
            "org/apache/hadoop/",
            "org/apache/doris/kerberos/");

    /** Dotted form: how a class name is spelled when it is loaded reflectively. */
    private static final List<String> FORBIDDEN_DOTTED = Arrays.asList(
            "org.apache.hadoop.",
            "org.apache.doris.kerberos.");

    /**
     * The classes allowed to name a forbidden package in dotted form, because naming it is their job:
     * both hold {@code "org.apache.hadoop."} in the parent-first prefix list they hand their plugin
     * classloaders. Exact class files, so a policy string that migrates into a lambda or a helper has
     * to be re-approved rather than inherited.
     */
    private static final Set<String> POLICY_OWNERS = new LinkedHashSet<>(Arrays.asList(
            "org/apache/doris/connector/ConnectorPluginManager.class",
            "org/apache/doris/fs/FileSystemPluginManager.class"));

    /**
     * Present in every build, so finding it proves the scan reads real constant pools instead of
     * quietly matching nothing.
     */
    private static final String POSITIVE_CONTROL = "org/apache/doris/catalog/Env";

    @Test
    public void feCoreBytecodeReferencesNoHadoop() throws IOException {
        Path classesRoot = feCoreClassesRoot();
        List<String> offenders = new ArrayList<>();
        Set<String> policyOwnersSeen = new TreeSet<>();
        boolean controlSeen = false;
        int scanned = 0;

        try (Stream<Path> tree = Files.walk(classesRoot)) {
            List<Path> classFiles = tree.filter(p -> p.toString().endsWith(".class"))
                    .collect(Collectors.toList());
            for (Path classFile : classFiles) {
                scanned++;
                String name = classesRoot.relativize(classFile).toString().replace(File.separatorChar, '/');
                // ISO-8859-1 is byte-preserving, so this is a byte search spelled as a String search.
                String bytecode = new String(Files.readAllBytes(classFile), StandardCharsets.ISO_8859_1);
                controlSeen |= bytecode.contains(POSITIVE_CONTROL);
                boolean policyOwner = POLICY_OWNERS.contains(name);
                String dotted = firstMatch(bytecode, FORBIDDEN_DOTTED);
                if (policyOwner && dotted != null) {
                    policyOwnersSeen.add(name);
                }
                // A policy owner is excused for the dotted spelling only; linking Hadoop is not part
                // of stating where plugins load it from.
                String hit = firstMatch(bytecode, FORBIDDEN);
                if (hit == null && !policyOwner) {
                    hit = dotted;
                }
                if (hit != null) {
                    offenders.add(name + "  ->  " + hit);
                }
            }
        }

        Assertions.assertTrue(scanned > 1000,
                "expected fe-core to compile into thousands of classes, scanned only " + scanned
                        + " under " + classesRoot + "; the scan is looking at the wrong directory");
        Assertions.assertTrue(controlSeen,
                "the positive control " + POSITIVE_CONTROL + " was not found in any of the " + scanned
                        + " classes scanned, so this test cannot detect anything");
        // Doubles as the positive control for the dotted needles: if these two stopped matching, the
        // dotted half of the scan would be silently matching nothing at all.
        Assertions.assertEquals(POLICY_OWNERS, policyOwnersSeen,
                "every allowlisted class must still hold a dotted forbidden prefix; one that does not "
                        + "is either renamed or no longer a classloader-policy owner, and its "
                        + "allowlist entry has to go with it");
        Assertions.assertTrue(offenders.isEmpty(),
                "fe-core must not reference Hadoop (or fe-kerberos, whose Hadoop half cannot link in "
                        + "fe/lib), in either linked or reflective form. " + offenders.size() + " of "
                        + scanned + " classes do:\n"
                        + String.join("\n", offenders.subList(0, Math.min(offenders.size(), 20))));
    }

    /** The first needle present in {@code bytecode}, or null. */
    private static String firstMatch(String bytecode, List<String> needles) {
        for (String needle : needles) {
            if (bytecode.contains(needle)) {
                return needle;
            }
        }
        return null;
    }

    /**
     * fe-core's compiled output, located through a class that lives in it. Reading it off the
     * classloader rather than a hardcoded {@code target/classes} keeps the test correct under any
     * build layout that still hands surefire a directory.
     */
    private static Path feCoreClassesRoot() {
        Path root;
        try {
            root = Paths.get(Env.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (Exception e) {
            throw new IllegalStateException("cannot locate fe-core's compiled classes", e);
        }
        Assertions.assertTrue(Files.isDirectory(root),
                "expected fe-core's classes as a directory, got " + root
                        + "; run this from the maven build, not against a packaged jar");
        return root;
    }
}
