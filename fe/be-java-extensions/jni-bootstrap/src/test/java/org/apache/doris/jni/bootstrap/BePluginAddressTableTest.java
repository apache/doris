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

package org.apache.doris.jni.bootstrap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@code (plugin, factory)} pairs in be/src/util/jni_plugin_registry.h are the ONLY thing BE
 * sends when it asks for a scanner, a writer or a UDF executor: {@link PluginRuntime} looks up a
 * directory by the first half and a factory by the second, and a pair naming something that does
 * not exist is a runtime error on the first query, never a compile error.
 *
 * <p>Nothing checked them. BE's own {@code PluginRefTableTest} asserts only that the strings carry
 * no slash, dot or space, and each plugin's {@code *PluginTest} asserts only its own literals - so
 * changing {@code plugin::JDBC_SCANNER} to {@code {"jdbc", "read"}} left every test green and every
 * JDBC query failing with "has no factory named 'read'". This is the check that spans the three
 * files the pair actually lives in:
 *
 * <ul>
 *   <li>be/src/util/jni_plugin_registry.h — what BE asks for;</li>
 *   <li>build.sh — which module is deployed under which plugin directory name;</li>
 *   <li>that module's {@code *Factory.NAME} — what answers.</li>
 * </ul>
 *
 * <p>Read as text rather than as classes on purpose: this module cannot depend on the plugins it
 * loads, and the C++ half is not on any classpath at all.
 */
public class BePluginAddressTableTest {

    private static final Pattern PLUGIN_REF = Pattern.compile(
            "inline\\s+constexpr\\s+PluginRef\\s+(\\w+)\\s*\\{\\s*\"([^\"]+)\"\\s*,\\s*\"([^\"]+)\"\\s*\\}");
    private static final Pattern PLUGIN_MODULE = Pattern.compile(
            "plugin_modules\\+?=\\(\"([^\":]+):([^\":]+)\"\\)");
    private static final Pattern FACTORY_NAME = Pattern.compile(
            "String\\s+NAME\\s*=\\s*\"([^\"]+)\"");
    /** The {@code const char* const NAME = "a/b/C"} table at the top of jni_plugin_registry.cpp. */
    private static final Pattern BE_CLASS_NAME = Pattern.compile(
            "const\\s+char\\*\\s+const\\s+(\\w+)\\s*=\\s*\"(org/[^\"]+)\"");
    /**
     * The same thing spelled inline: {@code env->FindClass("org/apache/doris/...")}. jni-util.cpp
     * and jni-util.h resolve two more Doris classes that way rather than through a named constant,
     * and both of those lines were rewritten by the package moves this table exists to guard.
     */
    private static final Pattern BE_INLINE_CLASS_NAME = Pattern.compile(
            "FindClass\\(\\s*\"(org/apache/doris/[^\"]+)\"\\s*\\)");
    /** The files those inline lookups live in, relative to the repository root. */
    private static final List<String> BE_INLINE_CLASS_SOURCES =
            List.of("be/src/util/jni-util.h", "be/src/util/jni-util.cpp");

    /**
     * Every {@code PluginRef} BE declares resolves to a factory some deployed plugin publishes.
     *
     * <p>Not exhaustive over everything BE can address, and the gap is by construction: the
     * {@code java-writer:local-file} pair reaches the loader as USER TEXT out of a TVF sink's
     * {@code writer_class} property rather than as a {@code PluginRef}, so it is not in the table
     * this reads. That one is covered end to end, with negative cases, by
     * {@code regression-test/suites/external_table_p0/tvf/insert/test_insert_into_local_tvf_jni.groovy}.
     */
    @Test
    public void everyPairBeAddressesResolvesToADeployedFactory() throws IOException {
        Path repo = repoRoot();
        Assumptions.assumeTrue(repo != null,
                "repository root not found from " + Path.of("").toAbsolutePath());

        Map<String, String> deployedNameToModule = pluginModules(repo);
        Assertions.assertFalse(deployedNameToModule.isEmpty(),
                "no plugin_modules entries parsed out of build.sh; the parser above has gone stale");

        List<String> broken = new ArrayList<>();
        Map<String, Set<String>> addressed = new TreeMap<>();
        for (Map.Entry<String, String> ref : pluginRefs(repo).entrySet()) {
            String symbol = ref.getKey();
            String plugin = ref.getValue().split("/", 2)[0];
            String factory = ref.getValue().split("/", 2)[1];
            addressed.computeIfAbsent(plugin, key -> new TreeSet<>()).add(factory);

            String module = deployedNameToModule.get(plugin);
            if (module == null) {
                broken.add(symbol + " names plugin directory '" + plugin + "', which build.sh never"
                        + " deploys; the deployed names are " + deployedNameToModule.keySet());
                continue;
            }
            Set<String> factories = factoryNames(repo, module);
            if (!factories.contains(factory)) {
                broken.add(symbol + " names factory '" + factory + "' in plugin '" + plugin
                        + "' (module " + module + "), whose factories are " + factories);
            }
        }
        Assertions.assertEquals(List.of(), broken,
                "a (plugin, factory) pair BE addresses has no answer on the Java side. Every one of"
                        + " these compiles and deploys; the first query using it fails with"
                        + " \"is not deployed\" or \"has no factory named\".");
    }

    /**
     * The invariant the header states in prose: within one plugin a factory name means one thing,
     * because BE sends a name and nothing saying which kind of factory it wanted.
     */
    @Test
    public void factoryNamesAreUniqueWithinAPlugin() throws IOException {
        Path repo = repoRoot();
        Assumptions.assumeTrue(repo != null);

        List<String> collisions = new ArrayList<>();
        for (Map.Entry<String, String> module : pluginModules(repo).entrySet()) {
            Set<String> seen = new HashSet<>();
            for (Path factory : factoryFiles(repo, module.getValue())) {
                String name = firstMatch(FACTORY_NAME, read(factory));
                if (name != null && !seen.add(name)) {
                    collisions.add("plugin '" + module.getKey() + "' declares the factory name '"
                            + name + "' twice; PluginRuntime resolves whichever list it searches first");
                }
            }
        }
        Assertions.assertEquals(List.of(), collisions);
    }

    /**
     * The five Java classes BE resolves by fully qualified name, checked against the tree.
     *
     * <p>Same shape of hole as the {@code (plugin, factory)} pairs and one level below them: these
     * are C++ string literals, so renaming {@code PluginRegistry} or moving {@code JniScanner} to
     * another package compiles on both sides and fails at {@code FindClass} on the first query -
     * for {@code PluginRegistry} that is every Java feature of the BE at once.
     *
     * <p>Two of the five are not named constants in jni_plugin_registry.cpp but inline
     * {@code FindClass} arguments in jni-util - {@code JniUtil} and {@code JNINativeMethod}, the
     * two classes {@code Util::_init_jni_base()} resolves. They matter more than the other three,
     * not less: a wrong name there fails the base, and a BE whose base failed runs no Java at all.
     * Both lines were rewritten by the package moves that made this table necessary.
     *
     * <p>Checks that the source file exists rather than loading the class: {@code JniScanner} and
     * {@code JniWriter} live in jni-spi, which this module has as a {@code provided} dependency, so
     * loading would prove they are on a test classpath and not that they are where BE will look.
     */
    @Test
    public void everyJavaClassBeResolvesByNameExists() throws IOException {
        Path repo = repoRoot();
        Assumptions.assumeTrue(repo != null);

        Map<String, String> classes = beClassNames(repo);
        Assertions.assertEquals(
                Set.of("REGISTRY_CLASS", "SCANNER_CLASS", "WRITER_CLASS",
                        "org/apache/doris/jni/spi/utils/JniUtil",
                        "org/apache/doris/jni/spi/utils/JNINativeMethod"),
                classes.keySet(),
                "the set of Java classes BE resolves by name changed; this test names them one by"
                        + " one so that a new one cannot be added without a source file to point at");

        List<String> missing = new ArrayList<>();
        for (Map.Entry<String, String> entry : classes.entrySet()) {
            Path source = sourceOf(repo, entry.getValue());
            if (source == null) {
                missing.add(entry.getKey() + " = \"" + entry.getValue() + "\" names no source file"
                        + " under fe/be-java-extensions/*/src/main/java");
            }
        }
        Assertions.assertEquals(List.of(), missing,
                "a Java class jni_plugin_registry.cpp resolves by name does not exist. This compiles"
                        + " on both sides and fails at FindClass on the first query that needs it.");
    }

    /** Symbol -&gt; binary class name, straight out of the C++ file. */
    /**
     * Every Doris class BE names as a string, keyed by how it is spelled: the constant's name for
     * the jni_plugin_registry.cpp table, the class name itself for the inline {@code FindClass}
     * calls, which have no constant to be named after.
     */
    private static Map<String, String> beClassNames(Path repo) throws IOException {
        Map<String, String> names = new LinkedHashMap<>();
        Matcher matcher = BE_CLASS_NAME.matcher(
                read(repo.resolve("be/src/util/jni_plugin_registry.cpp")));
        while (matcher.find()) {
            names.put(matcher.group(1), matcher.group(2));
        }
        Assertions.assertFalse(names.isEmpty(),
                "no class name constants parsed out of jni_plugin_registry.cpp; the parser above"
                        + " has gone stale");

        int inlineFound = 0;
        for (String source : BE_INLINE_CLASS_SOURCES) {
            Matcher inline = BE_INLINE_CLASS_NAME.matcher(read(repo.resolve(source)));
            while (inline.find()) {
                names.put(inline.group(1), inline.group(1));
                inlineFound++;
            }
        }
        Assertions.assertTrue(inlineFound >= 2,
                "no inline FindClass(\"org/apache/doris/...\") calls parsed out of "
                        + BE_INLINE_CLASS_SOURCES + "; either they moved to another file or the"
                        + " parser above has gone stale - both leave the JNI base unguarded");
        return names;
    }

    /** The .java file declaring {@code binaryName}, searched across every be-java-extensions module. */
    private static Path sourceOf(Path repo, String binaryName) throws IOException {
        String relative = binaryName.replace('$', '/') + ".java";
        Path extensions = repo.resolve("fe/be-java-extensions");
        if (!Files.isDirectory(extensions)) {
            return null;
        }
        try (java.util.stream.Stream<Path> modules = Files.list(extensions)) {
            for (Path module : (Iterable<Path>) modules.sorted()::iterator) {
                Path candidate = module.resolve("src/main/java").resolve(relative);
                if (Files.isRegularFile(candidate)) {
                    return candidate;
                }
            }
        }
        return null;
    }

    /** Symbol -&gt; "plugin/factory", straight out of the C++ table. */
    private static Map<String, String> pluginRefs(Path repo) throws IOException {
        Map<String, String> refs = new LinkedHashMap<>();
        Matcher matcher = PLUGIN_REF.matcher(read(repo.resolve("be/src/util/jni_plugin_registry.h")));
        while (matcher.find()) {
            refs.put(matcher.group(1), matcher.group(2) + "/" + matcher.group(3));
        }
        Assertions.assertFalse(refs.isEmpty(),
                "no PluginRef entries parsed out of jni_plugin_registry.h; the parser above has gone stale");
        return refs;
    }

    /** Deployed directory name -&gt; source module, straight out of build.sh. */
    private static Map<String, String> pluginModules(Path repo) throws IOException {
        Map<String, String> modules = new LinkedHashMap<>();
        Matcher matcher = PLUGIN_MODULE.matcher(read(repo.resolve("build.sh")));
        while (matcher.find()) {
            modules.put(matcher.group(2), matcher.group(1));
        }
        return modules;
    }

    private static Set<String> factoryNames(Path repo, String module) throws IOException {
        Set<String> names = new TreeSet<>();
        for (Path factory : factoryFiles(repo, module)) {
            String name = firstMatch(FACTORY_NAME, read(factory));
            if (name != null) {
                names.add(name);
            }
        }
        return names;
    }

    private static List<Path> factoryFiles(Path repo, String module) throws IOException {
        Path sources = repo.resolve("fe/be-java-extensions").resolve(module).resolve("src/main/java");
        if (!Files.isDirectory(sources)) {
            return List.of();
        }
        List<Path> factories = new ArrayList<>();
        try (java.util.stream.Stream<Path> walk = Files.walk(sources)) {
            walk.filter(path -> path.getFileName().toString().endsWith("Factory.java"))
                    .sorted()
                    .forEach(factories::add);
        }
        return factories;
    }

    private static String firstMatch(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        return matcher.find() ? matcher.group(1) : null;
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    /** Walks up from the working directory, which surefire sets to the module root. */
    private static Path repoRoot() {
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            if (Files.isRegularFile(dir.resolve("be/src/util/jni_plugin_registry.h"))
                    && Files.isRegularFile(dir.resolve("build.sh"))) {
                return dir;
            }
        }
        return null;
    }
}
