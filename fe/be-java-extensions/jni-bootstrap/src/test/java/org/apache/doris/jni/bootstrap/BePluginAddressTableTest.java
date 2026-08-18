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
