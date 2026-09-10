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
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A library layer shared by every plugin classloader of every family.
 *
 * <p>Some libraries cannot be bundled per plugin. A library whose classes inherit from one another
 * across jars has to be loaded once or the JVM refuses the link; one that holds process-wide static
 * state (a client cache, a login context, a first-caller-wins registry) has to be loaded once or two
 * plugins silently stop sharing it; one with a JNI native image can only be bound to a single
 * classloader per process. Hadoop is all three at once. Giving each plugin its own copy is therefore
 * not "the same thing, duplicated" - it changes behavior.
 *
 * <p>So such libraries are installed once, under a shared root, and this class turns that root into
 * a single classloader that is then the PARENT of every plugin classloader:
 *
 * <pre>
 *   app classloader (fe/lib)
 *     └── shared library layer      &lt;- plugins/shared/&lt;bundle&gt;/*.jar + lib/*.jar
 *           ├── filesystem plugin   (child-first)
 *           ├── connector plugin    (child-first)
 *           └── ...
 * </pre>
 *
 * <p>The layer is itself child-first with only the mandatory parent-first prefixes, so a bundle uses
 * its own dependencies where it has them and falls back to fe/lib otherwise; logging stays the
 * kernel's, which keeps the bundle's output in fe.log. Plugins reach the bundle by the ordinary
 * child-first fallback: a plugin that carries its own copy of the library keeps using it and is
 * simply not sharing, which is the pre-existing behavior for a third-party plugin.
 *
 * <h2>Layout</h2>
 *
 * <p>{@code <root>/<bundle>/*.jar} then {@code <root>/<bundle>/lib/*.jar}, bundles in name order -
 * the same convention {@link DirectoryPluginRuntimeManager} uses for a plugin directory. Root jars
 * before {@code lib/} is what lets a bundle ship a patched copy of a class that also exists in one
 * of its dependency jars: put the patch in the bundle root and the stock jar under {@code lib/}.
 *
 * <h2>Absence is normal</h2>
 *
 * <p>A root that does not exist, or holds no jar, yields {@code parent} unchanged, so a deployment
 * that installs no bundle keeps exactly the classloader graph it had before this class existed. An
 * unreadable root is not that case and is raised, because degrading it to "nothing installed" turns
 * a permissions mistake into a missing-class failure much later and somewhere else.
 *
 * <h2>One instance per root</h2>
 *
 * <p>Memoized on the <em>physical</em> root - symlinks resolved - for the whole process. This is the
 * point of the class rather than an optimization: two callers that got two layers over the same jars
 * would get two copies of every class in them, which is the situation the layer exists to prevent,
 * and a release directory reached both through a {@code current} symlink and through its real path is
 * the same jars under two names. The first caller's parent is the one that ends up in the graph -
 * there is one FE app classloader, so every caller passes the same one. The layer is never closed; it
 * lives as long as the process, like the app classloader.
 */
public final class SharedLibraryLayer {

    private static final ConcurrentMap<Path, ClassLoader> LAYERS = new ConcurrentHashMap<>();

    private SharedLibraryLayer() {
    }

    /**
     * Returns the classloader for the shared bundles under {@code root}, or {@code parent} itself
     * when there is nothing to install there.
     *
     * @param root the shared library root; null is treated as "not configured"
     * @param parent the classloader the layer delegates to, normally the FE app classloader
     * @throws UncheckedIOException if the root exists but cannot be read
     */
    public static ClassLoader resolve(Path root, ClassLoader parent) {
        Objects.requireNonNull(parent, "parent");
        if (root == null) {
            return parent;
        }
        Path physical = physicalRoot(root);
        if (physical == null) {
            return parent;
        }
        // A mapping function that throws leaves the map untouched, so an unreadable root is raised on
        // every call rather than memoized as "nothing installed" until the process is restarted.
        return LAYERS.computeIfAbsent(physical, dir -> build(dir, parent));
    }

    /**
     * The physical root, or {@code null} when nothing is installed there. {@code toRealPath} rather
     * than {@code toAbsolutePath().normalize()}: the latter removes lexical {@code ..} but leaves
     * symlinks in place, and two spellings of one directory memoize twice.
     */
    private static Path physicalRoot(Path root) {
        try {
            return root.toRealPath();
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to resolve shared library root: " + root, e);
        }
    }

    private static ClassLoader build(Path root, ClassLoader parent) {
        if (!isDirectory(root)) {
            return parent;
        }
        List<Path> jars = collectBundleJars(root);
        if (jars.isEmpty()) {
            return parent;
        }
        URL[] urls = new URL[jars.size()];
        for (int i = 0; i < jars.size(); i++) {
            try {
                urls[i] = jars.get(i).toUri().toURL();
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Invalid shared library jar path: " + jars.get(i), e);
            }
        }
        // Mandatory prefixes only: the layer adds no business parent-first rules of its own. A bundle
        // is a library, not a plugin, so nothing here participates in an SPI contract with the kernel.
        return new PluginLoader(ClassLoadingPolicy.defaultPolicy().toParentFirstPackages())
                .createClassLoader(urls, parent);
    }

    /** Bundle directories in name order; within each, root jars before {@code lib/} jars. */
    private static List<Path> collectBundleJars(Path root) {
        List<Path> jars = new ArrayList<>();
        for (Path bundle : listSorted(root, SharedLibraryLayer::isDirectory)) {
            jars.addAll(listSorted(bundle, SharedLibraryLayer::isJar));
            Path lib = bundle.resolve("lib");
            if (isDirectory(lib)) {
                jars.addAll(listSorted(lib, SharedLibraryLayer::isJar));
            }
        }
        return jars;
    }

    private static List<Path> listSorted(Path dir, Predicate<Path> filter) {
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(filter)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list shared library directory: " + dir, e);
        }
    }

    private static boolean isDirectory(Path path) {
        return is(path, BasicFileAttributes::isDirectory);
    }

    private static boolean isJar(Path path) {
        return path.getFileName().toString().endsWith(".jar") && is(path, BasicFileAttributes::isRegularFile);
    }

    /**
     * Whether {@code path} is there and of the given kind. Not {@code Files.isDirectory} /
     * {@code Files.isRegularFile}: those answer {@code false} when the attributes cannot be read at
     * all, so a bundle directory that can be listed but not searched - readable, not executable -
     * would have every jar in it quietly filtered out and the layer would degrade to a no-op, which
     * is the fail-quiet this class exists to avoid. Absence stays absence; an I/O failure is raised.
     */
    private static boolean is(Path path, Predicate<BasicFileAttributes> kind) {
        try {
            return kind.test(Files.readAttributes(path, BasicFileAttributes.class));
        } catch (NoSuchFileException e) {
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read shared library entry: " + path, e);
        }
    }
}
