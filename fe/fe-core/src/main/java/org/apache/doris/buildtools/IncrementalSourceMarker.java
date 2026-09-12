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

package org.apache.doris.buildtools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Computes the set of sources which must be recompiled after a local edit and marks them for the
 * compiler plugin, so that {@code mvn compile} stops rebuilding the whole fe-core module.
 *
 * <p>Background: {@code maven-compiler-plugin} is all-or-nothing by default (one stale source
 * recompiles every source of the module), and its {@code useIncrementalCompilation=false} mode,
 * which does compile only the stale sources, is unsafe on its own because it never recompiles the
 * classes which <em>use</em> a changed class.
 *
 * <p>This tool closes that gap. For every scanned source file it caches, keyed by the content hash:
 * <ul>
 *     <li>the identifiers the file mentions, which over-approximates the types it depends on,</li>
 *     <li>the type names the file declares.</li>
 * </ul>
 * A run then computes
 * <pre>
 *     changed  = files whose content changed, plus files whose set of declared names changed
 *                (a type was added, removed, renamed or moved)
 *     affected = changed, closed over "mentions a type name declared by an affected file"
 * </pre>
 * and updates the last modification time of every affected file which belongs to the main sources.
 * The compiler plugin then compiles exactly that set. Because every file which mentions a changed
 * type is touched, no class is left referring to a type which was changed or deleted underneath it;
 * the set is a conservative superset, so the only cost of an imprecise name match is a few extra
 * files being recompiled.
 *
 * <p>The compiler plugin still owns the compilation, its annotation processing and its classpath.
 * This tool only decides which sources are stale.
 */
public class IncrementalSourceMarker {
    /** Bump it whenever the cached facts change, then the index is rebuilt from scratch. */
    private static final int FORMAT_VERSION = 1;

    /** The java keywords which introduce a type name. {@code interface} also covers {@code @interface}. */
    private static final Set<String> DECLARATION_KEYWORDS = new HashSet<>(
            Arrays.asList("class", "interface", "enum", "record"));

    /**
     * The reserved java keywords, which can never be a type name. They are dropped from the mentioned
     * identifiers to keep the index small and to avoid matching the keywords that would otherwise
     * appear in every file. Contextual keywords such as {@code record} or {@code var} are deliberately
     * not listed, they can be type names.
     */
    private static final Set<String> RESERVED_KEYWORDS = new HashSet<>(Arrays.asList(
            "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char", "class", "const",
            "continue", "default", "do", "double", "else", "enum", "extends", "final", "finally", "float",
            "for", "goto", "if", "implements", "import", "instanceof", "int", "interface", "long", "native",
            "new", "package", "private", "protected", "public", "return", "short", "static", "strictfp",
            "super", "switch", "synchronized", "this", "throw", "throws", "transient", "try", "void",
            "volatile", "while", "true", "false", "null", "_"));

    /** entry point, see the {@code fast-fe} profile of fe-core/pom.xml. */
    public static void main(String[] args) throws Exception {
        long startNanos = System.nanoTime();
        Map<String, String> options = parseOptions(args);
        List<File> sourceRoots = splitPaths(requireOption(options, "sources"));
        File touchRoot = new File(requireOption(options, "touch-root")).getCanonicalFile();
        File cacheFile = new File(requireOption(options, "cache"));
        File classesDir = options.containsKey("classes")
                ? new File(options.get("classes")).getCanonicalFile() : null;

        List<File> sourceFiles = findJavaFiles(sourceRoots);
        SourceIndex index = SourceIndex.load(cacheFile);

        Map<String, Facts> factsByPath = new LinkedHashMap<>();
        List<Facts> derivedSources = new ArrayList<>();
        Map<String, List<String>> declaredBy = new TreeMap<>();
        Map<String, List<String>> referrersBy = new TreeMap<>();
        Set<String> changed = new TreeSet<>();
        int scanned = 0;
        for (File sourceFile : sourceFiles) {
            String path = sourceFile.getAbsolutePath();
            byte[] contentHash = contentHash(sourceFile);
            Facts facts = index.get(path, contentHash);
            if (facts == null) {
                facts = scan(sourceFile, contentHash);
                index.put(path, facts);
                changed.add(path);
                scanned++;
            }
            factsByPath.put(path, facts);
            if (!isUnder(sourceFile, touchRoot)) {
                derivedSources.add(facts);
            }
            for (String declaredName : facts.declaredNames) {
                declaredBy.computeIfAbsent(declaredName, name -> new ArrayList<>()).add(path);
            }
            for (String identifier : facts.identifiers) {
                referrersBy.computeIfAbsent(identifier, name -> new ArrayList<>()).add(path);
            }
        }

        // A file which disappeared, or a type which is not declared by the same files as before
        // (added, removed, renamed, moved to another package), invalidates the files which mention
        // that name exactly like a content change does. A removed file itself has nothing left to
        // compile, only its classes have to go away, and its type names are already covered by the
        // names whose declaring files changed.
        Set<String> affected = new TreeSet<>();
        for (String staleName : index.replaceDeclaredNames(declaredBy)) {
            addReferrers(affected, referrersBy, staleName);
        }
        for (String removedPath : index.removeMissing(sourceFiles)) {
            removeClasses(new File(removedPath), touchRoot, classesDir);
        }
        affected.addAll(changed);

        // Propagate one hop only: the files which mention a type declared by a changed file. A file
        // which is recompiled without being edited keeps its own api, so there is no reason to go
        // further, and a transitive closure would quickly reach the whole module.
        //
        // The exception is a source derived from another one, such as the Immutables output of an
        // annotated class: it is regenerated when its input changes, so its api may change too, and
        // the files which use it need a recompilation as well.
        for (String path : changed) {
            Facts facts = factsByPath.get(path);
            if (facts == null) {
                continue;
            }
            for (String declaredName : facts.declaredNames) {
                addReferrers(affected, referrersBy, declaredName);
                for (Facts derived : derivedSources) {
                    if (derived.identifiers.contains(declaredName)) {
                        for (String derivedName : derived.declaredNames) {
                            addReferrers(affected, referrersBy, derivedName);
                        }
                    }
                }
            }
        }

        int marked = 0;
        FileTime now = FileTime.fromMillis(System.currentTimeMillis());
        for (String path : affected) {
            File file = new File(path);
            if (isUnder(file, touchRoot)) {
                Files.setLastModifiedTime(file.toPath(), now);
                marked++;
            }
        }

        index.save(cacheFile);
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        System.out.println("[incremental-sources] " + sourceFiles.size() + " sources, " + scanned
                + " scanned, " + changed.size() + " changed, " + marked + " marked for recompilation in "
                + elapsedMillis + " ms");
    }

    private static void addReferrers(Set<String> affected, Map<String, List<String>> referrersBy, String name) {
        List<String> referrers = referrersBy.get(name);
        if (referrers != null) {
            affected.addAll(referrers);
        }
    }

    /** delete the classes of a source file which does not exist anymore. */
    private static void removeClasses(File removedSource, File touchRoot, File classesDir) throws IOException {
        if (classesDir == null || !isUnder(removedSource, touchRoot)) {
            return;
        }
        String relative = touchRoot.toPath().relativize(removedSource.getCanonicalFile().toPath()).toString();
        String prefix = relative.substring(0, relative.length() - ".java".length());
        File parent = new File(classesDir, prefix).getParentFile();
        if (parent == null || !parent.isDirectory()) {
            return;
        }
        String simpleName = new File(prefix).getName();
        File[] candidates = parent.listFiles((dir, name) -> name.endsWith(".class")
                && (name.equals(simpleName + ".class") || name.startsWith(simpleName + "$")));
        if (candidates != null) {
            for (File candidate : candidates) {
                Files.deleteIfExists(candidate.toPath());
            }
        }
    }

    private static boolean isUnder(File file, File directory) {
        return file.getAbsolutePath().startsWith(directory.getAbsolutePath() + File.separator);
    }

    private static List<File> findJavaFiles(List<File> roots) {
        List<File> files = new ArrayList<>();
        for (File root : roots) {
            collectJavaFiles(root, files);
        }
        // sorted, so that the reported results and the persisted index are reproducible
        files.sort(Comparator.comparing(File::getAbsolutePath));
        return files;
    }

    private static void collectJavaFiles(File directory, List<File> files) {
        File[] children = directory.listFiles();
        if (children == null) {
            return;
        }
        for (File child : children) {
            if (child.isDirectory()) {
                collectJavaFiles(child, files);
            } else if (child.getName().endsWith(".java")) {
                files.add(child);
            }
        }
    }

    private static byte[] contentHash(File file) throws IOException {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not supported", e);
        }
        return messageDigest.digest(Files.readAllBytes(file.toPath()));
    }

    /**
     * Collect the identifiers a file mentions and the type names it declares.
     *
     * <p>Comments, string literals, character literals and text blocks are skipped so that prose and
     * data cannot look like a dependency. Mentioning every identifier over-approximates the real
     * dependency set, which keeps the result safe: an identifier which is not a type name simply
     * never matches a declared name.
     */
    private static Facts scan(File file, byte[] contentHash) throws IOException {
        char[] text = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8).toCharArray();
        Set<String> identifiers = new HashSet<>();
        Set<String> declaredNames = new HashSet<>();
        declaredNames.add(stripExtension(file.getName()));
        boolean expectDeclaredName = false;
        char lastSignificant = 0;
        int position = 0;
        int length = text.length;
        while (position < length) {
            char current = text[position];
            if (current == '/' && position + 1 < length && text[position + 1] == '/') {
                position += 2;
                while (position < length && text[position] != '\n') {
                    position++;
                }
                continue;
            }
            if (current == '/' && position + 1 < length && text[position + 1] == '*') {
                position += 2;
                while (position + 1 < length && !(text[position] == '*' && text[position + 1] == '/')) {
                    position++;
                }
                position += 2;
                continue;
            }
            if (current == '"') {
                if (position + 2 < length && text[position + 1] == '"' && text[position + 2] == '"') {
                    position += 3;
                    while (position + 2 < length
                            && !(text[position] == '"' && text[position + 1] == '"' && text[position + 2] == '"')) {
                        position++;
                    }
                    position += 3;
                } else {
                    position++;
                    while (position < length && text[position] != '"') {
                        position += text[position] == '\\' ? 2 : 1;
                    }
                    position++;
                }
                lastSignificant = '"';
                continue;
            }
            if (current == '\'') {
                position++;
                while (position < length && text[position] != '\'') {
                    position += text[position] == '\\' ? 2 : 1;
                }
                position++;
                lastSignificant = '\'';
                continue;
            }
            if (Character.isJavaIdentifierStart(current)) {
                int start = position;
                while (position < length && Character.isJavaIdentifierPart(text[position])) {
                    position++;
                }
                String identifier = new String(text, start, position - start);
                if (expectDeclaredName) {
                    declaredNames.add(identifier);
                    expectDeclaredName = false;
                } else if (DECLARATION_KEYWORDS.contains(identifier) && lastSignificant != '.') {
                    // "Foo.class" is a class literal, not a declaration
                    expectDeclaredName = true;
                }
                if (!RESERVED_KEYWORDS.contains(identifier)) {
                    identifiers.add(identifier);
                }
                lastSignificant = 'a';
                continue;
            }
            if (!Character.isWhitespace(current)) {
                lastSignificant = current;
            }
            position++;
        }
        return new Facts(contentHash, identifiers, declaredNames);
    }

    private static String stripExtension(String name) {
        return name.endsWith(".java") ? name.substring(0, name.length() - ".java".length()) : name;
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        for (String arg : args) {
            int separator = arg.startsWith("--") ? arg.indexOf('=') : -1;
            if (separator < 0) {
                throw new IllegalArgumentException("Illegal option: " + arg);
            }
            options.put(arg.substring(2, separator), arg.substring(separator + 1));
        }
        return options;
    }

    private static String requireOption(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required option --" + name + "=<value>");
        }
        return value;
    }

    private static List<File> splitPaths(String paths) {
        List<File> roots = new ArrayList<>();
        for (String path : paths.split(",")) {
            String trimmed = path.trim();
            if (!trimmed.isEmpty()) {
                roots.add(new File(trimmed));
            }
        }
        if (roots.isEmpty()) {
            throw new IllegalArgumentException("No directory in --sources=" + paths);
        }
        return roots;
    }

    /** the cached facts of one source file. */
    private static class Facts {
        private final byte[] contentHash;
        private final Set<String> identifiers;
        private final Set<String> declaredNames;

        Facts(byte[] contentHash, Set<String> identifiers, Set<String> declaredNames) {
            this.contentHash = contentHash;
            this.identifiers = identifiers;
            this.declaredNames = declaredNames;
        }
    }

    /**
     * The persistent per file index.
     *
     * <p>Identifiers go through a string table because one type name is mentioned by thousands of
     * files, which keeps the index a few megabytes instead of hundreds.
     */
    private static class SourceIndex {
        private final Map<String, Facts> entries;
        private final Map<String, Integer> stringIds;
        private final List<String> strings;
        private Map<String, Set<String>> declaredBy = new HashMap<>();

        private boolean dirty;

        SourceIndex(Map<String, Facts> entries, Map<String, Integer> stringIds, List<String> strings,
                Map<String, Set<String>> declaredBy) {
            this.entries = entries;
            this.stringIds = stringIds;
            this.strings = strings;
            this.declaredBy = declaredBy;
        }

        static SourceIndex load(File cacheFile) {
            if (!cacheFile.isFile()) {
                return empty();
            }
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(cacheFile.toPath())))) {
                if (in.readInt() != FORMAT_VERSION) {
                    return empty();
                }
                int stringCount = in.readInt();
                List<String> strings = new ArrayList<>(stringCount);
                Map<String, Integer> stringIds = new HashMap<>(Math.max(16, stringCount * 4 / 3));
                for (int i = 0; i < stringCount; i++) {
                    String value = in.readUTF();
                    strings.add(value);
                    stringIds.put(value, i);
                }
                int declaredNameCount = in.readInt();
                Map<String, Set<String>> declaredBy = new HashMap<>(Math.max(16, declaredNameCount * 4 / 3));
                for (int i = 0; i < declaredNameCount; i++) {
                    String name = in.readUTF();
                    int size = in.readInt();
                    Set<String> paths = new HashSet<>(Math.max(16, size * 4 / 3));
                    for (int j = 0; j < size; j++) {
                        paths.add(in.readUTF());
                    }
                    declaredBy.put(name, paths);
                }
                int fileCount = in.readInt();
                Map<String, Facts> entries = new HashMap<>(Math.max(16, fileCount * 4 / 3));
                for (int i = 0; i < fileCount; i++) {
                    String path = in.readUTF();
                    byte[] contentHash = new byte[in.readInt()];
                    in.readFully(contentHash);
                    Set<String> identifiers = readStrings(in, strings);
                    Set<String> declaredNames = readStrings(in, strings);
                    entries.put(path, new Facts(contentHash, identifiers, declaredNames));
                }
                return new SourceIndex(entries, stringIds, strings, declaredBy);
            } catch (IOException | RuntimeException e) {
                // A broken or outdated index must never break the build, just scan everything again.
                return empty();
            }
        }

        private static SourceIndex empty() {
            return new SourceIndex(new HashMap<>(), new HashMap<>(), new ArrayList<>(), new HashMap<>());
        }

        private static Set<String> readStrings(DataInputStream in, List<String> strings) throws IOException {
            int size = in.readInt();
            Set<String> values = new HashSet<>(Math.max(16, size * 4 / 3));
            for (int i = 0; i < size; i++) {
                values.add(strings.get(in.readInt()));
            }
            return values;
        }

        Facts get(String path, byte[] contentHash) {
            Facts facts = entries.get(path);
            if (facts == null || (contentHash != null && !Arrays.equals(facts.contentHash, contentHash))) {
                return null;
            }
            return facts;
        }

        void put(String path, Facts facts) {
            entries.put(path, facts);
            for (String value : facts.identifiers) {
                intern(value);
            }
            for (String value : facts.declaredNames) {
                intern(value);
            }
            dirty = true;
        }

        /**
         * Record the current "type name to declaring files" index and return the names whose declaring
         * files changed, i.e. the types which were added, removed, renamed or moved.
         */
        Set<String> replaceDeclaredNames(Map<String, List<String>> currentByPath) {
            Map<String, Set<String>> current = new HashMap<>(Math.max(16, currentByPath.size() * 4 / 3));
            for (Map.Entry<String, List<String>> entry : currentByPath.entrySet()) {
                current.put(entry.getKey(), new TreeSet<>(entry.getValue()));
            }
            Set<String> changedNames = new TreeSet<>();
            for (Map.Entry<String, Set<String>> entry : current.entrySet()) {
                Set<String> before = declaredBy.get(entry.getKey());
                if (before == null || !before.equals(entry.getValue())) {
                    changedNames.add(entry.getKey());
                }
            }
            for (String name : declaredBy.keySet()) {
                if (!current.containsKey(name)) {
                    changedNames.add(name);
                }
            }
            dirty |= !current.equals(declaredBy);
            declaredBy = current;
            return changedNames;
        }

        /** drop the entries of the files which do not exist anymore and return their paths. */
        Set<String> removeMissing(Collection<File> existingFiles) {
            Set<String> existingPaths = new TreeSet<>();
            for (File file : existingFiles) {
                existingPaths.add(file.getAbsolutePath());
            }
            Set<String> removed = new TreeSet<>(entries.keySet());
            removed.removeAll(existingPaths);
            if (!removed.isEmpty()) {
                entries.keySet().removeAll(removed);
                dirty = true;
            }
            return removed;
        }

        void save(File cacheFile) throws IOException {
            if (!dirty) {
                return;
            }
            File parent = cacheFile.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
                throw new IOException("Can not create directory " + parent);
            }
            try (DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(cacheFile.toPath())))) {
                out.writeInt(FORMAT_VERSION);
                out.writeInt(strings.size());
                for (String value : strings) {
                    out.writeUTF(value);
                }
                out.writeInt(declaredBy.size());
                for (Map.Entry<String, Set<String>> entry : declaredBy.entrySet()) {
                    out.writeUTF(entry.getKey());
                    out.writeInt(entry.getValue().size());
                    for (String path : entry.getValue()) {
                        out.writeUTF(path);
                    }
                }
                out.writeInt(entries.size());
                for (Map.Entry<String, Facts> entry : entries.entrySet()) {
                    out.writeUTF(entry.getKey());
                    out.writeInt(entry.getValue().contentHash.length);
                    out.write(entry.getValue().contentHash);
                    writeStrings(out, entry.getValue().identifiers);
                    writeStrings(out, entry.getValue().declaredNames);
                }
            }
        }

        private void writeStrings(DataOutputStream out, Set<String> values) throws IOException {
            out.writeInt(values.size());
            for (String value : values) {
                out.writeInt(intern(value));
            }
        }

        private int intern(String value) {
            Integer id = stringIds.get(value);
            if (id != null) {
                return id;
            }
            int newId = strings.size();
            strings.add(value);
            stringIds.put(value, newId);
            return newId;
        }
    }
}
