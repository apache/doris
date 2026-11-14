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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.stream.Stream;

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
    private static final int FORMAT_VERSION = 2;

    /** The java keywords which introduce a type name. {@code interface} also covers {@code @interface}. */
    private static final Set<String> DECLARATION_KEYWORDS = new HashSet<>(
            Arrays.asList("class", "interface", "enum", "record"));

    /** The keywords which introduce the supertypes of a type, i.e. what it inherits from. */
    private static final Set<String> SUPERTYPE_KEYWORDS = new HashSet<>(
            Arrays.asList("extends", "implements"));

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
        List<Path> sourceRoots = canonicalRoots(requireOption(options, "sources"));
        // Every path is derived from these canonical roots, so that a checkout reached through a
        // symlink (mktemp gives /var/... on macOS, whose canonical form is /private/var/...) yields
        // the same absolute paths here and in the index of the next run.
        Path touchRoot = canonical(Paths.get(requireOption(options, "touch-root")));
        Path cacheFile = Paths.get(requireOption(options, "cache"));
        Path classesDir = options.containsKey("classes")
                ? canonical(Paths.get(options.get("classes"))) : null;

        List<Path> sourceFiles = findJavaFiles(sourceRoots);
        SourceIndex index = SourceIndex.load(cacheFile);

        Map<String, Facts> factsByPath = new LinkedHashMap<>();
        List<Facts> derivedSources = new ArrayList<>();
        Map<String, List<String>> declaredBy = new TreeMap<>();
        Map<String, List<String>> referrersBy = new TreeMap<>();
        Map<String, List<String>> subtypesBySuperType = new TreeMap<>();
        Set<String> changed = new TreeSet<>();
        int scanned = 0;
        for (Path sourceFile : sourceFiles) {
            String path = sourceFile.toString();
            byte[] contentHash = contentHash(sourceFile);
            Facts facts = index.get(path, contentHash);
            if (facts == null) {
                facts = scan(sourceFile, contentHash);
                index.put(path, facts);
                changed.add(path);
                scanned++;
            }
            factsByPath.put(path, facts);
            if (!sourceFile.startsWith(touchRoot)) {
                derivedSources.add(facts);
            }
            for (String declaredName : facts.declaredNames) {
                declaredBy.computeIfAbsent(declaredName, name -> new ArrayList<>()).add(path);
            }
            for (String identifier : facts.identifiers) {
                referrersBy.computeIfAbsent(identifier, name -> new ArrayList<>()).add(path);
            }
            // A class inherits the members of its supertypes, so a change to a supertype reaches the
            // files which name one of its subtypes even though they never name the supertype itself
            // (B extends A, C uses B.VALUE: changing A.VALUE must recompile C as well, its class file
            // holds the inlined constant).
            for (String superTypeName : facts.superTypeNames) {
                subtypesBySuperType.computeIfAbsent(superTypeName, name -> new ArrayList<>())
                        .addAll(facts.declaredNames);
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
        for (Map.Entry<String, Facts> removed : index.removeMissing(sourceFiles).entrySet()) {
            removeClasses(Paths.get(removed.getKey()), touchRoot, classesDir, removed.getValue().topLevelNames);
        }
        affected.addAll(changed);

        // Propagate over the types a changed file declares, plus every subtype of them: a source
        // which is recompiled without being edited keeps its own declarations, but its effective api
        // still changes when one of its supertypes changes.
        //
        // The other exception is a source derived from another one, such as the Immutables output of
        // an annotated class: it is regenerated when its input changes, so its api may change too, and
        // the files which use it need a recompilation as well.
        Set<String> seedNames = new TreeSet<>();
        for (String path : changed) {
            Facts facts = factsByPath.get(path);
            if (facts == null) {
                continue;
            }
            for (String declaredName : facts.declaredNames) {
                collectSubtypes(declaredName, subtypesBySuperType, seedNames);
            }
        }
        for (String seedName : seedNames) {
            addReferrers(affected, referrersBy, seedName);
            for (Facts derived : derivedSources) {
                if (derived.identifiers.contains(seedName)) {
                    for (String derivedName : derived.declaredNames) {
                        addReferrers(affected, referrersBy, derivedName);
                    }
                }
            }
        }

        int marked = 0;
        FileTime now = FileTime.fromMillis(System.currentTimeMillis());
        for (String path : affected) {
            Path file = Paths.get(path);
            if (file.startsWith(touchRoot)) {
                Files.setLastModifiedTime(file, now);
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

    /** add {@code name} and every transitive subtype of it to {@code names}. */
    private static void collectSubtypes(String name, Map<String, List<String>> subtypesBySuperType,
            Set<String> names) {
        if (!names.add(name)) {
            return;
        }
        List<String> subtypes = subtypesBySuperType.get(name);
        if (subtypes == null) {
            return;
        }
        for (String subtype : subtypes) {
            collectSubtypes(subtype, subtypesBySuperType, names);
        }
    }

    /**
     * Delete the classes of a source file which does not exist anymore.
     *
     * <p>A single source file may declare several top level types (only one of them may be public and
     * carry the file name), so the classes to remove are derived from the declared names, not from the
     * file name: {@code Removed.java} with {@code class Removed} and package-private {@code class
     * Extra} produces {@code Removed.class} and {@code Extra.class}, and each type's nested classes
     * live under {@code <Type>$...class}.
     */
    private static void removeClasses(Path removedSource, Path touchRoot, Path classesDir,
            Collection<String> topLevelNames) throws IOException {
        if (classesDir == null || !removedSource.startsWith(touchRoot)) {
            return;
        }
        Path relative = touchRoot.relativize(removedSource);
        Path classDir = classesDir.resolve(relative).getParent();
        if (classDir == null || !Files.isDirectory(classDir)) {
            return;
        }
        Set<String> names = new TreeSet<>(topLevelNames);
        names.add(stripExtension(removedSource.getFileName().toString()));
        try (Stream<Path> candidates = Files.list(classDir)) {
            for (Path candidate : (Iterable<Path>) candidates::iterator) {
                String fileName = candidate.getFileName().toString();
                if (!fileName.endsWith(".class")) {
                    continue;
                }
                String simpleName = fileName.substring(0, fileName.length() - ".class".length());
                int nestedSeparator = simpleName.indexOf('$');
                String outerName = nestedSeparator < 0 ? simpleName : simpleName.substring(0, nestedSeparator);
                if (names.contains(outerName)) {
                    Files.deleteIfExists(candidate);
                }
            }
        }
    }

    private static Path canonical(Path path) throws IOException {
        return path.toRealPath();
    }

    private static List<Path> findJavaFiles(List<Path> roots) throws IOException {
        List<Path> files = new ArrayList<>();
        for (Path root : roots) {
            if (!Files.isDirectory(root)) {
                continue;
            }
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path file : (Iterable<Path>) walk::iterator) {
                    if (Files.isRegularFile(file) && file.getFileName().toString().endsWith(".java")) {
                        files.add(file);
                    }
                }
            }
        }
        // sorted, so that the reported results and the persisted index are reproducible
        files.sort(Comparator.comparing(Path::toString));
        return files;
    }

    private static byte[] contentHash(Path file) throws IOException {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not supported", e);
        }
        return messageDigest.digest(Files.readAllBytes(file));
    }

    /**
     * Collect the identifiers a file mentions, the type names it declares, the top level ones among
     * them, and the supertypes it extends or implements.
     *
     * <p>Comments, string literals, character literals and text blocks are skipped so that prose and
     * data cannot look like a dependency. Mentioning every identifier over-approximates the real
     * dependency set, which keeps the result safe: an identifier which is not a type name simply
     * never matches a declared name.
     */
    private static Facts scan(Path file, byte[] contentHash) throws IOException {
        char[] text = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).toCharArray();
        Set<String> identifiers = new HashSet<>();
        Set<String> declaredNames = new HashSet<>();
        Set<String> topLevelNames = new HashSet<>();
        Set<String> superTypeNames = new HashSet<>();
        declaredNames.add(stripExtension(file.getFileName().toString()));
        boolean expectDeclaredName = false;
        boolean inTypeHeader = false;
        boolean inSuperTypeClause = false;
        int braceDepth = 0;
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
                    if (braceDepth == 0) {
                        topLevelNames.add(identifier);
                    }
                    expectDeclaredName = false;
                } else if (DECLARATION_KEYWORDS.contains(identifier) && lastSignificant != '.') {
                    // "Foo.class" is a class literal, not a declaration
                    expectDeclaredName = true;
                    inTypeHeader = true;
                    inSuperTypeClause = false;
                } else if (inTypeHeader && SUPERTYPE_KEYWORDS.contains(identifier)) {
                    inSuperTypeClause = true;
                } else if (inSuperTypeClause) {
                    superTypeNames.add(identifier);
                }
                if (!RESERVED_KEYWORDS.contains(identifier)) {
                    identifiers.add(identifier);
                }
                lastSignificant = 'a';
                continue;
            }
            if (current == '{') {
                braceDepth++;
                inTypeHeader = false;
                inSuperTypeClause = false;
            } else if (current == '}') {
                braceDepth--;
            } else if (current == ';') {
                inTypeHeader = false;
                inSuperTypeClause = false;
            }
            if (!Character.isWhitespace(current)) {
                lastSignificant = current;
            }
            position++;
        }
        return new Facts(contentHash, identifiers, declaredNames, topLevelNames, superTypeNames);
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

    private static List<Path> splitPaths(String paths) {
        List<Path> roots = new ArrayList<>();
        for (String path : paths.split(",")) {
            String trimmed = path.trim();
            if (!trimmed.isEmpty()) {
                roots.add(Paths.get(trimmed));
            }
        }
        if (roots.isEmpty()) {
            throw new IllegalArgumentException("No directory in --sources=" + paths);
        }
        return roots;
    }

    /**
     * Split and canonicalize the source roots. Canonicalizing here (and not per file) keeps the walk
     * cheap while making every derived path stable across runs, which matters when the checkout is
     * reached through a symlink.
     */
    private static List<Path> canonicalRoots(String paths) throws IOException {
        List<Path> roots = new ArrayList<>();
        for (Path root : splitPaths(paths)) {
            if (Files.isDirectory(root)) {
                roots.add(canonical(root));
            }
        }
        return roots;
    }

    /** the cached facts of one source file. */
    private static class Facts {
        private final byte[] contentHash;
        /** every identifier the file mentions, an over-approximation of the types it depends on. */
        private final Set<String> identifiers;
        /** every type name the file declares, nested ones included. */
        private final Set<String> declaredNames;
        /** the types the file declares at the top level, which own the class files to clean up. */
        private final Set<String> topLevelNames;
        /** the types the file extends or implements. */
        private final Set<String> superTypeNames;

        Facts(byte[] contentHash, Set<String> identifiers, Set<String> declaredNames,
                Set<String> topLevelNames, Set<String> superTypeNames) {
            this.contentHash = contentHash;
            this.identifiers = identifiers;
            this.declaredNames = declaredNames;
            this.topLevelNames = topLevelNames;
            this.superTypeNames = superTypeNames;
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

        static SourceIndex load(Path cacheFile) {
            if (!Files.isRegularFile(cacheFile)) {
                return empty();
            }
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(cacheFile)))) {
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
                    Set<String> topLevelNames = readStrings(in, strings);
                    Set<String> superTypeNames = readStrings(in, strings);
                    entries.put(path, new Facts(contentHash, identifiers, declaredNames, topLevelNames,
                            superTypeNames));
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
            for (String value : facts.topLevelNames) {
                intern(value);
            }
            for (String value : facts.superTypeNames) {
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

        /**
         * Drop the entries of the files which do not exist anymore and return them, keyed by path, with
         * the facts recorded by the previous run so that their classes can be cleaned up.
         */
        Map<String, Facts> removeMissing(Collection<Path> existingFiles) {
            Set<String> existingPaths = new TreeSet<>();
            for (Path file : existingFiles) {
                existingPaths.add(file.toString());
            }
            Map<String, Facts> removed = new TreeMap<>();
            for (String path : entries.keySet()) {
                if (!existingPaths.contains(path)) {
                    removed.put(path, entries.get(path));
                }
            }
            if (!removed.isEmpty()) {
                entries.keySet().removeAll(removed.keySet());
                dirty = true;
            }
            return removed;
        }

        void save(Path cacheFile) throws IOException {
            if (!dirty) {
                return;
            }
            Path parent = cacheFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(cacheFile)))) {
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
                    writeStrings(out, entry.getValue().topLevelNames);
                    writeStrings(out, entry.getValue().superTypeNames);
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
