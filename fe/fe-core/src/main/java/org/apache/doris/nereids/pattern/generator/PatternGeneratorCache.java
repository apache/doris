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

package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.TypeDeclaration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Persistent cache of the parsed ast of every scanned source file.
 *
 * <p>Parsing the whole nereids source tree with antlr on every build is the expensive part of the
 * pattern generation. Here only the content hash is computed for every scanned file, and the antlr
 * parser runs just for the files which were actually modified, added or removed.
 */
public class PatternGeneratorCache {
    /** Bump it whenever the encoded ast layout changes, then the cache is rebuilt from scratch. */
    private static final int FORMAT_VERSION = 1;

    private final Map<String, Entry> entries;

    private boolean dirty;

    private PatternGeneratorCache(Map<String, Entry> entries) {
        this.entries = entries;
    }

    /** load the cache, or return an empty one when it is missing or is not usable. */
    public static PatternGeneratorCache load(File cacheFile) {
        if (!cacheFile.isFile()) {
            return new PatternGeneratorCache(new HashMap<>());
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(cacheFile.toPath())))) {
            if (in.readInt() != FORMAT_VERSION) {
                return new PatternGeneratorCache(new HashMap<>());
            }
            int size = in.readInt();
            Map<String, Entry> entries = new HashMap<>(Math.max(16, size * 4 / 3));
            for (int i = 0; i < size; i++) {
                String path = in.readUTF();
                byte[] contentHash = new byte[in.readInt()];
                in.readFully(contentHash);
                entries.put(path, new Entry(contentHash, JavaAstCodec.readDeclarations(in)));
            }
            return new PatternGeneratorCache(entries);
        } catch (IOException | RuntimeException e) {
            // A broken or outdated cache must never break the build, just parse everything again.
            return new PatternGeneratorCache(new HashMap<>());
        }
    }

    /** compute the content hash of a file. */
    public static byte[] contentHash(File file) throws IOException {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not supported", e);
        }
        return messageDigest.digest(Files.readAllBytes(file.toPath()));
    }

    /** return the cached ast of the file, or null when the content changed. */
    public List<TypeDeclaration> getDeclarations(String path, byte[] contentHash) {
        Entry entry = entries.get(path);
        if (entry == null || !Arrays.equals(entry.contentHash, contentHash)) {
            return null;
        }
        return entry.declarations;
    }

    /** store the ast of the file. */
    public void putDeclarations(String path, byte[] contentHash, List<TypeDeclaration> declarations) {
        entries.put(path, new Entry(contentHash, declarations));
        dirty = true;
    }

    /** drop the entries of the files which do not exist anymore. */
    public void retain(Collection<File> existingFiles) {
        Set<String> existingPaths = new TreeSet<>();
        for (File file : existingFiles) {
            existingPaths.add(file.getAbsolutePath());
        }
        dirty |= entries.keySet().retainAll(existingPaths);
    }

    /** write the cache back to the disk, only when it changed. */
    public void save(File cacheFile) throws IOException {
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
            out.writeInt(entries.size());
            // sorted, so that the cache is a deterministic function of its inputs
            for (String path : new TreeSet<>(entries.keySet())) {
                Entry entry = entries.get(path);
                out.writeUTF(path);
                out.writeInt(entry.contentHash.length);
                out.write(entry.contentHash);
                JavaAstCodec.writeDeclarations(out, entry.declarations);
            }
        }
    }

    private static class Entry {
        private final byte[] contentHash;
        private final List<TypeDeclaration> declarations;

        Entry(byte[] contentHash, List<TypeDeclaration> declarations) {
            this.contentHash = contentHash;
            this.declarations = declarations;
        }
    }
}
