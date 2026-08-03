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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Immutable snapshot of remote/local names and the case-insensitive remote-name index.
 */
public final class NameCacheValue {
    private static final String CONFLICT_PREFIX = "Found conflicting external metadata name mapping: ";

    private final ImmutableList<Pair<String, String>> names;
    private final ImmutableSet<String> localNames;
    private final ImmutableMap<String, String> lowerCaseToRemoteName;
    private final ImmutableMap<String, String> localNameToRemoteName;

    private NameCacheValue(List<Pair<String, String>> names) {
        // One local name must identify exactly one remote object. Normalize identical pairs and reject ambiguous
        // mappings before publishing the immutable snapshot.
        Map<String, String> remoteToLocalName = new LinkedHashMap<>();
        Map<String, String> localToRemoteName = new LinkedHashMap<>();
        ImmutableList.Builder<Pair<String, String>> namesBuilder = ImmutableList.builder();
        for (Pair<String, String> pair : names) {
            String remoteName = Objects.requireNonNull(pair.key(), "remote name can not be null");
            String localName = Objects.requireNonNull(pair.value(), "local name can not be null");
            String existingLocalName = remoteToLocalName.get(remoteName);
            if (existingLocalName != null && !existingLocalName.equals(localName)) {
                throw new IllegalArgumentException(String.format(
                        CONFLICT_PREFIX + "remote name '%s' maps to local names '%s' and '%s'",
                        remoteName, existingLocalName, localName));
            }
            String existingRemoteName = localToRemoteName.get(localName);
            if (existingRemoteName != null) {
                if (!existingRemoteName.equals(remoteName)) {
                    throw new IllegalArgumentException(String.format(
                            CONFLICT_PREFIX + "local name '%s' maps to remote names '%s' and '%s'",
                            localName, existingRemoteName, remoteName));
                }
                // Treat an identical pair as an idempotent duplicate.
                continue;
            }
            remoteToLocalName.put(remoteName, localName);
            localToRemoteName.put(localName, remoteName);
            // Deep-copy each pair so callers cannot mutate the snapshot through reused Pair instances.
            namesBuilder.add(Pair.of(remoteName, localName));
        }
        this.names = namesBuilder.build();
        // Build the lower-case index with last-write-wins semantics so the snapshot does not
        // introduce case-conflict validation beyond what the catalog-aware loader already enforces.
        Map<String, String> indexBuilder = new java.util.HashMap<>();
        ImmutableSet.Builder<String> localNamesBuilder = ImmutableSet.builder();
        for (Pair<String, String> pair : this.names) {
            indexBuilder.put(pair.key().toLowerCase(Locale.ROOT), pair.key());
            localNamesBuilder.add(pair.value());
        }
        localNames = localNamesBuilder.build();
        lowerCaseToRemoteName = ImmutableMap.copyOf(indexBuilder);
        localNameToRemoteName = ImmutableMap.copyOf(localToRemoteName);
    }

    public static NameCacheValue of(List<Pair<String, String>> names) {
        return new NameCacheValue(Objects.requireNonNull(names, "names can not be null"));
    }

    public static NameCacheValue empty() {
        return of(ImmutableList.of());
    }

    public List<Pair<String, String>> names() {
        // Return fresh Pair objects even though the list itself is immutable, so callers cannot
        // mutate the published snapshot through reused Pair instances.
        return ImmutableList.copyOf(copyPairs(names));
    }

    public List<String> localNames() {
        return localNames.asList();
    }

    public String remoteNameOfLocalName(String localName) {
        return localNameToRemoteName.get(localName);
    }

    public boolean containsLocalName(String localName) {
        return localNames.contains(localName);
    }

    public String remoteNameForCaseInsensitiveLookup(String name) {
        return lowerCaseToRemoteName.get(name.toLowerCase(Locale.ROOT));
    }

    public NameCacheValue withName(String remoteName, String localName) {
        boolean existingMapping = false;
        for (Pair<String, String> pair : names) {
            if (pair.key().equals(remoteName)) {
                if (!pair.value().equals(localName)) {
                    throw new IllegalArgumentException(
                            "remote name already maps to another local name: " + remoteName);
                }
                existingMapping = true;
            }
        }
        if (existingMapping) {
            return this;
        }
        // Copy-on-write keeps readers on a stable snapshot while publishing a new value atomically.
        List<Pair<String, String>> copy = Lists.newArrayList(names);
        copy.removeIf(pair -> pair.value().equals(localName));
        copy.add(Pair.of(remoteName, localName));
        return of(copy);
    }

    public NameCacheValue withoutLocalName(String localName) {
        // Copy-on-write keeps readers on a stable snapshot while publishing a new value atomically.
        List<Pair<String, String>> copy = Lists.newArrayList(names);
        copy.removeIf(pair -> pair.value().equals(localName));
        return of(copy);
    }

    private static List<Pair<String, String>> copyPairs(List<Pair<String, String>> names) {
        return names.stream()
                .map(pair -> Pair.of(pair.key(), pair.value()))
                .collect(Collectors.toList());
    }
}
