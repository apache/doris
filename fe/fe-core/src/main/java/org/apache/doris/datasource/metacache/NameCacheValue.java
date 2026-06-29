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
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable snapshot of remote/local names and the case-insensitive remote-name index.
 */
public final class NameCacheValue {
    private final ImmutableList<Pair<String, String>> names;
    private final ImmutableMap<String, String> lowerCaseToRemoteName;

    private NameCacheValue(List<Pair<String, String>> names) {
        this.names = ImmutableList.copyOf(names);
        ImmutableMap.Builder<String, String> indexBuilder = ImmutableMap.builder();
        for (Pair<String, String> pair : names) {
            indexBuilder.put(pair.key().toLowerCase(Locale.ROOT), pair.key());
        }
        lowerCaseToRemoteName = indexBuilder.build();
    }

    public static NameCacheValue of(List<Pair<String, String>> names) {
        return new NameCacheValue(Objects.requireNonNull(names, "names can not be null"));
    }

    public static NameCacheValue empty() {
        return of(ImmutableList.of());
    }

    public List<Pair<String, String>> names() {
        return names;
    }

    public String remoteNameOfLocalName(String localName) {
        for (Pair<String, String> pair : names) {
            if (pair.value().equals(localName)) {
                return pair.key();
            }
        }
        return null;
    }

    public boolean containsLocalName(String localName) {
        for (Pair<String, String> pair : names) {
            if (pair.value().equals(localName)) {
                return true;
            }
        }
        return false;
    }

    public String remoteNameForCaseInsensitiveLookup(String name) {
        return lowerCaseToRemoteName.get(name.toLowerCase(Locale.ROOT));
    }

    public NameCacheValue withName(String remoteName, String localName) {
        for (Pair<String, String> pair : names) {
            if (pair.key().equals(remoteName) && !pair.value().equals(localName)) {
                throw new IllegalArgumentException(
                        "remote name already maps to another local name: " + remoteName);
            }
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
}
