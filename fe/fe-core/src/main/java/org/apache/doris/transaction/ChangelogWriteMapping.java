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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Column;
import org.apache.doris.thrift.TChangelogIndexWriteSnapshot;
import org.apache.doris.thrift.TChangelogWriteColumnMapping;
import org.apache.doris.thrift.TChangelogWriteColumnMappings;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Immutable UID relationships captured from the schema used to plan a write. */
public final class ChangelogWriteMapping {
    private static final class Entry {
        @SerializedName("source")
        private final int source;
        @SerializedName("current")
        private final int current;
        @SerializedName("before")
        private final Integer before;

        private Entry(int source, int current, Integer before) {
            this.source = source;
            this.current = current;
            this.before = before;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Entry)) {
                return false;
            }
            Entry entry = (Entry) other;
            return source == entry.source && current == entry.current && Objects.equals(before, entry.before);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, current, before);
        }
    }

    @SerializedName("historical")
    private final boolean needHistoricalValue;
    @SerializedName("entries")
    private final List<Entry> entries;

    private ChangelogWriteMapping(boolean historical, List<Entry> entries) {
        Set<Integer> sources = new HashSet<>();
        Set<Integer> targets = new HashSet<>();
        for (Entry entry : entries) {
            Preconditions.checkArgument(sources.add(entry.source), "Duplicate source UID: %s", entry.source);
            Preconditions.checkArgument(targets.add(entry.current), "Duplicate target UID: %s", entry.current);
            if (entry.before != null) {
                Preconditions.checkArgument(historical, "Before UID requires historical values");
                Preconditions.checkArgument(targets.add(entry.before), "Duplicate target UID: %s", entry.before);
            }
        }
        List<Entry> sorted = new ArrayList<>(entries);
        sorted.sort(Comparator.comparingInt(entry -> entry.source));
        this.entries = ImmutableList.copyOf(sorted);
        this.needHistoricalValue = historical;
    }

    public static ChangelogWriteMapping fromColumns(List<Column> sourceColumns, List<Column> changelogColumns,
            boolean historical) {
        Map<String, Column> targets = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : changelogColumns) {
            Preconditions.checkState(targets.put(column.getNonShadowName(), column) == null,
                    "Duplicate changelog column: %s", column.getName());
        }
        List<Entry> entries = new ArrayList<>();
        for (Column source : sourceColumns) {
            if (!source.isVisible() && !source.isKey()) {
                continue;
            }
            Column current = targets.get(source.getNonShadowName());
            Preconditions.checkState(current != null, "Missing current column: %s", source.getName());
            Integer beforeUid = null;
            if (historical && !source.isKey()) {
                Column before = targets.get(Column.generateBeforeColName(source.getNonShadowName()));
                Preconditions.checkState(before != null, "Missing before column: %s", source.getName());
                beforeUid = before.getUniqueId();
            }
            entries.add(new Entry(source.getUniqueId(), current.getUniqueId(), beforeUid));
        }
        return new ChangelogWriteMapping(historical, entries);
    }

    public static ChangelogWriteMapping fromThrift(TChangelogWriteColumnMappings mappings) {
        Preconditions.checkArgument(mappings != null && mappings.isSetEntries()
                && mappings.isSetNeedHistoricalValue(), "Changelog mapping requires entries and historical flag");
        List<Entry> entries = new ArrayList<>();
        for (TChangelogWriteColumnMapping entry : mappings.getEntries()) {
            Preconditions.checkArgument(entry != null && entry.isSetSourceColumnUniqueId()
                    && entry.isSetCurrentColumnUniqueId(), "Changelog entry requires source and current UID");
            entries.add(new Entry(entry.getSourceColumnUniqueId(), entry.getCurrentColumnUniqueId(),
                    entry.isSetBeforeColumnUniqueId() ? entry.getBeforeColumnUniqueId() : null));
        }
        return new ChangelogWriteMapping(mappings.isNeedHistoricalValue(), entries);
    }

    public TChangelogWriteColumnMappings toThrift() {
        List<TChangelogWriteColumnMapping> result = new ArrayList<>();
        for (Entry entry : entries) {
            TChangelogWriteColumnMapping mapping = new TChangelogWriteColumnMapping()
                    .setSourceColumnUniqueId(entry.source).setCurrentColumnUniqueId(entry.current);
            if (entry.before != null) {
                mapping.setBeforeColumnUniqueId(entry.before);
            }
            result.add(mapping);
        }
        return new TChangelogWriteColumnMappings().setEntries(result).setNeedHistoricalValue(needHistoricalValue);
    }

    public static List<TChangelogIndexWriteSnapshot> toThrift(Map<Long, ChangelogWriteMapping> mappings) {
        List<TChangelogIndexWriteSnapshot> result = new ArrayList<>();
        new TreeMap<>(mappings).forEach((indexId, mapping) -> result.add(new TChangelogIndexWriteSnapshot()
                .setSourceIndexId(indexId).setMappings(mapping.toThrift())));
        return result;
    }

    public static Map<Long, ChangelogWriteMapping> fromThrift(List<TChangelogIndexWriteSnapshot> snapshots) {
        Preconditions.checkArgument(snapshots != null, "Missing changelog index snapshots");
        Map<Long, ChangelogWriteMapping> result = new TreeMap<>();
        for (TChangelogIndexWriteSnapshot snapshot : snapshots) {
            Preconditions.checkArgument(snapshot != null && snapshot.isSetSourceIndexId() && snapshot.isSetMappings(),
                    "Changelog snapshot requires source index and mappings");
            Preconditions.checkArgument(result.put(snapshot.getSourceIndexId(), fromThrift(snapshot.getMappings()))
                    == null, "Duplicate changelog source index: %s", snapshot.getSourceIndexId());
        }
        return ImmutableMap.copyOf(result);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ChangelogWriteMapping)) {
            return false;
        }
        ChangelogWriteMapping mapping = (ChangelogWriteMapping) other;
        return needHistoricalValue == mapping.needHistoricalValue && entries.equals(mapping.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(needHistoricalValue, entries);
    }
}
