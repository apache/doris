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

package org.apache.doris.datasource.lance.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** All physical segments belonging to one logical index in a fixed dataset snapshot. */
public final class LanceIndexSegmentGroup {
    private final String name;
    private final List<LanceIndexSegmentInfo> segments;

    private LanceIndexSegmentGroup(String name, List<LanceIndexSegmentInfo> segments) {
        this.name = name;
        this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
    }

    public static List<LanceIndexSegmentGroup> groupByName(List<LanceIndexSegmentInfo> segments) {
        Map<String, List<LanceIndexSegmentInfo>> grouped = new LinkedHashMap<>();
        for (LanceIndexSegmentInfo segment : segments) {
            grouped.computeIfAbsent(segment.getIndexName(), ignored -> new ArrayList<>()).add(segment);
        }
        List<LanceIndexSegmentGroup> indexes = new ArrayList<>(grouped.size());
        grouped.forEach((name, parts) -> indexes.add(new LanceIndexSegmentGroup(name, parts)));
        return Collections.unmodifiableList(indexes);
    }

    public String getName() {
        return name;
    }

    public List<LanceIndexSegmentInfo> getSegments() {
        return segments;
    }

    public List<LanceIndexSegmentInfo> getVectorSegments(int fieldId) {
        return matchingSegments(fieldId, LanceIndexSegmentInfo::isVectorIndex);
    }

    public List<LanceIndexSegmentInfo> getFullTextSegments(int fieldId) {
        return matchingSegments(fieldId, LanceIndexSegmentInfo::isFullTextIndex);
    }

    private List<LanceIndexSegmentInfo> matchingSegments(int fieldId,
            java.util.function.Predicate<LanceIndexSegmentInfo> typeMatches) {
        List<LanceIndexSegmentInfo> matches = new ArrayList<>();
        for (LanceIndexSegmentInfo segment : segments) {
            if (typeMatches.test(segment) && segment.getFieldIds().contains(fieldId)) {
                matches.add(segment);
            }
        }
        return matches;
    }
}
