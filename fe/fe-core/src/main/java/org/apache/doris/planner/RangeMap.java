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

package org.apache.doris.planner;

import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RangeMap<C extends Comparable<C>, V> {

    private final NavigableMap<Range<C>, V> rangeMap = new TreeMap<>(new RangeComparator<C>());

    public void put(Range<C> range, V value) {
        rangeMap.put(range, value);
    }

    public List<V> getOverlappingRangeValues(Range<C> searchRange) {
        return getOverlappingRanges(searchRange).stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    public List<Map.Entry<Range<C>, V>> getOverlappingRanges(Range<C> searchRange) {
        List<Map.Entry<Range<C>, V>> overlappingRanges = new ArrayList<>();

        // Find the possible starting point for the search
        Map.Entry<Range<C>, V> floorEntry = rangeMap.floorEntry(searchRange);
        Map.Entry<Range<C>, V> ceilingEntry = rangeMap.ceilingEntry(searchRange);

        // Start iterating from the earlier of the floor or ceiling entry
        Map.Entry<Range<C>, V> startEntry = (floorEntry != null) ? floorEntry : ceilingEntry;
        if (startEntry == null) {
            return overlappingRanges;
        }

        for (Map.Entry<Range<C>, V> entry : rangeMap.tailMap(startEntry.getKey()).entrySet()) {
            if (entry.getKey().lowerEndpoint().compareTo(searchRange.upperEndpoint()) > 0) {
                break; // No more overlapping ranges possible
            }
            if (entry.getKey().isConnected(searchRange) && !entry.getKey().intersection(searchRange).isEmpty()) {
                overlappingRanges.add(entry);
            }
        }
        return overlappingRanges;
    }

    private static class RangeComparator<C extends Comparable<C>> implements java.util.Comparator<Range<C>> {
        @Override
        public int compare(Range<C> r1, Range<C> r2) {
            return r1.lowerEndpoint().compareTo(r2.lowerEndpoint());
        }
    }
}
