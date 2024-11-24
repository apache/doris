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

package org.apache.doris.qe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The new strategy is as follow
 * 1. stat unselect and select bucket on each be
 * 2. find the be having highest select priority, which means having least selected or least left buckets
 * 3. select one bucket of it's unselected bucket to server query, with min select priority
 * 4. remove from unselected bucket of other be for selected bucket of step 3
 * 5. loop until unselected bucket on all be is empty, which means all bucket has found proper be.
 */
public class ScanRangeAssignmentColocateV2 extends ScanRangeAssignmentColocate {

    public static class BucketScore implements Comparable<BucketScore> {
        private final int bucketSeq;
        private final LocationAssignment location;
        private final int num;

        public BucketScore(int bucketSeq, LocationAssignment location, int num) {
            this.bucketSeq = bucketSeq;
            this.num = num;
            this.location = location;
        }

        public int getBucketSeq() {
            return bucketSeq;
        }

        public int getNum() {
            return num;
        }

        public LocationAssignment getLocation() {
            return location;
        }

        @Override
        public int compareTo(ScanRangeAssignmentColocateV2.BucketScore other) {
            int res = this.location.compareRemovePriority(other.location);
            if (res != 0) {
                return res;
            }
            return Integer.compare(this.num, other.num);
        }

        @Override
        public String toString() {
            return "BucketScore{"
                    + "bucketSeq=" + bucketSeq
                    + ", select=" + location.getSelectedBuckets()
                    + ", unselect=" + location.getUnselectBuckets()
                    + ", num=" + num
                    + '}';
        }
    }

    private BucketScore getBucketScore(int bucketSeq, Map<Integer, List<LocationAssignment>> bucketToLocationMap) {
        List<LocationAssignment> bucketLocations = bucketToLocationMap.get(bucketSeq);
        Iterator<LocationAssignment> iter = bucketLocations.iterator();
        LocationAssignment max = iter.next();
        while (iter.hasNext()) {
            LocationAssignment current = iter.next();
            if (current.compareRemovePriority(max) > 0) {
                max = current;
            }
        }
        int num = 0;
        for (LocationAssignment location : bucketLocations) {
            if (location.compareRemovePriority(max) >= 0) {
                num++;
            }
        }
        return new BucketScore(bucketSeq, max, num);
    }

    private LocationAssignment getHighestRemovePriority(List<LocationAssignment> locations,
            Map<Integer, List<LocationAssignment>> bucketToLocationMap) {
        // When be has less selected buckets, we should better not remove it early.
        // so we find the bucket which will remove the be with most selected buckets.
        Set<Integer> buckets = new HashSet<>();
        for (LocationAssignment location : locations) {
            buckets.addAll(location.getUnselectBuckets());
        }
        Iterator<Integer> iter = buckets.iterator();
        int bucketSeq = iter.next();
        BucketScore maxBucket = getBucketScore(bucketSeq, bucketToLocationMap);
        while (iter.hasNext()) {
            int current = iter.next();
            BucketScore score = getBucketScore(current, bucketToLocationMap);
            if (score.compareTo(maxBucket) > 0) {
                maxBucket = score;
            }
        }
        for (LocationAssignment location : locations) {
            if (location.getUnselectBuckets().contains(maxBucket.getBucketSeq())) {
                location.setNextBucket(maxBucket.getBucketSeq());
                return location;
            }
        }
        throw new RuntimeException("should no reach here");
    }

    private List<LocationAssignment> getHighestSelectPriority(Collection<LocationAssignment> locations) {
        Iterator<LocationAssignment> iterator = locations.iterator();
        LocationAssignment priorLocation = iterator.next();
        // find highest one
        while (iterator.hasNext()) {
            LocationAssignment current = iterator.next();
            if (current.compareSelectPriority(priorLocation) > 0) {
                priorLocation = current;
            }
        }
        // find all highest location
        List<LocationAssignment> priorLocations = new ArrayList<>();
        for (LocationAssignment current : locations) {
            if (current.compareSelectPriority(priorLocation) >= 0) {
                priorLocations.add(current);
            }
        }
        return priorLocations;
    }

    @Override
    public Map<Integer, Location> getSelectedBucketLocation(Map<Location, LocationAssignment> locationAssignmentMap) {
        Map<Integer, List<LocationAssignment>> bucketToLocationMap = new HashMap<>();
        for (LocationAssignment assignment : locationAssignmentMap.values()) {
            for (Integer bucketSeq : assignment.getUnselectBuckets()) {
                bucketToLocationMap.computeIfAbsent(bucketSeq, k -> new ArrayList<>()).add(assignment);
            }
        }
        Map<Integer, Location> selectedBucketLocation = new HashMap<>();
        // loop until all bucket find proper backend.
        while (!locationAssignmentMap.isEmpty()) {
            // 1. find locations with least selected or unselected buckets.
            List<LocationAssignment> priorLocations = getHighestSelectPriority(locationAssignmentMap.values());
            // 2. find bucket and backend with most selected or unselected buckets.
            LocationAssignment priorLocation = getHighestRemovePriority(priorLocations, bucketToLocationMap);
            int bucketSeq = priorLocation.getNextBucket();
            priorLocation.getSelectedBuckets().add(bucketSeq);
            selectedBucketLocation.put(bucketSeq, priorLocation.getLocation());
            // 3. remove bucket on other be and remove be if empty
            List<LocationAssignment> bucketLocations = bucketToLocationMap.get(bucketSeq);
            for (LocationAssignment locationAssignment : bucketLocations) {
                locationAssignment.getUnselectBuckets().remove(bucketSeq);
                if (locationAssignment.getUnselectBuckets().isEmpty()) {
                    locationAssignmentMap.remove(locationAssignment.getLocation());
                }
            }
        }
        return selectedBucketLocation;
    }
}
