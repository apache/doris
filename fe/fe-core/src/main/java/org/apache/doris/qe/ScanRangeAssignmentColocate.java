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

import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public abstract class ScanRangeAssignmentColocate {

    public static class Location implements Comparable<Location> {
        private final long backend;

        public Location(long backend) {
            this.backend = backend;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Location location = (Location) object;
            return backend == location.backend;
        }

        @Override
        public int hashCode() {
            return Objects.hash(backend);
        }

        @Override
        public int compareTo(Location other) {
            return Long.compare(this.backend, other.backend);
        }

        @Override
        public String toString() {
            return "Location{"
                    + "backend=" + backend
                    + '}';
        }
    }

    public static class LocationAssignment implements Comparable<LocationAssignment> {
        // means be
        private final Location location;
        // The bucketSeq not selected on this be
        private final Set<Integer> unselectBuckets = new HashSet<>();
        // The bucketSeq selected on this be
        private final Set<Integer> selectedBuckets = new HashSet<>();
        // next candidate bucket
        private int nextBucket = -1;

        public LocationAssignment(Location location) {
            this.location = location;
        }

        public Location getLocation() {
            return location;
        }

        public Set<Integer> getSelectedBuckets() {
            return selectedBuckets;
        }

        public Set<Integer> getUnselectBuckets() {
            return unselectBuckets;
        }

        public int getNextBucket() {
            return nextBucket;
        }

        public void setNextBucket(int nextBucket) {
            this.nextBucket = nextBucket;
        }

        /**
         * when the number of selected bucket is smaller or left bucket is smaller, be is more prior
         */
        public int compareSelectPriority(LocationAssignment other) {
            int res = Integer.compare(other.selectedBuckets.size(), this.selectedBuckets.size());
            if (res != 0) {
                return res;
            }
            return Integer.compare(other.unselectBuckets.size(), this.unselectBuckets.size());
        }

        public int compareRemovePriority(LocationAssignment other) {
            return other.compareSelectPriority(this);
        }

        @Override
        public String toString() {
            return "LocationAssignment{"
                    + "location=" + location
                    + ", unselectBuckets=" + unselectBuckets
                    + ", selectedBuckets=" + selectedBuckets
                    + '}';
        }

        @Override
        public int compareTo(LocationAssignment other) {
            return this.compareSelectPriority(other);
        }
    }

    @VisibleForTesting
    public abstract Map<Integer, Location> getSelectedBucketLocation(Map<Location, LocationAssignment> locationMap);

    public Map<Integer, List<TScanRangeLocation>> computeScanRangeAssignmentByColocate(
            Map<Integer, List<TScanRangeLocation>> bucketLocationMap,
            Map<Integer, TNetworkAddress> bucketSeqToAddress) {
        Map<Location, LocationAssignment> locationAssignmentMap = new HashMap<>();
        for (Map.Entry<Integer, List<TScanRangeLocation>> entry : bucketLocationMap.entrySet()) {
            Integer bucketSeq = entry.getKey();
            List<TScanRangeLocation> scanRangeLocations = entry.getValue();
            TNetworkAddress address = bucketSeqToAddress.get(bucketSeq);
            for (TScanRangeLocation scanRangeLocation : scanRangeLocations) {
                if (address != null && !Objects.equals(address, scanRangeLocation.getServer())) {
                    continue;
                }
                Location location = new Location(scanRangeLocation.getBackendId());
                LocationAssignment locationAssignment = locationAssignmentMap.computeIfAbsent(location,
                        k -> new LocationAssignment(location));
                if (address != null) {
                    locationAssignment.selectedBuckets.add(bucketSeq);
                } else {
                    locationAssignment.unselectBuckets.add(bucketSeq);
                }
            }
        }
        Map<Integer, Location> selectedBucketLocation = getSelectedBucketLocation(locationAssignmentMap);
        Map<Integer, List<TScanRangeLocation>> bucketPreferLocationsMap = new HashMap<>();
        for (Map.Entry<Integer, Location> entry : selectedBucketLocation.entrySet()) {
            int bucketSeq = entry.getKey();
            Location location = entry.getValue();
            List<TScanRangeLocation> scanRangeLocations = bucketLocationMap.get(bucketSeq);
            for (TScanRangeLocation scanRangeLocation : scanRangeLocations) {
                if (Objects.equals(location.backend, scanRangeLocation.getBackendId())) {
                    bucketPreferLocationsMap.put(bucketSeq, Collections.singletonList(scanRangeLocation));
                    break;
                }
            }
        }
        return bucketPreferLocationsMap;
    }
}