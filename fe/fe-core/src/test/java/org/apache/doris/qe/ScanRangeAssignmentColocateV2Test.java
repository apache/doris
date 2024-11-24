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

import org.apache.doris.qe.ScanRangeAssignmentColocate.Location;
import org.apache.doris.qe.ScanRangeAssignmentColocate.LocationAssignment;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ScanRangeAssignmentColocateV2Test {

    @Test
    public void testTwoReplica() {
        // example of bad case:
        // bucket to be map: {0: {1001,1002}, 1: {1004,1003}, 2: {1002,1003}, 3:{1004, 1001}}
        // strategy v1 may get not balanced result: { 0:{1001}, 1: {1004}, 2: {1002}, 3:{1004}}
        List<LocationAssignment> assignments = createBackend(4);
        assignments.get(0).getUnselectBuckets().addAll(Arrays.asList(0, 3));
        assignments.get(1).getUnselectBuckets().addAll(Arrays.asList(0, 2));
        assignments.get(2).getUnselectBuckets().addAll(Arrays.asList(1, 2));
        assignments.get(3).getUnselectBuckets().addAll(Arrays.asList(1, 3));

        Map<Location, LocationAssignment> assignmentMap = assignments.stream()
                .collect(Collectors.toMap(LocationAssignment::getLocation, Function.identity()));
        ScanRangeAssignmentColocateV2 colocateV2 = new ScanRangeAssignmentColocateV2();
        Map<Integer, Location> result = colocateV2.getSelectedBucketLocation(assignmentMap);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(4, new HashSet<>(result.values()).size());
    }

    private void runNoShuffle(int backendNum, int bucketNum, int replicaNum) {
        List<LocationAssignment> assignments = createBackend(backendNum);
        assigmentTabletBalanced(assignments, bucketNum, replicaNum, false);
        Map<Location, LocationAssignment> assignmentMap = assignments.stream()
                .collect(Collectors.toMap(LocationAssignment::getLocation, Function.identity()));
        ScanRangeAssignmentColocateV2 colocateV2 = new ScanRangeAssignmentColocateV2();
        Map<Integer, Location> bucketToLocationMap = colocateV2.getSelectedBucketLocation(assignmentMap);
        Assert.assertEquals(bucketNum, bucketToLocationMap.size());

        Map<Location, Integer> locationBucketCountMap = bucketToLocationMap.values().stream()
                .collect(Collectors.toMap(Function.identity(), location -> 1, Integer::sum));
        if (backendNum >= bucketNum) {
            Assert.assertEquals(bucketNum, locationBucketCountMap.size());
            Assert.assertEquals(ImmutableSet.of(1), new HashSet<>(locationBucketCountMap.values()));
        } else if (bucketNum % backendNum == 0) {
            Assert.assertEquals(backendNum, locationBucketCountMap.size());
            Assert.assertEquals(ImmutableSet.of(bucketNum / backendNum),
                    new HashSet<>(locationBucketCountMap.values()));
        } else {
            Assert.assertEquals(backendNum, locationBucketCountMap.size());
            Assert.assertEquals(ImmutableSet.of(bucketNum / backendNum, bucketNum / backendNum + 1),
                    new HashSet<>(locationBucketCountMap.values()));
        }
    }

    private boolean runShuffle(int backendNum, int bucketNum, int replicaNum, boolean v2) {
        List<LocationAssignment> assignments = createBackend(backendNum);
        assigmentTabletBalanced(assignments, bucketNum, replicaNum, true);
        return runShuffle(assignments, backendNum, bucketNum, v2);
    }

    private boolean runShuffle(List<LocationAssignment> assignments, int backendNum, int bucketNum, boolean v2) {
        Map<Location, LocationAssignment> assignmentMap = assignments.stream()
                .collect(Collectors.toMap(LocationAssignment::getLocation, Function.identity()));
        ScanRangeAssignmentColocate colocateStrategy = getAssignmentStrategy(v2);
        Map<Integer, Location> bucketToLocationMap = colocateStrategy.getSelectedBucketLocation(assignmentMap);
        Assert.assertEquals(bucketNum, bucketToLocationMap.size());

        Map<Location, Integer> locationBucketCountMap = bucketToLocationMap.values().stream()
                .collect(Collectors.toMap(Function.identity(), location -> 1, Integer::sum));
        if (backendNum >= bucketNum) {
            return bucketNum == locationBucketCountMap.size()
                    && Objects.equals(ImmutableSet.of(1), new HashSet<>(locationBucketCountMap.values()));
        } else if (bucketNum % backendNum == 0) {
            return backendNum == locationBucketCountMap.size()
                    && Objects.equals(ImmutableSet.of(bucketNum / backendNum),
                        new HashSet<>(locationBucketCountMap.values()));
        } else {
            return backendNum == locationBucketCountMap.size()
                    && Objects.equals(ImmutableSet.of(bucketNum / backendNum, bucketNum / backendNum + 1),
                        new HashSet<>(locationBucketCountMap.values()));
        }
    }

    private void runBattle(int backendNum, int bucketNum, int replicaNum) {
        long v1 = 0;
        long v2 = 0;
        for (int i = 0; i < 5000; i++) {
            v1 += runShuffle(backendNum, bucketNum, replicaNum, false) ? 1 : 0;
            v2 += runShuffle(backendNum, bucketNum, replicaNum, true) ? 1 : 0;
        }
        System.out.printf("be:%d, bucket:%d, replica:%d, v1:%04d, v2:%04d\n", backendNum, bucketNum, replicaNum, v1,
                v2);
    }

    @Test
    public void runBattle() {
        runBattle(10, 20, 3);
        runBattle(10, 60, 3);
        runBattle(20, 20, 2);
        runBattle(20, 40, 2);
        runBattle(20, 40, 3);
        runBattle(40, 40, 2);
        runBattle(40, 80, 2);
        runBattle(40, 40, 3);
    }

    @Test
    public void testNoShuffle() {
        runNoShuffle(20, 20, 2);
        runNoShuffle(20, 40, 2);
        runNoShuffle(40, 80, 2);
        runNoShuffle(20, 80, 2);
        runNoShuffle(10, 20, 3);

        runNoShuffle(20, 4, 3);
        runNoShuffle(4, 4, 1);
        runNoShuffle(4, 4, 3);
        runNoShuffle(11, 20, 3);
        runNoShuffle(11, 20, 2);
    }

    @Test
    public void bench() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 20000; i++) {
            runShuffle(20, 20, 2, true);
            runShuffle(20, 40, 2, true);
            runShuffle(40, 80, 2, true);
            runShuffle(20, 80, 2, true);
            runShuffle(10, 20, 3, true);
        }
        long cost = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("avg cost " + (cost / 100000.0) + " ms");
    }

    @Test
    public void testLocationHash() {
        Location location1 = createBackend(1).get(0).getLocation();
        Location location2 = createBackend(1).get(0).getLocation();
        Location location3 = createBackend(2).get(1).getLocation();
        Assert.assertEquals(location1, location2);
        Assert.assertNotEquals(location1, location3);
        Assert.assertEquals(location1.hashCode(), location2.hashCode());
        runShuffle(20, 20, 2, true);
    }

    private List<LocationAssignment> createBackend(int num) {
        List<LocationAssignment> result = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            TNetworkAddress address = new TNetworkAddress();
            address.setHostname("be-n" + i);
            address.setPort(9050);
            Location location = new Location(1000 + i);
            result.add(new LocationAssignment(location));
        }
        return result;
    }

    private void assigmentTabletBalanced(List<LocationAssignment> assignments, int bucketNum, int replicaNum,
            boolean shuffle) {
        Preconditions.checkState(replicaNum <= assignments.size());
        for (int bucketSeq = 0; bucketSeq < bucketNum; bucketSeq++) {
            for (int i = 0; i < replicaNum; i++) {
                if (shuffle) {
                    Collections.shuffle(assignments);
                }
                Iterator<LocationAssignment> iterator = assignments.iterator();
                LocationAssignment assignment = iterator.next();
                while (iterator.hasNext()) {
                    LocationAssignment current = iterator.next();
                    if (current.getUnselectBuckets().contains(bucketSeq)) {
                        continue;
                    }
                    if (assignment.getUnselectBuckets().contains(bucketSeq)
                            || current.getUnselectBuckets().size() < assignment.getUnselectBuckets().size()) {
                        assignment = current;
                    }
                }
                Preconditions.checkState(!assignment.getUnselectBuckets().contains(bucketSeq));
                assignment.getUnselectBuckets().add(bucketSeq);
            }
        }
        assignments.removeIf(assignment -> assignment.getUnselectBuckets().isEmpty());
    }

    private static ScanRangeAssignmentColocate getAssignmentStrategy(
            boolean v2) {
        if (v2) {
            return new ScanRangeAssignmentColocateV2();
        }
        return new ScanRangeAssignmentColocateV1(true);
    }
}
