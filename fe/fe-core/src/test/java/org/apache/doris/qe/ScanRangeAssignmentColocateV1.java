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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScanRangeAssignmentColocateV1 extends ScanRangeAssignmentColocate {

    private final boolean enableOrderedLocations;

    public ScanRangeAssignmentColocateV1(boolean enableOrderedLocations) {
        this.enableOrderedLocations = enableOrderedLocations;
    }

    @Override
    public Map<Integer, Location> getSelectedBucketLocation(Map<Location, LocationAssignment> locationAssignmentMap) {
        Map<Integer, List<LocationAssignment>> bucketToLocationMap = new LinkedHashMap<>();
        for (LocationAssignment assignment : locationAssignmentMap.values()) {
            for (Integer bucketSeq : assignment.getUnselectBuckets()) {
                bucketToLocationMap.computeIfAbsent(bucketSeq, k -> new ArrayList<>()).add(assignment);
            }
        }

        Map<Integer, Location> selectedBucketLocation = new HashMap<>();
        for (int bucketSeq = 0; bucketSeq < bucketToLocationMap.size(); bucketSeq++) {
            List<LocationAssignment> bucketLocations = bucketToLocationMap.get(bucketSeq);
            if (enableOrderedLocations){
                bucketLocations.sort(Comparator.comparing(LocationAssignment::getLocation));
            } else {
                Collections.shuffle(bucketLocations);
            }
            Iterator<LocationAssignment> iter = bucketLocations.iterator();
            LocationAssignment priorLocation = iter.next();
            while (iter.hasNext()) {
                LocationAssignment current = iter.next();
                if (current.compareTo(priorLocation) > 0) {
                    priorLocation = current;
                }
            }
            priorLocation.getSelectedBuckets().add(bucketSeq);
            for (LocationAssignment locationAssignment : bucketLocations) {
                locationAssignment.getUnselectBuckets().remove(bucketSeq);
            }
            selectedBucketLocation.put(bucketSeq, priorLocation.getLocation());
        }
        return selectedBucketLocation;
    }
}
