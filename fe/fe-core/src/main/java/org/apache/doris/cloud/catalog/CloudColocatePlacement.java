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

package org.apache.doris.cloud.catalog;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

import java.util.Arrays;

public class CloudColocatePlacement {
    @FunctionalInterface
    interface ScoreFunction {
        long score(long grpId, long idx, long beId);
    }

    private CloudColocatePlacement() {
    }

    public static long score(long grpId, long idx, long beId) {
        return Hashing.murmur3_128().newHasher()
                .putLong(grpId)
                .putLong(idx)
                .putLong(beId)
                .hash()
                .asLong();
    }

    public static long pickBackendId(long grpId, long idx, long[] candidateBeIds) {
        return pickBackendId(grpId, idx, candidateBeIds, CloudColocatePlacement::score);
    }

    static long pickBackendId(long grpId, long idx, long[] candidateBeIds, ScoreFunction scoreFunction) {
        Preconditions.checkArgument(candidateBeIds.length > 0);
        long[] sortedBeIds = Arrays.copyOf(candidateBeIds, candidateBeIds.length);
        Arrays.sort(sortedBeIds);

        long pickedBeId = sortedBeIds[0];
        long maxScore = scoreFunction.score(grpId, idx, pickedBeId);
        for (int i = 1; i < sortedBeIds.length; i++) {
            long beId = sortedBeIds[i];
            long score = scoreFunction.score(grpId, idx, beId);
            if (score > maxScore || (score == maxScore && beId < pickedBeId)) {
                maxScore = score;
                pickedBeId = beId;
            }
        }
        return pickedBeId;
    }
}
