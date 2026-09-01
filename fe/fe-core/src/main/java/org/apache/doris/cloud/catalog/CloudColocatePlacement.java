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
    private static final long EXTRA_QUOTA_SCORE_IDX = -1L;

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

    public static long[] buildPlacement(long grpId, long[] candidateBeIds, int bucketNum) {
        return buildPlacement(grpId, candidateBeIds, bucketNum, CloudColocatePlacement::score);
    }

    static long[] buildPlacement(long grpId, long[] candidateBeIds, int bucketNum, ScoreFunction scoreFunction) {
        Preconditions.checkArgument(candidateBeIds.length > 0);
        Preconditions.checkArgument(bucketNum > 0);
        long[] sortedBeIds = Arrays.copyOf(candidateBeIds, candidateBeIds.length);
        Arrays.sort(sortedBeIds);

        int[] remainingQuota = new int[sortedBeIds.length];
        Arrays.fill(remainingQuota, bucketNum / sortedBeIds.length);
        int[] extraQuotaCandidates = new int[sortedBeIds.length];
        Arrays.fill(extraQuotaCandidates, 1);
        for (int i = 0; i < bucketNum % sortedBeIds.length; i++) {
            int pickedIndex = pickBackendIndex(grpId, EXTRA_QUOTA_SCORE_IDX, sortedBeIds,
                    extraQuotaCandidates, scoreFunction);
            remainingQuota[pickedIndex]++;
            extraQuotaCandidates[pickedIndex] = 0;
        }

        long[] placement = new long[bucketNum];
        for (int idx = 0; idx < bucketNum; idx++) {
            int pickedIndex = pickBackendIndex(grpId, idx, sortedBeIds, remainingQuota, scoreFunction);
            placement[idx] = sortedBeIds[pickedIndex];
            remainingQuota[pickedIndex]--;
        }
        return placement;
    }

    private static int pickBackendIndex(long grpId, long idx, long[] sortedBeIds, int[] remainingQuota,
            ScoreFunction scoreFunction) {
        int pickedIndex = 0;
        while (remainingQuota[pickedIndex] == 0) {
            pickedIndex++;
        }
        long maxScore = scoreFunction.score(grpId, idx, sortedBeIds[pickedIndex]);
        for (int i = pickedIndex + 1; i < sortedBeIds.length; i++) {
            if (remainingQuota[i] > 0) {
                long score = scoreFunction.score(grpId, idx, sortedBeIds[i]);
                if (score > maxScore) {
                    maxScore = score;
                    pickedIndex = i;
                }
            }
        }
        return pickedIndex;
    }
}
