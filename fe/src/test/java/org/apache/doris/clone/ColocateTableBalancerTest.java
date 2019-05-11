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

package org.apache.doris.clone;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ColocateTableBalancerTest {

    private static final int replicateNum = 3;
    // [[1, 2, 3], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
    private static final List<List<Long>> backendsPerBucketSeq =
    Lists.partition(Lists.newArrayList(1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), replicateNum);

    @Test
    /*
     * backends: [1,2,3,4]
     * bucket num: 5
     * replicateNum: 3
     * new backends: [5]
     */
    public void testBalanceNormalWithOneBackend() {
        List<Long> newBackends = Lists.newArrayList(5L);
        System.out.println("newBackends: " + newBackends);

        List<List<Long>> newBackendsPerBucketSeq = ColocateTableBalancer.balance(backendsPerBucketSeq, newBackends);
        System.out.println("new backendsPerBucketSeq: " + newBackendsPerBucketSeq);

        List<List<Long>> expectBackendsPerBucketSeq  = Lists.partition(Lists.newArrayList(5L, 2L, 3L, 4L, 1L, 5L, 5L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), replicateNum);
        Assert.assertEquals(expectBackendsPerBucketSeq, newBackendsPerBucketSeq);
    }

    @Test
    /*
     * backends: [1,2,3,4]
     * bucket num: 5
     * replicateNum: 3
     * new backends: [5,6]
     */
    public void testBalanceNormalWithTwoBackend() {
        List<Long> newBackends = Lists.newArrayList(5L, 6L);
        System.out.println("newBackends: " + newBackends);

        List<List<Long>> newBackendsPerBucketSeq = ColocateTableBalancer.balance(backendsPerBucketSeq, newBackends);
        System.out.println("new backendsPerBucketSeq: " + newBackendsPerBucketSeq);

        List<List<Long>> expectBackendsPerBucketSeq  = Lists.partition(Lists.newArrayList(5L, 6L, 3L, 6L, 1L, 2L, 5L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), replicateNum);
        Assert.assertEquals(expectBackendsPerBucketSeq, newBackendsPerBucketSeq);
    }

    @Test
    /*
     * backends: [1,2,3,4]
     * bucket num: 5
     * replicateNum: 3
     * new backends: [5,6,7,8,9,10,11,12,13,14,15]
     */
    public void testBalanceNormalWithManyBackendEqualReplicateNum() {
        List<Long> newBackends = Lists.newArrayList(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L);
        System.out.println("newBackends: " + newBackends);

        List<List<Long>> newBackendsPerBucketSeq = ColocateTableBalancer.balance(backendsPerBucketSeq, newBackends);
        System.out.println("new backendsPerBucketSeq: " + newBackendsPerBucketSeq);

        List<List<Long>> expectBackendsPerBucketSeq  = Lists.partition(Lists.newArrayList(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 4L, 1L, 2L, 3L), replicateNum);
        Assert.assertEquals(expectBackendsPerBucketSeq, newBackendsPerBucketSeq);
    }


    @Test
    /*
     * backends: [1,2,3,4]
     * bucket num: 5
     * replicateNum: 3
     * new backends: [5,6,7,8,9,10,11,12,13,14,15,16]
     */
    public void testBalanceNormalWithManyBackendExceedReplicateNum() {
        List<Long> newBackends = Lists.newArrayList(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L);
        System.out.println("newBackends: " + newBackends);

        List<List<Long>> newBackendsPerBucketSeq = ColocateTableBalancer.balance(backendsPerBucketSeq, newBackends);
        System.out.println("new backendsPerBucketSeq: " + newBackendsPerBucketSeq);

        List<List<Long>> expectBackendsPerBucketSeq  = Lists.partition(Lists.newArrayList(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 4L, 1L, 2L, 3L), replicateNum);
        Assert.assertEquals(expectBackendsPerBucketSeq, newBackendsPerBucketSeq);
    }
}
