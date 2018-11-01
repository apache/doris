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

package org.apache.doris.common.util;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;
import java.util.Random;

public class LoadBalancerTest {

    @Test
    public void test() {
        Integer[] keys = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
        LoadBalancer<Integer> balancer = new LoadBalancer<Integer>(1L);

        Random random = new Random(System.currentTimeMillis());

        for (int i = 0; i < 10000; i++) {
            List<Integer> randomKeys = Lists.newArrayList();
            for (int j = 0; j < 3; j++) {
                int index = Math.abs(random.nextInt()) % 10;
                randomKeys.add(keys[index]);
            }

            balancer.chooseKey(randomKeys);
        }

        System.out.println(balancer);
    }

}
