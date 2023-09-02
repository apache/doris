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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DistributionSpecTest {

    @Test
    public void testSatisfy() {
        DistributionSpec replicated = DistributionSpecReplicated.INSTANCE;
        DistributionSpec any = DistributionSpecAny.INSTANCE;
        DistributionSpec gather = DistributionSpecGather.INSTANCE;
        DistributionSpec hash = new DistributionSpecHash(Lists.newArrayList(), ShuffleType.REQUIRE);

        Assertions.assertTrue(replicated.satisfy(any));
        Assertions.assertTrue(gather.satisfy(any));
        Assertions.assertTrue(hash.satisfy(any));
        Assertions.assertTrue(any.satisfy(any));
        Assertions.assertFalse(replicated.satisfy(gather));
        Assertions.assertTrue(gather.satisfy(gather));
        Assertions.assertFalse(hash.satisfy(gather));
        Assertions.assertFalse(any.satisfy(gather));
        Assertions.assertTrue(replicated.satisfy(replicated));
        Assertions.assertFalse(gather.satisfy(replicated));
        Assertions.assertFalse(hash.satisfy(replicated));
        Assertions.assertFalse(any.satisfy(replicated));
    }
}
