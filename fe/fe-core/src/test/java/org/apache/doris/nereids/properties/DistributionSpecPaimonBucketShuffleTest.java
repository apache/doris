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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.PaimonBucketId;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class DistributionSpecPaimonBucketShuffleTest {

    @Test
    public void testEqualsHashCodeAndSatisfy() {
        List<Expression> expressions = buildExpressions();
        DistributionSpecPaimonBucketShuffle left = new DistributionSpecPaimonBucketShuffle(expressions);
        DistributionSpecPaimonBucketShuffle same = new DistributionSpecPaimonBucketShuffle(expressions);
        DistributionSpecPaimonBucketShuffle different = new DistributionSpecPaimonBucketShuffle(
                Collections.singletonList(new SlotReference("k2", IntegerType.INSTANCE)));

        Assertions.assertEquals(left, same);
        Assertions.assertEquals(left.hashCode(), same.hashCode());
        Assertions.assertNotEquals(left, different);
        Assertions.assertTrue(left.satisfy(same));
        Assertions.assertTrue(left.satisfy(DistributionSpecAny.INSTANCE));
        Assertions.assertFalse(left.satisfy(different));
        Assertions.assertTrue(left.toString().contains("DistributionSpecPaimonBucketShuffle"));
        Assertions.assertEquals("DistributionSpecPaimonBucketShuffle", left.shapeInfo());
    }

    private List<Expression> buildExpressions() {
        SlotReference slot = new SlotReference("k1", IntegerType.INSTANCE);
        return Collections.singletonList(new PaimonBucketId(slot, new IntegerLiteral(2)));
    }
}
