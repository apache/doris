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

package org.apache.doris.load.loadv2;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.doris.load.loadv2.Hll.*;

public class HllTest {

    @Test
    public void testFindFirstNonZeroBitPosition() {
        Assert.assertTrue(getLongTailZeroNum(0) == 0);
        Assert.assertTrue(getLongTailZeroNum(1) == 0);
        Assert.assertTrue(getLongTailZeroNum(1l << 30) == 30);
        Assert.assertTrue(getLongTailZeroNum(1l << 62) == 62);
    }

    @Test
    public void HllBasicTest() throws IOException {
        // test empty
        Hll emptyHll = new Hll();

        Assert.assertTrue(emptyHll.getType() == HLL_DATA_EMPTY);
        Assert.assertTrue(emptyHll.estimateCardinality() == 0);

        ByteArrayOutputStream emptyOutputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(emptyOutputStream);
        emptyHll.serialize(output);
        DataInputStream emptyInputStream = new DataInputStream(new ByteArrayInputStream(emptyOutputStream.toByteArray()));
        Hll deserializedEmptyHll = new Hll();
        deserializedEmptyHll.deserialize(emptyInputStream);
        Assert.assertTrue(deserializedEmptyHll.getType() == HLL_DATA_EMPTY);

        // test explicit
        Hll explicitHll = new Hll();
        for (int i = 0; i < HLL_EXPLICLIT_INT64_NUM; i++) {
            explicitHll.updateWithHash(i);
        }
        Assert.assertTrue(explicitHll.getType() == HLL_DATA_EXPLICIT);
        Assert.assertTrue(explicitHll.estimateCardinality() == HLL_EXPLICLIT_INT64_NUM);

        ByteArrayOutputStream explicitOutputStream = new ByteArrayOutputStream();
        DataOutput explicitOutput = new DataOutputStream(explicitOutputStream);
        explicitHll.serialize(explicitOutput);
        DataInputStream explicitInputStream = new DataInputStream(new ByteArrayInputStream(explicitOutputStream.toByteArray()));
        Hll deserializedExplicitHll = new Hll();
        deserializedExplicitHll.deserialize(explicitInputStream);
        Assert.assertTrue(deserializedExplicitHll.getType() == HLL_DATA_EXPLICIT);

        // test sparse
        Hll sparseHll = new Hll();
        for (int i = 0; i < HLL_SPARSE_THRESHOLD; i++) {
            sparseHll.updateWithHash(i);
        }
        Assert.assertTrue(sparseHll.getType() == HLL_DATA_FULL);
        // 2% error rate
        Assert.assertTrue(sparseHll.estimateCardinality() > HLL_SPARSE_THRESHOLD * (1 - 0.02) &&
                sparseHll.estimateCardinality() < HLL_SPARSE_THRESHOLD * (1 + 0.02));

        ByteArrayOutputStream sparseOutputStream = new ByteArrayOutputStream();
        DataOutput sparseOutput = new DataOutputStream(sparseOutputStream);
        sparseHll.serialize(sparseOutput);
        DataInputStream sparseInputStream = new DataInputStream(new ByteArrayInputStream(sparseOutputStream.toByteArray()));
        Hll deserializedSparseHll = new Hll();
        deserializedSparseHll.deserialize(sparseInputStream);
        Assert.assertTrue(deserializedSparseHll.getType() == HLL_DATA_SPARSE);
        Assert.assertTrue(sparseHll.estimateCardinality() == deserializedSparseHll.estimateCardinality());


        // test full
        Hll fullHll = new Hll();
        for (int i = 1; i <= Short.MAX_VALUE; i++) {
            fullHll.updateWithHash(i);
        }
        Assert.assertTrue(fullHll.getType() == HLL_DATA_FULL);
        // the result 32748 is consistent with C++ 's implementation
        Assert.assertTrue(fullHll.estimateCardinality() == 32748);
        // 2% error rate
        Assert.assertTrue(fullHll.estimateCardinality() > Short.MAX_VALUE * (1 - 0.02) &&
                fullHll.estimateCardinality() < Short.MAX_VALUE * (1 + 0.02));

        ByteArrayOutputStream fullHllOutputStream = new ByteArrayOutputStream();
        DataOutput fullHllOutput = new DataOutputStream(fullHllOutputStream);
        fullHll.serialize(fullHllOutput);
        DataInputStream fullHllInputStream = new DataInputStream(new ByteArrayInputStream(fullHllOutputStream.toByteArray()));
        Hll deserializedFullHll = new Hll();
        deserializedFullHll.deserialize(fullHllInputStream);
        Assert.assertTrue(deserializedFullHll.getType() == HLL_DATA_FULL);
        Assert.assertTrue(deserializedFullHll.estimateCardinality() == fullHll.estimateCardinality());

    }

    @Test
    public void testMerge() {
        Hll emptyHll = new Hll();

        Hll explicitHll = new Hll();
        for (int i = 0 ;i < 100; i++) {
            explicitHll.updateWithHash(i);
        }

        Hll fullHll = new Hll();
        for (int i = 0; i < Short.MAX_VALUE; i++) {
            fullHll.updateWithHash(i);
        }

        // empty to empty
        Hll emptyHll1 = new Hll();
        emptyHll1.merge(emptyHll);
        Assert.assertTrue(emptyHll1.getType() == HLL_DATA_EMPTY);
        Assert.assertTrue(emptyHll1.estimateCardinality() == 0);

        // empty to explicit
        Hll emptyHll3 = new Hll();
        emptyHll3.merge(explicitHll);
        Assert.assertTrue(emptyHll3.getType() == HLL_DATA_EXPLICIT);
        Assert.assertTrue(emptyHll3.estimateCardinality() == explicitHll.estimateCardinality());

        // empty to full
        Hll emptyHll2 = new Hll();
        emptyHll2.merge(fullHll);
        Assert.assertTrue(emptyHll2.getType() == HLL_DATA_FULL);
        Assert.assertTrue(emptyHll2.estimateCardinality() == fullHll.estimateCardinality());

        // explicit to full
        Hll explicitHll2 = new Hll();
        for (int i = 0 ;i < 100; i++) {
            explicitHll2.updateWithHash(i + Short.MAX_VALUE);
        }
        explicitHll2.merge(fullHll);
        Assert.assertTrue(explicitHll2.getType() == HLL_DATA_FULL);
        Assert.assertTrue(explicitHll2.estimateCardinality() > fullHll.estimateCardinality());

        // full to full
        Hll fullHll2 = new Hll();
        for (int i = 0; i < Short.MAX_VALUE; i++) {
            fullHll2.updateWithHash(i + Short.MAX_VALUE);
        }
        fullHll2.merge(fullHll);
        Assert.assertTrue(fullHll2.getType() == HLL_DATA_FULL);
        Assert.assertTrue(fullHll2.estimateCardinality() > fullHll.estimateCardinality());
    }


}
