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

package org.apache.doris.statistics.util;

import org.apache.doris.common.io.Hll;

import org.apache.commons.codec.binary.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class Hll128Test {

    @Test
    public void basicTest() {
        // test empty
        Hll128 emptyHll = new Hll128();
        Assert.assertEquals(Hll128.HLL_DATA_EMPTY, emptyHll.getType());
        Assert.assertEquals(0, emptyHll.estimateCardinality());

        // test explicit
        Hll128 explicitHll = new Hll128();
        for (int i = 0; i < Hll.HLL_EXPLICIT_INT64_NUM; i++) {
            explicitHll.update(i);
        }
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, explicitHll.getType());
        Assert.assertEquals(Hll.HLL_EXPLICIT_INT64_NUM, explicitHll.estimateCardinality());

        // test full
        Hll128 fullHll = new Hll128();
        for (int i = 1; i <= Short.MAX_VALUE; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            fullHll.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        Assert.assertEquals(Hll.HLL_DATA_FULL, fullHll.getType());
        Assert.assertEquals(33141, fullHll.estimateCardinality());
        Assert.assertTrue(fullHll.estimateCardinality() > Short.MAX_VALUE * (1 - 0.1)
                && fullHll.estimateCardinality() < Short.MAX_VALUE * (1 + 0.1));

    }

    @Test
    public void testFromHll() throws IOException {
        // test empty
        Hll emptyHll = new Hll();
        Hll128 hll128 = Hll128.fromHll(emptyHll);
        Assert.assertEquals(Hll128.HLL_DATA_EMPTY, hll128.getType());
        Assert.assertEquals(0, hll128.estimateCardinality());

        // test explicit
        Hll explicitHll = new Hll();
        for (int i = 0; i < Hll.HLL_EXPLICIT_INT64_NUM; i++) {
            explicitHll.updateWithHash(i);
        }
        hll128 = Hll128.fromHll(explicitHll);
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, hll128.getType());
        Assert.assertEquals(Hll.HLL_EXPLICIT_INT64_NUM, hll128.estimateCardinality());

        // test full
        Hll fullHll = new Hll();
        for (int i = 0; i < 10000; i++) {
            fullHll.updateWithHash(i);
        }
        hll128 = Hll128.fromHll(fullHll);
        Assert.assertEquals(Hll128.HLL_DATA_FULL, hll128.getType());
        Assert.assertTrue(hll128.estimateCardinality() > 9000 && hll128.estimateCardinality() < 11000);
    }

    @Test
    public void testMerge() throws IOException {
        // test empty merge empty
        Hll128 empty1 = new Hll128();
        Hll128 empty2 = new Hll128();
        empty1.merge(empty2);
        Assert.assertEquals(Hll128.HLL_DATA_EMPTY, empty1.getType());
        Assert.assertEquals(0, empty1.estimateCardinality());

        // test empty merge explicit
        Hll128 empty = new Hll128();
        Hll128 explicit = new Hll128();
        for (int i = 1; i < Hll.HLL_EXPLICIT_INT64_NUM; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            explicit.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        empty.merge(explicit);
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, empty.getType());
        Assert.assertEquals(Hll.HLL_EXPLICIT_INT64_NUM - 1, empty.estimateCardinality());

        // test empty merge full
        empty = new Hll128();
        Hll128 full = new Hll128();
        for (int i = 1; i < 10000; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            full.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        empty.merge(full);
        Assert.assertEquals(Hll128.HLL_DATA_FULL, empty.getType());
        Assert.assertTrue(empty.estimateCardinality() > 9000 && empty.estimateCardinality() < 11000);

        // test explicit merge empty
        empty = new Hll128();
        explicit = new Hll128();
        for (int i = 1; i < Hll.HLL_EXPLICIT_INT64_NUM; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            explicit.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        explicit.merge(empty);
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, explicit.getType());
        Assert.assertEquals(Hll.HLL_EXPLICIT_INT64_NUM - 1, explicit.estimateCardinality());

        // test explicit merge explicit
        Hll128 explicit1 = new Hll128();
        Hll128 explicit2 = new Hll128();
        for (int i = 0; i < 10; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            explicit1.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        for (int i = 0; i < 30; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            explicit2.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        explicit1.merge(explicit2);
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, explicit1.getType());
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, explicit2.getType());
        Assert.assertEquals(30, explicit1.estimateCardinality());

        explicit2 = new Hll128();
        for (int i = 10001; i < 10000 + Hll.HLL_EXPLICIT_INT64_NUM; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            explicit2.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, explicit1.getType());
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, explicit2.getType());
        explicit1.merge(explicit2);
        Assert.assertEquals(Hll128.HLL_DATA_FULL, explicit1.getType());
        Assert.assertTrue(explicit1.estimateCardinality() > 170 && explicit1.estimateCardinality() < 210);

        // Test explicit merge full
        explicit = new Hll128();
        for (int i = 0; i < 10; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            explicit.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        full = new Hll128();
        for (int i = 1; i < 10000; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            full.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        Assert.assertEquals(Hll128.HLL_DATA_FULL, full.getType());
        explicit.merge(full);
        Assert.assertEquals(Hll128.HLL_DATA_FULL, explicit.getType());
        Assert.assertTrue(explicit.estimateCardinality() > 9000 && explicit.estimateCardinality() < 11000);

        // Test full merge explicit
        explicit = new Hll128();
        for (int i = 0; i < 10; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            explicit.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        full = new Hll128();
        for (int i = 1; i < 10000; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            full.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        Assert.assertEquals(Hll128.HLL_DATA_EXPLICIT, explicit.getType());
        full.merge(explicit);
        Assert.assertEquals(Hll128.HLL_DATA_FULL, full.getType());
        Assert.assertTrue(full.estimateCardinality() > 9000 && full.estimateCardinality() < 11000);

        // Test full merge full
        Hll128 full1 = new Hll128();
        Hll128 full2 = new Hll128();
        for (int i = 1; i < 10000; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            full1.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        for (int i = 5000; i < 15000; i++) {
            byte[] v = StringUtils.getBytesUtf8(String.valueOf(i));
            full2.update(Hll.hash64(v, v.length, Hll.SEED));
        }
        Assert.assertEquals(Hll128.HLL_DATA_FULL, full1.getType());
        Assert.assertEquals(Hll128.HLL_DATA_FULL, full2.getType());
        full1.merge(full2);
        Assert.assertEquals(Hll128.HLL_DATA_FULL, full1.getType());
        Assert.assertTrue(full1.estimateCardinality() > 13500 && full1.estimateCardinality() < 16500);
    }

}
