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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TUniqueId;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class DebugUtilTest {
    @Test
    public void testGetUint() {
        Pair<Double, String> result;
        result = DebugUtil.getUint(2000000000L);
        Assert.assertEquals(Double.valueOf(2.0), result.first);
        Assert.assertEquals(result.second, "B");

        result = DebugUtil.getUint(1234567L);
        Assert.assertEquals(result.first, Double.valueOf(1.234567));
        Assert.assertEquals(result.second, "M");

        result = DebugUtil.getUint(1234L);
        Assert.assertEquals(result.first, Double.valueOf(1.234));
        Assert.assertEquals(result.second, "K");

        result = DebugUtil.getUint(123L);
        Assert.assertEquals(result.first, Double.valueOf(123.0));
        Assert.assertEquals(result.second, "");
    }

    @Test
    public void testGetPrettyStringMs() {
        // 6hour1min
        Assert.assertEquals("6hour1min", DebugUtil.getPrettyStringMs(21660222));

        // 1min222ms
        Assert.assertEquals("1min", DebugUtil.getPrettyStringMs(60222));

        // 2s222ms
        Assert.assertEquals("2sec222ms", DebugUtil.getPrettyStringMs(2222));

        // 22ms
        Assert.assertEquals("22ms", DebugUtil.getPrettyStringMs(22));
    }

    @Test
    public void testGetByteUint() {
        Pair<Double, String> result;
        result = DebugUtil.getByteUint(0);
        Assert.assertEquals(result.first,  Double.valueOf(0.0));
        Assert.assertEquals(result.second, "");

        result = DebugUtil.getByteUint(123);     // B
        Assert.assertEquals(result.first, Double.valueOf(123.0));
        Assert.assertEquals(result.second, "B");

        result = DebugUtil.getByteUint(123456);  // K
        Assert.assertEquals(result.first, Double.valueOf(120.5625));
        Assert.assertEquals(result.second, "KB");

        result = DebugUtil.getByteUint(1234567);  // M
        Assert.assertEquals(result.first, Double.valueOf(1.1773748397827148));
        Assert.assertEquals(result.second, "MB");

        result = DebugUtil.getByteUint(1234567890L);  // G
        Assert.assertEquals(result.first, Double.valueOf(1.1497809458523989));
        Assert.assertEquals(result.second, "GB");
    }

    @Test
    public void testUtilGetStackTrace() {
        Exception e1 = new Exception("exception1");
        DdlException e2 = new DdlException("exception2", e1);
        e2.printStackTrace();
        System.out.println(Util.getRootCauseStack(e2));
        Assert.assertTrue(Util.getRootCauseStack(e2).contains("java.lang.Exception: exception1"));

        DdlException e3 = new DdlException("only one exception");
        System.out.println(Util.getRootCauseStack(e3));
        Assert.assertTrue(Util.getRootCauseStack(e3)
                .contains("org.apache.doris.common.DdlException: errCode = 2, detailMessage = only one exception"));
        Assert.assertEquals("unknown", Util.getRootCauseStack(null));
    }

    @Test
    public void testParseIdFromString() {
        // test null
        TUniqueId nullTUniqueId = null;
        try {
            nullTUniqueId = DebugUtil.parseTUniqueIdFromString(null);
        } catch (NumberFormatException e) {
            Assert.assertTrue("invalid query id".equals(e.getMessage()));
        }
        Assert.assertTrue(nullTUniqueId == null);


        try {
            nullTUniqueId = DebugUtil.parseTUniqueIdFromString("");
        } catch (NumberFormatException e) {
            Assert.assertTrue("invalid query id".equals(e.getMessage()));
        }
        Assert.assertTrue(nullTUniqueId == null);

        Assert.assertEquals(new TUniqueId(), DebugUtil.parseTUniqueIdFromString("0-0"));

        try {
            nullTUniqueId = DebugUtil.parseTUniqueIdFromString("INVALID-STRING");
        } catch (NumberFormatException e) {
            Assert.assertTrue(e.getMessage().contains("For input string"));
        }
        Assert.assertTrue(nullTUniqueId == null);

        for (int i = 0; i < 100; i++) {
            UUID uuid = UUID.randomUUID();
            TUniqueId originTQueryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            String originStrQueryId = DebugUtil.printId(originTQueryId);

            TUniqueId convertedTQueryId = DebugUtil.parseTUniqueIdFromString(originStrQueryId);
            String convertedStrQueryId = DebugUtil.printId(convertedTQueryId);

            Assert.assertTrue(originTQueryId.hi == convertedTQueryId.hi);
            Assert.assertTrue(originTQueryId.lo == convertedTQueryId.lo);
            Assert.assertTrue(originStrQueryId.equals(convertedStrQueryId));
        }
    }
}
