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

package org.apache.doris.common;

import org.junit.Assert;
import org.junit.Test;

public class CidrTest {
    @Test
    public void testWrongFormat() {
        // no mask
        try {
            new CIDR("192.168.17.0/");
            // should not be here
            Assert.fail();
        }  catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        // mask is too big
        try {
            new CIDR("192.168.17.0/88");
            // should not be here
            Assert.fail();
        }  catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        // ip is too short
        try {
            new CIDR("192.168./88");
            // should not be here
            Assert.fail();
        }  catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testNormal() throws Exception {
        // the real value is 10.1.16.0/20
        CIDR cidr = new CIDR("192.168.17.0/20");
        Assert.assertEquals("192.168.17.0", cidr.getIP());
    }

    @Test
    public void testContain() {
        CIDR cidrV4 = new CIDR("192.168.17.0/16");
        Assert.assertTrue(cidrV4.contains("192.168.88.88"));
        Assert.assertFalse(cidrV4.contains("192.2.88.88"));

        CIDR cidr2V4 = new CIDR("192.168.17.0/20");
        Assert.assertTrue(cidr2V4.contains("192.168.31.1"));
        Assert.assertFalse(cidr2V4.contains("192.168.32.1"));

        CIDR cidrV6 = new CIDR("fdbd:ff1:ce00:1c26::d8/64");
        Assert.assertTrue(cidrV6.contains("fdbd:ff1:ce00:1c26::d8"));
        Assert.assertTrue(cidrV6.contains("fdbd:ff1:ce00:1c26::12:234b:def8"));
        Assert.assertFalse(cidrV6.contains("fdbd:ff1:ce00:1c27::12:234b:def8"));

        CIDR cidr2V6 = new CIDR("fdbd:ff1:ce00:1c26:1000::d8/68");
        Assert.assertTrue(cidr2V6.contains("fdbd:ff1:ce00:1c26:1a3f:12:234b:def8"));
        Assert.assertFalse(cidr2V6.contains("fdbd:ff1:ce00:1c26::d8"));
    }
}
