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

public class CIDRTest {
    @Test
    public void testWrongFormat() {
        // no mask
        try {
            CIDR cidr = new CIDR("192.168.17.0/");
            // should not be here
            Assert.assertTrue(false);
        }  catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        // mask is too big
        try {
            CIDR cidr = new CIDR("192.168.17.0/88");
            // should not be here
            Assert.assertTrue(false);
        }  catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        // ip is too short
        try {
            CIDR cidr = new CIDR("192.168./88");
            // should not be here
            Assert.assertTrue(false);
        }  catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testNormal() throws UserException {
        // the real value is 10.1.16.0/20
        CIDR cidr = new CIDR("192.168.17.0/20");
        Assert.assertEquals("192.168.17.0", cidr.getIP());
        Assert.assertEquals("255.255.240.0", cidr.getNetmask());
        Assert.assertEquals("192.168.16.0/20", cidr.getCIDR());
        System.out.println("range: " + cidr.getIpRange());
    }

    @Test
    public void testContain() {
        CIDR cidr = new CIDR("192.168.17.0/16");
        Assert.assertTrue(cidr.contains("192.168.88.88"));
        Assert.assertFalse(cidr.contains("192.2.88.88"));
    }
}
