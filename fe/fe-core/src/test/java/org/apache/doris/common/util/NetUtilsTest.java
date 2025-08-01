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

import com.google.common.net.InetAddresses;
import org.junit.Assert;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;

public class NetUtilsTest {

    @Test
    public void testConvertIp() throws Exception {
        long ipValue = 3232235786L;
        InetAddress ip = InetAddress.getByName("192.168.1.10");
        Assert.assertTrue(ip instanceof Inet4Address);
        Assert.assertEquals(ipValue, NetUtils.inet4AddressToLong((Inet4Address) ip));
        Inet4Address convertIp = NetUtils.longToInet4Address(ipValue);
        Assert.assertEquals(ip, convertIp);
        System.out.println(InetAddresses.forString("192.168.1.10").toString());
    }
}
