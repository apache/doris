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

package org.apache.doris.common.proc;

import org.apache.doris.common.util.FormatIpUtil;

import org.junit.Assert;
import org.junit.Test;

public class FrontendsProcNodeTest {

    @Test
    public void testFormatIp() throws Exception {
        // Test input and expected output for IPv6
        String inputIPv6 = "fe80:0:0:0:20c:29ff:fef9:3a18";
        String expectedIPv6 = "fe80::20c:29ff:fef9:3a18";

        // Test input and expected output for IPv4
        String inputIPv4 = "192.168.1.1";
        String expectedIPv4 = "192.168.1.1";  // No change for IPv4

        // Test IPv6
        String actualIPv6 = FormatIpUtil.formatIp(inputIPv6);
        Assert.assertEquals(expectedIPv6, actualIPv6);

        // Test IPv4
        String actualIPv4 = FormatIpUtil.formatIp(inputIPv4);
        Assert.assertEquals(expectedIPv4, actualIPv4);
    }
}
