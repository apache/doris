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

package org.apache.doris.service;

import org.apache.doris.common.AnalysisException;

import com.google.common.net.InetAddresses;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class FrontendOptionsTest {
    @Mocked
    private InetAddresses inetAddresses;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        new Expectations() {
            {
                inetAddresses.toAddrString((InetAddress) any);
                minTimes = 0;
                result = "2408:400a:5a:ea00:2fb5:112e:39dd:9bba%eth0";
            }
        };
    }

    @Test
    public void testGetIpByLocalAddr() {
        String ip = FrontendOptions.getIpByLocalAddr(null);
        Assert.assertEquals("2408:400a:5a:ea00:2fb5:112e:39dd:9bba", ip);
    }
}
