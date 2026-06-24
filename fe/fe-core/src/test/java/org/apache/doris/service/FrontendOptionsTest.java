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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.InetAddress;

public class FrontendOptionsTest {
    private MockedStatic<InetAddresses> mockedInetAddresses;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        mockedInetAddresses = Mockito.mockStatic(InetAddresses.class, Mockito.CALLS_REAL_METHODS);
        mockedInetAddresses.when(() -> InetAddresses.toAddrString(Mockito.nullable(InetAddress.class)))
                .thenReturn("2408:400a:5a:ea00:2fb5:112e:39dd:9bba%eth0");
    }

    @After
    public void tearDown() {
        if (mockedInetAddresses != null) {
            mockedInetAddresses.close();
        }
    }

    @Test
    public void testGetIpByLocalAddr() {
        String ip = FrontendOptions.getIpByLocalAddr(null);
        Assert.assertEquals("2408:400a:5a:ea00:2fb5:112e:39dd:9bba", ip);
    }
}
