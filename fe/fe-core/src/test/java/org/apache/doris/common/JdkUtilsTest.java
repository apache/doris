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

import org.apache.doris.common.util.JdkUtils;

import org.junit.Assert;
import org.junit.Test;

public class JdkUtilsTest {

    @Test
    public void testNormal() {
        Assert.assertTrue(JdkUtils.checkJavaVersion());
    }

    @Test
    public void testFunctions() {
        String versionStr = JdkUtils.getJavaVersionFromFullVersion("java full version \"1.8.0_131-b11\"");
        Assert.assertEquals("1.8.0_131-b11", versionStr);
        versionStr = JdkUtils.getJavaVersionFromFullVersion("openjdk full version \"13.0.1+9\"");
        Assert.assertEquals("13.0.1+9", versionStr);

        int version = JdkUtils.getJavaVersionAsInteger("1.8.0_131-b11");
        Assert.assertEquals(8, version);
        version = JdkUtils.getJavaVersionAsInteger("1.7.0_79-b15");
        Assert.assertEquals(7, version);
        version = JdkUtils.getJavaVersionAsInteger("13.0.1+9");
        Assert.assertEquals(13, version);
        version = JdkUtils.getJavaVersionAsInteger("11.0.0+7");
        Assert.assertEquals(11, version);
    }
}
