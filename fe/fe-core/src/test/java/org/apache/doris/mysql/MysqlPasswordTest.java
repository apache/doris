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

package org.apache.doris.mysql;

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

public class MysqlPasswordTest {
    @Test
    public void testMakePassword() {
        Assert.assertEquals("*6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4",
                new String(MysqlPassword.makeScrambledPassword("mypass")));

        Assert.assertEquals("", new String(MysqlPassword.makeScrambledPassword("")));

        // null
        Assert.assertEquals("", new String(MysqlPassword.makeScrambledPassword(null)));

        Assert.assertEquals("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32",
                new String(MysqlPassword.makeScrambledPassword("aBc@321")));

        Assert.assertEquals(new String(new byte[0]),
                new String(MysqlPassword.getSaltFromPassword(new byte[0])));

    }

    @Test
    public void testCheckPass() throws UnsupportedEncodingException {
        // client
        byte[] publicSeed = MysqlPassword.createRandomString(20);
        byte[] codePass = MysqlPassword.scramble(publicSeed, "mypass");

        Assert.assertTrue(MysqlPassword.checkScramble(codePass,
                publicSeed,
                MysqlPassword.getSaltFromPassword("*6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4".getBytes("UTF-8"))));

        Assert.assertFalse(MysqlPassword.checkScramble(codePass,
                publicSeed,
                MysqlPassword.getSaltFromPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32".getBytes("UTF-8"))));
    }

    @Test
    public void testCheckPassword() throws AnalysisException {
        Assert.assertEquals("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32",
                new String(MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32")));

        Assert.assertEquals("", new String(MysqlPassword.checkPassword(null)));
    }

    @Test(expected = AnalysisException.class)
    public void testCheckPasswdFail() throws AnalysisException {
        MysqlPassword.checkPassword("*9A6EC1164108A8D3DA3BE3F35A56F6499B6FC32");
        Assert.fail("No exception throws");
    }

    @Test(expected = AnalysisException.class)
    public void testCheckPasswdFail2() throws AnalysisException {
        Assert.assertNotNull(MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32"));
        MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC3H");
        Assert.fail("No exception throws");
    }

}
