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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;

import org.junit.Assert;
import org.junit.Test;


public class UserIdentityTest {

    @Test
    public void test() {
        UserIdentity userIdent = new UserIdentity("cmy", "192.%");
        userIdent.setIsAnalyzed();

        String str = "'" + "cmy" + "'@'192.%'";
        Assert.assertEquals(str, userIdent.toString());

        UserIdentity userIdent2 = UserIdentity.fromString(str);
        Assert.assertEquals(userIdent2.toString(), userIdent.toString());

        String str2 = "'walletdc_write'@['cluster-leida.orp.all']";
        userIdent = UserIdentity.fromString(str2);
        Assert.assertNotNull(userIdent);
        Assert.assertTrue(userIdent.isDomain());
        userIdent.setIsAnalyzed();
        Assert.assertEquals(str2, userIdent.toString());
    }

}
