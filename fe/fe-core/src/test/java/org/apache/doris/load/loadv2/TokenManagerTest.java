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

package org.apache.doris.load.loadv2;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TokenManagerTest {

    @Before
    public void runBefore() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testTokenCheck() throws UserException {
        TokenManager tokenManager = new TokenManager();
        tokenManager.start();
        String token = tokenManager.acquireToken();
        Assert.assertTrue(tokenManager.checkAuthToken(token));
    }

    @Test
    public void testSameToken() throws UserException {
        TokenManager tokenManager = new TokenManager();
        tokenManager.start();
        String token1 = tokenManager.acquireToken();
        String token2 = tokenManager.acquireToken();
        Assert.assertNotNull(token1);
        Assert.assertEquals(token1, token2);
    }
}
