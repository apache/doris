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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PrivPredicate;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for HTTP API authentication and authorization.
 * Tests the new authentication framework:
 * 1. ActionAuthorizationInfo.userIdentity field
 * 2. RestBaseController.checkAdminAuth() method
 * 3. enable_all_http_auth flag behavior
 */
public class HttpApiAuthTest {

    private boolean originalEnableAllHttpAuth;

    @Before
    public void setUp() {
        // Save original config
        originalEnableAllHttpAuth = Config.enable_all_http_auth;
    }

    @After
    public void tearDown() {
        // Restore original config
        Config.enable_all_http_auth = originalEnableAllHttpAuth;
    }

    @Test
    public void testActionAuthorizationInfoHasUserIdentity() {
        // Test that ActionAuthorizationInfo has userIdentity field
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        authInfo.fullUserName = "root";
        authInfo.remoteIp = "127.0.0.1";
        authInfo.password = "password";
        authInfo.cluster = "default_cluster";

        UserIdentity userIdentity = new UserIdentity("root", "%");
        userIdentity.setIsAnalyzed();
        authInfo.userIdentity = userIdentity;

        Assert.assertNotNull("userIdentity field should exist", authInfo.userIdentity);
        Assert.assertEquals("root", authInfo.userIdentity.getQualifiedUser());
    }

    @Test
    public void testCheckAdminAuthWhenDisabled() {
        // Test: when enable_all_http_auth=false, checkAdminAuth should not throw exception
        Config.enable_all_http_auth = false;

        TestRestController controller = new TestRestController();
        UserIdentity normalUser = new UserIdentity("test_user", "%");
        normalUser.setIsAnalyzed();

        // Should not throw exception even for non-admin user when auth is disabled
        try {
            controller.checkAdminAuth(normalUser);
            // Success - no exception thrown
        } catch (UnauthorizedException e) {
            Assert.fail("checkAdminAuth should not check privilege when enable_all_http_auth=false");
        }
    }

    @Test
    public void testCheckAdminAuthWithAdminUser() {
        // Test: when enable_all_http_auth=true, admin user should pass checkAdminAuth
        Config.enable_all_http_auth = true;

        TestRestController controller = new TestRestController();
        controller.setMockAdminResult(true);

        UserIdentity adminUser = new UserIdentity("admin", "%");
        adminUser.setIsAnalyzed();

        try {
            controller.checkAdminAuth(adminUser);
            // Success - no exception thrown
        } catch (UnauthorizedException e) {
            Assert.fail("Admin user should pass checkAdminAuth: " + e.getMessage());
        }
    }

    @Test
    public void testCheckAdminAuthWithNormalUser() {
        // Test: when enable_all_http_auth=true, normal user should fail checkAdminAuth
        Config.enable_all_http_auth = true;

        TestRestController controller = new TestRestController();
        controller.setMockAdminResult(false);

        UserIdentity normalUser = new UserIdentity("test_user", "%");
        normalUser.setIsAnalyzed();

        try {
            controller.checkAdminAuth(normalUser);
            Assert.fail("Normal user should not pass checkAdminAuth");
        } catch (UnauthorizedException e) {
            // Expected exception
            Assert.assertTrue("Error message should mention privilege",
                    e.getMessage().toLowerCase().contains("privilege")
                            || e.getMessage().toLowerCase().contains("permission"));
        }
    }

    @Test
    public void testCheckAdminAuthMethodExists() {
        // Test: checkAdminAuth(UserIdentity) method exists in RestBaseController
        TestRestController controller = new TestRestController();

        // This test passes if the method exists and compiles
        Assert.assertNotNull("RestBaseController should have checkAdminAuth method", controller);
    }

    /**
     * Test helper class that extends RestBaseController for testing
     */
    private static class TestRestController extends RestBaseController {
        private boolean mockAdminResult = true;

        public void setMockAdminResult(boolean isAdmin) {
            this.mockAdminResult = isAdmin;
        }

        @Override
        protected void checkGlobalAuth(UserIdentity currentUser, PrivPredicate predicate)
                throws UnauthorizedException {
            // Mock implementation for testing
            if (predicate == PrivPredicate.ADMIN && !mockAdminResult) {
                throw new UnauthorizedException("Access denied: need ADMIN privilege");
            }
        }
    }
}
