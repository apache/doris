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

import org.apache.doris.mysql.privilege.PasswordPolicy.FailedLoginPolicy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PasswordPolicyTest {

    @Test
    public void testFailedLoginPolicyBasicLock() {
        FailedLoginPolicy policy = new FailedLoginPolicy();
        policy.numFailedLogin = 3;
        policy.passwordLockSeconds = 60;

        // First 2 failures should not lock
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertFalse(policy.onFailedLogin());
        // 3rd failure should lock
        Assertions.assertTrue(policy.onFailedLogin());
        Assertions.assertTrue(policy.isLocked());
    }

    @Test
    public void testFailedLoginPolicyRelockAfterExpiry() {
        FailedLoginPolicy policy = new FailedLoginPolicy();
        policy.numFailedLogin = 3;
        policy.passwordLockSeconds = 5;

        // Trigger first lock
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertTrue(policy.onFailedLogin());
        Assertions.assertTrue(policy.isLocked());

        // Simulate lock expiry by setting lockTime to the past
        policy.lockTime.set(System.currentTimeMillis() - 6000);
        Assertions.assertFalse(policy.isLocked());

        // Now trigger re-lock: counter should reset and start counting again
        // 1st failed login after expiry — counter resets from 3 to 0, then increments to 1
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertFalse(policy.isLocked());
        // 2nd
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertFalse(policy.isLocked());
        // 3rd should lock again
        Assertions.assertTrue(policy.onFailedLogin());
        Assertions.assertTrue(policy.isLocked());
    }

    @Test
    public void testFailedLoginPolicyStillLockedWhileActive() {
        FailedLoginPolicy policy = new FailedLoginPolicy();
        policy.numFailedLogin = 2;
        policy.passwordLockSeconds = 60;

        // Lock the account
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertTrue(policy.onFailedLogin());
        Assertions.assertTrue(policy.isLocked());

        // While still locked, onFailedLogin should still return true
        Assertions.assertTrue(policy.onFailedLogin());
        Assertions.assertTrue(policy.isLocked());
    }

    @Test
    public void testFailedLoginPolicyManualUnlock() {
        FailedLoginPolicy policy = new FailedLoginPolicy();
        policy.numFailedLogin = 2;
        policy.passwordLockSeconds = 60;

        // Lock the account
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertTrue(policy.onFailedLogin());
        Assertions.assertTrue(policy.isLocked());

        // Manual unlock
        policy.unlock();
        Assertions.assertFalse(policy.isLocked());

        // Should be able to re-lock
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertTrue(policy.onFailedLogin());
        Assertions.assertTrue(policy.isLocked());
    }

    @Test
    public void testFailedLoginPolicyDisabled() {
        FailedLoginPolicy policy = new FailedLoginPolicy();
        // Both disabled by default (0)
        Assertions.assertFalse(policy.onFailedLogin());
        Assertions.assertFalse(policy.isLocked());

        // Only numFailedLogin set
        policy.numFailedLogin = 3;
        policy.passwordLockSeconds = 0;
        Assertions.assertFalse(policy.onFailedLogin());

        // Only passwordLockSeconds set
        policy.numFailedLogin = 0;
        policy.passwordLockSeconds = 60;
        Assertions.assertFalse(policy.onFailedLogin());
    }
}
