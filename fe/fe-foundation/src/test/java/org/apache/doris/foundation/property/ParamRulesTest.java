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

package org.apache.doris.foundation.property;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParamRulesTest {

    @Test
    void testRequirePassesWhenValueIsPresent() {
        ParamRules rules = new ParamRules().require("non-empty", "Field is required");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireFailsWhenValueIsMissing() {
        ParamRules rules = new ParamRules().require("  ", "Field is required");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, rules::validate);
        Assertions.assertEquals("Field is required", e.getMessage());
    }

    @Test
    void testMutuallyExclusivePassesWhenOnlyOneIsPresent() {
        ParamRules rules = new ParamRules().mutuallyExclusive("value1", "", "Cannot be both set");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testMutuallyExclusiveFailsWhenBothArePresent() {
        ParamRules rules = new ParamRules().mutuallyExclusive("val1", "val2", "Cannot be both set");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, rules::validate);
        Assertions.assertEquals("Cannot be both set", e.getMessage());
    }

    @Test
    void testRequireIfPassesWhenConditionDoesNotMatch() {
        ParamRules rules = new ParamRules().requireIf("simple", "kerberos", "", "Must be set when kerberos");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireIfPassesWhenConditionMatchesAndRequiredIsPresent() {
        ParamRules rules = new ParamRules().requireIf("kerberos", "kerberos", "keytab", "Must be set when kerberos");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireIfFailsWhenConditionMatchesAndRequiredMissing() {
        ParamRules rules = new ParamRules().requireIf("kerberos", "kerberos", "", "Must be set when kerberos");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, rules::validate);
        Assertions.assertEquals("Must be set when kerberos", e.getMessage());
    }

    @Test
    void testRequireAllIfPresentPassesWhenConditionAbsent() {
        ParamRules rules = new ParamRules().requireAllIfPresent("", new String[] {"a", "b"}, "All required");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireAllIfPresentPassesWhenAllArePresent() {
        ParamRules rules = new ParamRules().requireAllIfPresent("condition", new String[] {"a", "b"}, "All required");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireAllIfPresentFailsWhenAnyMissing() {
        ParamRules rules = new ParamRules().requireAllIfPresent("set", new String[] {"a", ""}, "Missing dependency");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, rules::validate);
        Assertions.assertEquals("Missing dependency", e.getMessage());
    }

    @Test
    void testValidateWithPrefix() {
        ParamRules rules = new ParamRules().require("", "Missing value");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> rules.validate("Config Error"));
        Assertions.assertEquals("Config Error: Missing value", e.getMessage());
    }

    @Test
    void testRequireTogether() {
        ParamRules rules = new ParamRules()
                .requireTogether(new String[] {"accessKey", ""}, "Both accessKey and secretKey are required together");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, rules::validate);
        Assertions.assertEquals("Both accessKey and secretKey are required together", e.getMessage());

        ParamRules rightRule = new ParamRules().requireTogether(
                new String[] {"accessKey", "secretKey"},
                "Both accessKey and secretKey are required together");
        Assertions.assertDoesNotThrow(() -> rightRule.validate());
    }

    @Test
    void testRequireAtLeastOne() {
        ParamRules rules = new ParamRules()
                .requireAtLeastOne(new String[] {""}, "At least one of accessKey and iamrole is required");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, rules::validate);
        Assertions.assertEquals("At least one of accessKey and iamrole is required", e.getMessage());

        Assertions.assertDoesNotThrow(() -> new ParamRules()
                .requireAtLeastOne(new String[] {"accessKey", "iamrole"},
                        "At least one of accessKey and iamrole is required")
                .validate());
        Assertions.assertDoesNotThrow(() -> new ParamRules()
                .requireAtLeastOne(new String[] {"accessKey", ""},
                        "At least one of accessKey and iamrole is required")
                .validate());
        Assertions.assertDoesNotThrow(() -> new ParamRules()
                .requireAtLeastOne(new String[] {"", "iamrole"},
                        "At least one of accessKey and iamrole is required")
                .validate());
    }

    @Test
    void testComplexLambdaValidation() {
        String username = "alice";
        String password = "";
        String email = "alice@example.com";
        int age = 17;
        int maxAge = 100;

        ParamRules rules = new ParamRules();
        rules.check(() -> username == null || username.isEmpty(), "Username must not be empty")
                .check(() -> password == null || password.length() < 6, "Password must be at least 6 characters")
                .check(() -> !email.contains("@"), "Email must be valid")
                .check(() -> age < 18 || age > maxAge, "Age must be between 18 and 100")
                .check(() -> username.equals(email), "Username and email cannot be the same");

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> rules.validate("Validation Failed"));
        Assertions.assertTrue(ex.getMessage().startsWith("Validation Failed: "));
    }

    @Test
    void testComplexLambdaValidationSuccess() {
        String username = "alice";
        String password = "password123";
        String email = "alice@example.com";
        int age = 25;
        int maxAge = 100;

        Assertions.assertDoesNotThrow(() -> new ParamRules()
                .check(() -> username == null || username.isEmpty(), "Username must not be empty")
                .check(() -> password == null || password.length() < 6, "Password must be at least 6 characters")
                .check(() -> !email.contains("@"), "Email must be valid")
                .check(() -> age < 18 || age > maxAge, "Age must be between 18 and 100")
                .check(() -> username.equals(email), "Username and email cannot be the same")
                .validate());
    }
}
