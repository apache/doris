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

package org.apache.doris.datasource.property;

import  org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class ParamRulesTest {

    @Test
    void testRequire_passesWhenValueIsPresent() {
        ParamRules rules = new ParamRules()
                .require("non-empty", "Field is required");
        Assertions.assertDoesNotThrow(new Executable() {
            @Override
            public void execute() {
                rules.validate();
            }
        });
    }

    @Test
    void testRequire_failsWhenValueIsMissing() {
        ParamRules rules = new ParamRules()
                .require("  ", "Field is required");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                rules.validate();
            }
        });
        Assertions.assertEquals("Field is required", e.getMessage());
    }

    @Test
    void testMutuallyExclusive_passesWhenOnlyOneIsPresent() {
        ParamRules rules = new ParamRules()
                .mutuallyExclusive("value1", "", "Cannot be both set");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testMutuallyExclusive_failsWhenBothArePresent() {
        ParamRules rules = new ParamRules()
                .mutuallyExclusive("val1", "val2", "Cannot be both set");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> rules.validate());
        Assertions.assertEquals("Cannot be both set", e.getMessage());
    }

    @Test
    void testRequireIf_passesWhenConditionDoesNotMatch() {
        ParamRules rules = new ParamRules()
                .requireIf("simple", "kerberos", "", "Must be set when kerberos");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireIf_passesWhenConditionMatchesAndRequiredIsPresent() {
        ParamRules rules = new ParamRules()
                .requireIf("kerberos", "kerberos", "keytab", "Must be set when kerberos");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireIf_failsWhenConditionMatchesAndRequiredMissing() {
        ParamRules rules = new ParamRules()
                .requireIf("kerberos", "kerberos", "", "Must be set when kerberos");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> rules.validate());
        Assertions.assertEquals("Must be set when kerberos", e.getMessage());
    }

    @Test
    void testRequireAllIfPresent_passesWhenConditionAbsent() {
        ParamRules rules = new ParamRules()
                .requireAllIfPresent("", new String[]{"a", "b"}, "All required");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireAllIfPresent_passesWhenAllArePresent() {
        ParamRules rules = new ParamRules()
                .requireAllIfPresent("condition", new String[]{"a", "b"}, "All required");
        Assertions.assertDoesNotThrow(() -> rules.validate());
    }

    @Test
    void testRequireAllIfPresent_failsWhenAnyMissing() {
        ParamRules rules = new ParamRules()
                .requireAllIfPresent("set", new String[]{"a", ""}, "Missing dependency");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> rules.validate());
        Assertions.assertEquals("Missing dependency", e.getMessage());
    }

    @Test
    void testValidateWithPrefix() {
        ParamRules rules = new ParamRules()
                .require("", "Missing value");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> rules.validate("Config Error"));
        Assertions.assertEquals("Config Error: Missing value", e.getMessage());
    }
}
