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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ParamRules {

    private final List<Runnable> rules = new ArrayList<>();

    /**
     * Require that the given value is present (non-null and not empty).
     *
     * @param value the value to check
     * @param errorMessage the error message to throw if validation fails
     * @return this ParamRules instance for chaining
     */
    public ParamRules require(String value, String errorMessage) {
        rules.add(() -> {
            if (!isPresent(value)) {
                throw new IllegalArgumentException(errorMessage);
            }
        });
        return this;
    }

    /**
     * Require that two parameters are mutually exclusive, i.e. both cannot be present at the same time.
     *
     * @param value1 first value to check
     * @param value2 second value to check
     * @param errorMessage error message to throw if both are present
     * @return this ParamRules instance for chaining
     */
    public ParamRules mutuallyExclusive(String value1, String value2, String errorMessage) {
        rules.add(() -> {
            if (isPresent(value1) && isPresent(value2)) {
                throw new IllegalArgumentException(errorMessage);
            }
        });
        return this;
    }

    /**
     * Require that the requiredValue is present if the conditionValue equals the expectedValue.
     *
     * @param conditionValue the value to compare
     * @param expectedValue the expected value for the condition
     * @param requiredValue the value required if condition matches
     * @param errorMessage error message to throw if requiredValue is missing
     * @return this ParamRules instance for chaining
     */
    public ParamRules requireIf(String conditionValue, String expectedValue, String requiredValue,
                                String errorMessage) {
        rules.add(() -> {
            if (Objects.equals(expectedValue, conditionValue) && !isPresent(requiredValue)) {
                throw new IllegalArgumentException(errorMessage);
            }
        });
        return this;
    }

    /**
     * Require that the requiredValues are present if conditionValue is non-empty and equals expectedValue.
     *
     * @param conditionValue the value to check for presence and match
     * @param expectedValue the expected value to match against
     * @param requiredValues values that must be present if conditionValue matches expectedValue
     * @param errorMessage error message to throw if any required value is missing
     * @return this ParamRules instance for chaining
     */
    public ParamRules requireIf(String conditionValue, String expectedValue, String[] requiredValues,
                                String errorMessage) {
        rules.add(() -> {
            if (isPresent(conditionValue) && Objects.equals(expectedValue, conditionValue)) {
                for (String val : requiredValues) {
                    if (!isPresent(val)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                }
            }
        });
        return this;
    }

    /**
     * Execute all validation rules.
     *
     * @throws IllegalArgumentException if any rule fails
     */
    public void validate() {
        for (Runnable rule : rules) {
            rule.run();
        }
    }

    /**
     * Execute all validation rules with a prefix message added to any thrown exception.
     *
     * @param prefixMessage the prefix to prepend to error messages
     * @throws IllegalArgumentException if any rule fails, with prefixed message
     */
    public void validate(String prefixMessage) {
        try {
            validate();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(prefixMessage + ": " + e.getMessage(), e);
        }
    }

    /**
     * Require that all values in requiredValues are present if the conditionValue is present.
     *
     * @param conditionValue the value whose presence triggers the requirement
     * @param requiredValues array of values all required if conditionValue is present
     * @param errorMessage error message to throw if any required value is missing
     * @return this ParamRules instance for chaining
     */
    public ParamRules requireAllIfPresent(String conditionValue, String[] requiredValues, String errorMessage) {
        rules.add(() -> {
            if (isPresent(conditionValue)) {
                for (String val : requiredValues) {
                    if (!isPresent(val)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                }
            }
        });
        return this;
    }

    /**
     * Require that if a is present and equals expectedValue, then none of the forbiddenValues may be present.
     *
     * @param a the condition value
     * @param expectedValue the expected value of a
     * @param forbiddenValues values that must not be present if a matches expectedValue
     * @param errorMessage the error message to throw if any forbiddenValue is present
     * @return this ParamRules instance for chaining
     */
    public ParamRules forbidIf(String a, String expectedValue, String[] forbiddenValues, String errorMessage) {
        rules.add(() -> {
            if (Objects.equals(a, expectedValue)) {
                for (String val : forbiddenValues) {
                    if (isPresent(val)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                }
            }
        });
        return this;
    }

    // --------- Utility Methods ----------

    /**
     * Checks if a string value is present (non-null and not empty after trimming).
     *
     * @param value the string to check
     * @return true if the value is non-null and not empty; false otherwise
     */
    private boolean isPresent(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
