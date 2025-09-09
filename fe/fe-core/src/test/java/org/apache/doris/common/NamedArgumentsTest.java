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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class NamedArgumentsTest {

        private NamedArguments namedArguments;

        @BeforeEach
        void setUp() {
                namedArguments = new NamedArguments();
        }

        @Test
        void testRegisterRequiredArgument() {
                // Test registering a required argument
                namedArguments.registerRequiredArgument("port", "Server port",
                                ArgumentParsers.positiveInt("port"));

                Assertions.assertEquals(1, namedArguments.size());
                Assertions.assertFalse(namedArguments.isEmpty());

                // Check argument definition
                NamedArguments.ArgumentDefinition<?> argDef = namedArguments.getArgumentDefinitions().get(0);
                Assertions.assertEquals("port", argDef.getName());
                Assertions.assertEquals("Server port", argDef.getDescription());
                Assertions.assertTrue(argDef.isRequired());
                Assertions.assertNull(argDef.getDefaultValue());
        }

        @Test
        void testRegisterOptionalArgument() {
                // Test registering an optional argument with default value
                namedArguments.registerOptionalArgument("timeout", "Request timeout in seconds", 30,
                                ArgumentParsers.positiveInt("timeout"));

                Assertions.assertEquals(1, namedArguments.size());
                Assertions.assertFalse(namedArguments.isEmpty());

                // Check argument definition
                NamedArguments.ArgumentDefinition<?> argDef = namedArguments.getArgumentDefinitions().get(0);
                Assertions.assertEquals("timeout", argDef.getName());
                Assertions.assertEquals("Request timeout in seconds", argDef.getDescription());
                Assertions.assertFalse(argDef.isRequired());
                Assertions.assertEquals(30, argDef.getDefaultValue());
        }

        @Test
        void testValidateWithRequiredArguments() throws AnalysisException {
                // Register required arguments
                namedArguments.registerRequiredArgument("host", "Server host",
                                ArgumentParsers.nonEmptyString("host"));
                namedArguments.registerRequiredArgument("port", "Server port",
                                ArgumentParsers.positiveInt("port"));

                // Test successful validation
                Map<String, String> properties = new HashMap<>();
                properties.put("host", "localhost");
                properties.put("port", "8080");

                namedArguments.validate(properties);

                // Check parsed values
                Assertions.assertEquals("localhost", namedArguments.getString("host"));
                Assertions.assertEquals(Integer.valueOf(8080), namedArguments.getInt("port"));
        }

        @Test
        void testValidateWithOptionalArguments() throws AnalysisException {
                // Register optional arguments with defaults
                namedArguments.registerOptionalArgument("timeout", "Request timeout", 30,
                                ArgumentParsers.positiveInt("timeout"));
                namedArguments.registerOptionalArgument("enabled", "Feature enabled", true,
                                ArgumentParsers.booleanValue("enabled"));

                // Test validation with no provided values (should use defaults)
                Map<String, String> properties = new HashMap<>();
                namedArguments.validate(properties);

                // Check default values are used
                Assertions.assertEquals(Integer.valueOf(30), namedArguments.getInt("timeout"));
                Assertions.assertEquals(Boolean.TRUE, namedArguments.getBoolean("enabled"));

                // Test validation with provided values
                properties.put("timeout", "60");
                properties.put("enabled", "false");
                namedArguments.validate(properties);

                // Check provided values are used
                Assertions.assertEquals(Integer.valueOf(60), namedArguments.getInt("timeout"));
                Assertions.assertEquals(Boolean.FALSE, namedArguments.getBoolean("enabled"));
        }

        @Test
        void testValidateWithMixedArguments() throws AnalysisException {
                // Register mixed required and optional arguments
                namedArguments.registerRequiredArgument("name", "Service name",
                                ArgumentParsers.nonEmptyString("name"));
                namedArguments.registerOptionalArgument("debug", "Debug mode", false,
                                ArgumentParsers.booleanValue("debug"));
                namedArguments.registerOptionalArgument("retries", "Retry count", 3,
                                ArgumentParsers.positiveInt("retries"));

                Map<String, String> properties = new HashMap<>();
                properties.put("name", "test-service");
                properties.put("debug", "true");
                // retries not provided, should use default

                namedArguments.validate(properties);

                // Check values
                Assertions.assertEquals("test-service", namedArguments.getString("name"));
                Assertions.assertEquals(Boolean.TRUE, namedArguments.getBoolean("debug"));
                Assertions.assertEquals(Integer.valueOf(3), namedArguments.getInt("retries"));
        }

        @Test
        void testValidateFailsOnMissingRequiredArgument() {
                namedArguments.registerRequiredArgument("host", "Server host",
                                ArgumentParsers.nonEmptyString("host"));

                Map<String, String> properties = new HashMap<>();
                // host is missing

                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> namedArguments.validate(properties));
                Assertions.assertTrue(exception.getMessage().contains("Missing required argument: host"));
        }

        @Test
        void testValidateFailsOnEmptyRequiredArgument() {
                namedArguments.registerRequiredArgument("host", "Server host",
                                ArgumentParsers.nonEmptyString("host"));

                Map<String, String> properties = new HashMap<>();
                properties.put("host", "   "); // Empty string

                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> namedArguments.validate(properties));
                Assertions.assertTrue(exception.getMessage().contains("host cannot be empty"));
        }

        @Test
        void testValidateFailsOnUnknownArgument() {
                namedArguments.registerOptionalArgument("timeout", "Request timeout", 30,
                                ArgumentParsers.positiveInt("timeout"));

                Map<String, String> properties = new HashMap<>();
                properties.put("timeout", "60");
                properties.put("unknown_param", "value"); // Unknown argument

                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> namedArguments.validate(properties));
                Assertions.assertTrue(exception.getMessage().contains("Unknown argument: unknown_param"));
        }

        @Test
        void testValidateFailsOnInvalidValue() {
                namedArguments.registerRequiredArgument("port", "Server port",
                                ArgumentParsers.positiveInt("port"));

                Map<String, String> properties = new HashMap<>();
                properties.put("port", "not-a-number");

                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> namedArguments.validate(properties));
                Assertions.assertTrue(exception.getMessage().contains("Invalid value for argument 'port'"));
        }

        @Test
        void testValidateFailsOnInvalidPositiveInt() {
                namedArguments.registerRequiredArgument("port", "Server port",
                                ArgumentParsers.positiveInt("port"));

                Map<String, String> properties = new HashMap<>();
                properties.put("port", "-1"); // Negative number

                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> namedArguments.validate(properties));
                Assertions.assertTrue(exception.getMessage().contains("Invalid value for argument 'port'"));
        }

        @Test
        void testAllowedArguments() throws AnalysisException {
                namedArguments.registerRequiredArgument("name", "Service name",
                                ArgumentParsers.nonEmptyString("name"));
                namedArguments.addAllowedArgument("system_param");

                Map<String, String> properties = new HashMap<>();
                properties.put("name", "test");
                properties.put("system_param", "value"); // Should be allowed

                // Should not throw exception
                namedArguments.validate(properties);

                // system_param is not parsed (not registered), only allowed
                Assertions.assertEquals("test", namedArguments.getString("name"));
                Assertions.assertNull(namedArguments.getString("system_param"));
        }

        @Test
        void testGetTypedValues() throws AnalysisException {
                namedArguments.registerOptionalArgument("timeout", "Timeout", 30,
                                ArgumentParsers.positiveInt("timeout"));
                namedArguments.registerOptionalArgument("rate", "Rate", 1.5,
                                ArgumentParsers.positiveDouble("rate"));
                namedArguments.registerOptionalArgument("enabled", "Enabled", false,
                                ArgumentParsers.booleanValue("enabled"));
                namedArguments.registerOptionalArgument("count", "Count", 100L,
                                ArgumentParsers.positiveLong("count"));

                Map<String, String> properties = new HashMap<>();
                properties.put("timeout", "60");
                properties.put("rate", "2.5");
                properties.put("enabled", "true");
                properties.put("count", "200");

                namedArguments.validate(properties);

                // Test typed getters
                Assertions.assertEquals(Integer.valueOf(60), namedArguments.getInt("timeout"));
                Assertions.assertEquals(Double.valueOf(2.5), namedArguments.getDouble("rate"));
                Assertions.assertEquals(Boolean.TRUE, namedArguments.getBoolean("enabled"));
                Assertions.assertEquals(Long.valueOf(200), namedArguments.getLong("count"));

                // Test getString works for all types
                Assertions.assertEquals("60", namedArguments.getString("timeout"));
                Assertions.assertEquals("2.5", namedArguments.getString("rate"));
                Assertions.assertEquals("true", namedArguments.getString("enabled"));
                Assertions.assertEquals("200", namedArguments.getString("count"));
        }

        @Test
        void testGetValueWithGenericType() throws AnalysisException {
                namedArguments.registerOptionalArgument("count", "Count", 100,
                                ArgumentParsers.positiveInt("count"));

                Map<String, String> properties = new HashMap<>();
                properties.put("count", "150");

                namedArguments.validate(properties);

                // Test generic getValue method
                Integer count = namedArguments.getValue("count");
                Assertions.assertEquals(Integer.valueOf(150), count);

                // Test with wrong type should throw ClassCastException
                Assertions.assertThrows(ClassCastException.class, () -> {
                        Long wrongType = namedArguments.getValue("count");
                        // This line should not be reached, but keeps the variable "used"
                        Assertions.assertNotNull(wrongType);
                });
        }

        @Test
        void testGetNullValues() throws AnalysisException {
                namedArguments.registerOptionalArgument("optional_param", "Optional", null,
                                ArgumentParsers.nonEmptyString("optional_param"));

                Map<String, String> properties = new HashMap<>();
                // Don't provide optional_param

                namedArguments.validate(properties);

                // Should return null for missing optional arguments with null defaults
                Assertions.assertNull(namedArguments.getString("optional_param"));
                Assertions.assertNull(namedArguments.getInt("optional_param"));
                Assertions.assertNull(namedArguments.getBoolean("optional_param"));
                Assertions.assertNull(namedArguments.getValue("optional_param"));
        }

        @Test
        void testClear() {
                namedArguments.registerRequiredArgument("test", "Test param",
                                ArgumentParsers.nonEmptyString("test"));
                namedArguments.addAllowedArgument("allowed");

                Assertions.assertEquals(1, namedArguments.size());
                Assertions.assertFalse(namedArguments.isEmpty());

                namedArguments.clear();

                Assertions.assertEquals(0, namedArguments.size());
                Assertions.assertTrue(namedArguments.isEmpty());
        }

        @Test
        void testArgumentDefinitionToString() {
                namedArguments.registerOptionalArgument("test", "Test parameter", "default",
                                ArgumentParsers.nonEmptyString("test"));

                NamedArguments.ArgumentDefinition<?> argDef = namedArguments.getArgumentDefinitions().get(0);
                String toString = argDef.toString();

                Assertions.assertTrue(toString.contains("name='test'"));
                Assertions.assertTrue(toString.contains("required=false"));
                Assertions.assertTrue(toString.contains("defaultValue='default'"));
                Assertions.assertTrue(toString.contains("description='Test parameter'"));
        }

        @Test
        void testValidateWithStringChoice() throws AnalysisException {
                namedArguments.registerOptionalArgument("level", "Log level", "INFO",
                                ArgumentParsers.stringChoice("level", "DEBUG", "INFO", "WARN", "ERROR"));

                Map<String, String> properties = new HashMap<>();
                properties.put("level", "DEBUG");

                namedArguments.validate(properties);
                Assertions.assertEquals("DEBUG", namedArguments.getString("level"));

                // Test invalid choice
                properties.put("level", "INVALID");
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> namedArguments.validate(properties));
                Assertions.assertTrue(exception.getMessage().contains("must be one of"));
        }

        @Test
        void testValidateWithRangedInt() throws AnalysisException {
                namedArguments.registerOptionalArgument("port", "Port number", 8080,
                                ArgumentParsers.intRange("port", 1, 65535));

                Map<String, String> properties = new HashMap<>();
                properties.put("port", "9000");

                namedArguments.validate(properties);
                Assertions.assertEquals(Integer.valueOf(9000), namedArguments.getInt("port"));

                // Test out of range
                properties.put("port", "70000");
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> namedArguments.validate(properties));
                Assertions.assertTrue(exception.getMessage().contains("must be between 1 and 65535"));
        }

        @Test
        void testMultipleValidationCalls() throws AnalysisException {
                namedArguments.registerOptionalArgument("value", "Test value", 10,
                                ArgumentParsers.positiveInt("value"));

                // First validation
                Map<String, String> properties1 = new HashMap<>();
                properties1.put("value", "20");
                namedArguments.validate(properties1);
                Assertions.assertEquals(Integer.valueOf(20), namedArguments.getInt("value"));

                // Second validation should clear previous values
                Map<String, String> properties2 = new HashMap<>();
                properties2.put("value", "30");
                namedArguments.validate(properties2);
                Assertions.assertEquals(Integer.valueOf(30), namedArguments.getInt("value"));

                // Third validation with no value should use default
                Map<String, String> properties3 = new HashMap<>();
                namedArguments.validate(properties3);
                Assertions.assertEquals(Integer.valueOf(10), namedArguments.getInt("value"));
        }

        @Test
        void testValidateWithEmptyProperties() throws AnalysisException {
                // Only optional arguments
                namedArguments.registerOptionalArgument("debug", "Debug mode", false,
                                ArgumentParsers.booleanValue("debug"));

                Map<String, String> properties = new HashMap<>();
                namedArguments.validate(properties);

                // Should use default value
                Assertions.assertEquals(Boolean.FALSE, namedArguments.getBoolean("debug"));
        }
}
