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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Type-safe named arguments registry for parameter parsing, validation and
 * management.
 * This utility class provides a centralized way to register parameters with
 * type-safe parsers and validate them against provided property maps.
 * After validation, parsed values are stored internally and can be retrieved
 * using type-safe getter methods.
 * 
 * <p>
 * Usage example:
 * 
 * <pre>
 * NamedArguments args = new NamedArguments();
 * args.registerArgument("timeout", "Request timeout in seconds", 30,
 *         ArgumentParsers.positiveInt("timeout"));
 * args.registerArgument("enabled", "Whether feature is enabled", true,
 *         ArgumentParsers.booleanValue("enabled"));
 * 
 * Map&lt;String, String&gt; properties = Map.of("timeout", "60", "enabled", "false");
 * args.validate(properties); // Parse and store values
 * 
 * int timeout = args.getInt("timeout"); // Returns 60
 * boolean enabled = args.getBoolean("enabled"); // Returns false
 * </pre>
 */
public class NamedArguments {

    private final List<ArgumentDefinition<?>> argumentDefinitions = new ArrayList<>();
    private final Set<String> allowedArguments = new HashSet<>();
    private final Map<String, Object> parsedValues = new HashMap<>();

    /**
     * Register a required argument with type-safe parser.
     * 
     * @param <T>         The type of the argument value
     * @param name        The argument name
     * @param description Human-readable description
     * @param parser      Type-safe parser for validation and conversion
     */
    public <T> void registerArgument(String name, String description, ArgumentParser<T> parser) {
        registerArgument(name, description, true, null, parser);
    }

    /**
     * Register an optional argument with default value and type-safe parser.
     * 
     * @param <T>          The type of the argument value
     * @param name         The argument name
     * @param description  Human-readable description
     * @param defaultValue Default value if not provided
     * @param parser       Type-safe parser for validation and conversion
     */
    public <T> void registerArgument(String name, String description, T defaultValue, ArgumentParser<T> parser) {
        registerArgument(name, description, false, defaultValue, parser);
    }

    /**
     * Register an argument with full specification.
     * 
     * @param <T>          The type of the argument value
     * @param name         The argument name
     * @param description  Human-readable description
     * @param required     Whether the argument is required
     * @param defaultValue Default value if not provided (can be null)
     * @param parser       Type-safe parser for validation and conversion
     */
    public <T> void registerArgument(String name, String description, boolean required,
            T defaultValue, ArgumentParser<T> parser) {
        argumentDefinitions.add(new ArgumentDefinition<>(name, description, required, defaultValue, parser));
    }

    /**
     * Add an allowed argument name that should not trigger unknown argument errors.
     * This is useful for framework-level arguments that are handled elsewhere.
     * 
     * @param argumentName The allowed argument name
     */
    public void addAllowedArgument(String argumentName) {
        allowedArguments.add(argumentName);
    }

    /**
     * Validate and parse the provided properties against registered arguments.
     * This method will:
     * 1. Check that all required arguments are provided
     * 2. Apply default values for missing optional arguments
     * 3. Parse and validate all provided argument values using their parsers
     * 4. Store parsed values for later retrieval
     * 5. Report unknown arguments that weren't registered or allowed
     * 
     * @param properties The property map to validate and parse
     * @throws AnalysisException If validation or parsing fails
     */
    public void validate(Map<String, String> properties) throws AnalysisException {
        // Clear previous parsed values
        parsedValues.clear();

        // Check required arguments, apply defaults, and parse values
        for (ArgumentDefinition<?> arg : argumentDefinitions) {
            String stringValue = properties.get(arg.getName());

            // Check required arguments
            if (arg.isRequired() && (stringValue == null || stringValue.trim().isEmpty())) {
                throw new AnalysisException("Missing required argument: " + arg.getName());
            }

            // Determine the value to parse (either provided or default)
            Object valueToStore = null;
            if (stringValue != null && !stringValue.trim().isEmpty()) {
                // Parse provided value
                try {
                    valueToStore = arg.getParser().parse(stringValue);
                } catch (Exception e) {
                    throw new AnalysisException(String.format(
                            "Invalid value for argument '%s': %s. %s",
                            arg.getName(), stringValue, e.getMessage()));
                }
            } else if (arg.getDefaultValue() != null) {
                // Use default value directly (no parsing needed since it's already typed)
                valueToStore = arg.getDefaultValue();
            }

            // Store the parsed/default value
            if (valueToStore != null) {
                parsedValues.put(arg.getName(), valueToStore);
            }
        }

        // Check for unknown arguments
        for (String providedArg : properties.keySet()) {
            if (!isRegisteredArgument(providedArg) && !isAllowedArgument(providedArg)) {
                throw new AnalysisException("Unknown argument: " + providedArg);
            }
        }
    }

    /**
     * Get a string value by argument name.
     * 
     * @param name The argument name
     * @return The string value, or null if not set
     */
    public String getString(String name) {
        Object value = parsedValues.get(name);
        return value != null ? value.toString() : null;
    }

    /**
     * Get an integer value by argument name.
     * 
     * @param name The argument name
     * @return The integer value, or null if not set
     * @throws ClassCastException If the stored value is not an Integer
     */
    public Integer getInt(String name) {
        return getValue(name);
    }

    /**
     * Get a long value by argument name.
     * 
     * @param name The argument name
     * @return The long value, or null if not set
     * @throws ClassCastException If the stored value is not a Long
     */
    public Long getLong(String name) {
        return getValue(name);
    }

    /**
     * Get a double value by argument name.
     * 
     * @param name The argument name
     * @return The double value, or null if not set
     * @throws ClassCastException If the stored value is not a Double
     */
    public Double getDouble(String name) {
        return getValue(name);
    }

    /**
     * Get a boolean value by argument name.
     * 
     * @param name The argument name
     * @return The boolean value, or null if not set
     * @throws ClassCastException If the stored value is not a Boolean
     */
    public Boolean getBoolean(String name) {
        return getValue(name);
    }

    /**
     * Get a typed value by argument name.
     * 
     * @param <T>  The expected type of the value
     * @param name The argument name
     * @return The typed value, or null if not set
     * @throws ClassCastException If the stored value cannot be cast to T
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(String name) {
        return (T) parsedValues.get(name);
    }

    /**
     * Get all registered argument definitions.
     * 
     * @return List of argument definitions
     */
    public List<ArgumentDefinition<?>> getArgumentDefinitions() {
        return new ArrayList<>(argumentDefinitions);
    }

    /**
     * Get the number of registered arguments.
     * 
     * @return Number of registered arguments
     */
    public int size() {
        return argumentDefinitions.size();
    }

    /**
     * Check if any arguments have been registered.
     * 
     * @return true if no arguments are registered, false otherwise
     */
    public boolean isEmpty() {
        return argumentDefinitions.isEmpty();
    }

    /**
     * Clear all registered arguments, allowed arguments, and parsed values.
     */
    public void clear() {
        argumentDefinitions.clear();
        allowedArguments.clear();
        parsedValues.clear();
    }

    /**
     * Check if an argument is registered.
     */
    private boolean isRegisteredArgument(String argName) {
        return argumentDefinitions.stream().anyMatch(arg -> arg.getName().equals(argName));
    }

    /**
     * Check if an argument is explicitly allowed.
     */
    private boolean isAllowedArgument(String argName) {
        return allowedArguments.contains(argName);
    }

    /**
     * Type-safe argument definition class for the parsing framework.
     */
    public static class ArgumentDefinition<T> {
        private final String name;
        private final String description;
        private final boolean required;
        private final T defaultValue;
        private final ArgumentParser<T> parser;

        public ArgumentDefinition(String name, String description, boolean required,
                T defaultValue, ArgumentParser<T> parser) {
            this.name = name;
            this.description = description;
            this.required = required;
            this.defaultValue = defaultValue;
            this.parser = parser;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public boolean isRequired() {
            return required;
        }

        public T getDefaultValue() {
            return defaultValue;
        }

        public ArgumentParser<T> getParser() {
            return parser;
        }

        @Override
        public String toString() {
            return String.format("ArgumentDefinition{name='%s', required=%s, defaultValue='%s', description='%s'}",
                    name, required, defaultValue, description);
        }
    }
}
