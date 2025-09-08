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

/**
 * Common argument parsers for NamedArguments.
 * This utility class provides pre-built parsers for common data types
 * and validation patterns. All parsers implement ArgumentParser interface
 * and return typed values after validation and conversion.
 *
 * <p>
 * Usage example:
 *
 * <pre>
 * NamedArguments args = new NamedArguments();
 * args.registerArgument("port", "Server port", 8080,
 *         ArgumentParsers.positiveInt("port", 1, 65535));
 * args.registerArgument("timeout", "Request timeout", 30.0,
 *         ArgumentParsers.positiveDouble("timeout"));
 * </pre>
 */
public class ArgumentParsers {
    // String parsers

    /**
     * Parse and validate non-empty string.
     *
     * @param paramName Parameter name for error messages
     * @return String parser
     */
    public static ArgumentParser<String> nonEmptyString(String paramName) {
        return value -> {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(paramName + " cannot be empty");
            }
            return value.trim();
        };
    }

    /**
     * Parse string with length constraints.
     *
     * @param paramName Parameter name for error messages
     * @param minLength Minimum string length (inclusive)
     * @param maxLength Maximum string length (inclusive)
     * @return String parser
     */
    public static ArgumentParser<String> stringLength(String paramName, int minLength, int maxLength) {
        return value -> {
            if (value == null) {
                throw new IllegalArgumentException(paramName + " cannot be null");
            }
            String trimmed = value.trim();
            int length = trimmed.length();
            if (length < minLength || length > maxLength) {
                throw new IllegalArgumentException(String.format(
                        "%s length must be between %d and %d, got: %d",
                        paramName, minLength, maxLength, length));
            }
            return trimmed;
        };
    }

    /**
     * Parse string from a set of allowed values (case-sensitive).
     *
     * @param paramName     Parameter name for error messages
     * @param allowedValues Array of allowed string values
     * @return String parser
     */
    public static ArgumentParser<String> stringChoice(String paramName, String... allowedValues) {
        return value -> {
            if (value == null) {
                throw new IllegalArgumentException(paramName + " cannot be null");
            }
            String trimmed = value.trim();
            for (String allowed : allowedValues) {
                if (allowed.equals(trimmed)) {
                    return trimmed;
                }
            }
            throw new IllegalArgumentException(String.format(
                    "%s must be one of %s, got: %s",
                    paramName, String.join(", ", allowedValues), trimmed));
        };
    }

    /**
     * Parse string from a set of allowed values (case-insensitive).
     *
     * @param paramName     Parameter name for error messages
     * @param allowedValues Array of allowed string values
     * @return String parser
     */
    public static ArgumentParser<String> stringChoiceIgnoreCase(String paramName, String... allowedValues) {
        return value -> {
            if (value == null) {
                throw new IllegalArgumentException(paramName + " cannot be null");
            }
            String trimmed = value.trim();
            for (String allowed : allowedValues) {
                if (allowed.equalsIgnoreCase(trimmed)) {
                    return allowed; // Return the canonical form
                }
            }
            throw new IllegalArgumentException(String.format(
                    "%s must be one of %s (case-insensitive), got: %s",
                    paramName, String.join(", ", allowedValues), trimmed));
        };
    }

    // Integer parsers

    /**
     * Parse positive integer value.
     *
     * @param paramName Parameter name for error messages
     * @return Integer parser
     */
    public static ArgumentParser<Integer> positiveInt(String paramName) {
        return value -> {
            try {
                int intValue = Integer.parseInt(value.trim());
                if (intValue <= 0) {
                    throw new IllegalArgumentException(paramName + " must be positive, got: " + intValue);
                }
                return intValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + paramName + " format: " + value);
            }
        };
    }

    /**
     * Parse integer value within specified range.
     *
     * @param paramName Parameter name for error messages
     * @param minValue  Minimum allowed value (inclusive)
     * @param maxValue  Maximum allowed value (inclusive)
     * @return Integer parser
     */
    public static ArgumentParser<Integer> intRange(String paramName, int minValue, int maxValue) {
        return value -> {
            try {
                int intValue = Integer.parseInt(value.trim());
                if (intValue < minValue || intValue > maxValue) {
                    throw new IllegalArgumentException(String.format(
                            "%s must be between %d and %d, got: %d", paramName, minValue, maxValue, intValue));
                }
                return intValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + paramName + " format: " + value);
            }
        };
    }

    // Long parsers

    /**
     * Parse positive long value.
     *
     * @param paramName Parameter name for error messages
     * @return Long parser
     */
    public static ArgumentParser<Long> positiveLong(String paramName) {
        return value -> {
            try {
                long longValue = Long.parseLong(value.trim());
                if (longValue <= 0) {
                    throw new IllegalArgumentException(paramName + " must be positive, got: " + longValue);
                }
                return longValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + paramName + " format: " + value);
            }
        };
    }

    /**
     * Parse non-negative long value.
     *
     * @param paramName Parameter name for error messages
     * @return Long parser
     */
    public static ArgumentParser<Long> nonNegativeLong(String paramName) {
        return value -> {
            try {
                long longValue = Long.parseLong(value.trim());
                if (longValue < 0) {
                    throw new IllegalArgumentException(paramName + " must be non-negative, got: " + longValue);
                }
                return longValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + paramName + " format: " + value);
            }
        };
    }

    /**
     * Parse long value within specified range.
     *
     * @param paramName Parameter name for error messages
     * @param minValue  Minimum allowed value (inclusive)
     * @param maxValue  Maximum allowed value (inclusive)
     * @return Long parser
     */
    public static ArgumentParser<Long> longRange(String paramName, long minValue, long maxValue) {
        return value -> {
            try {
                long longValue = Long.parseLong(value.trim());
                if (longValue < minValue || longValue > maxValue) {
                    throw new IllegalArgumentException(String.format(
                            "%s must be between %d and %d, got: %d", paramName, minValue, maxValue, longValue));
                }
                return longValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + paramName + " format: " + value);
            }
        };
    }

    // Double parsers

    /**
     * Parse positive double value.
     *
     * @param paramName Parameter name for error messages
     * @return Double parser
     */
    public static ArgumentParser<Double> positiveDouble(String paramName) {
        return value -> {
            try {
                double doubleValue = Double.parseDouble(value.trim());
                if (doubleValue <= 0) {
                    throw new IllegalArgumentException(paramName + " must be positive, got: " + doubleValue);
                }
                return doubleValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + paramName + " format: " + value);
            }
        };
    }

    /**
     * Parse double value within specified range.
     *
     * @param paramName Parameter name for error messages
     * @param minValue  Minimum allowed value (inclusive)
     * @param maxValue  Maximum allowed value (inclusive)
     * @return Double parser
     */
    public static ArgumentParser<Double> doubleRange(String paramName, double minValue, double maxValue) {
        return value -> {
            try {
                double doubleValue = Double.parseDouble(value.trim());
                if (doubleValue < minValue || doubleValue > maxValue) {
                    throw new IllegalArgumentException(String.format(
                            "%s must be between %.2f and %.2f, got: %.2f",
                            paramName, minValue, maxValue, doubleValue));
                }
                return doubleValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + paramName + " format: " + value);
            }
        };
    }

    // Boolean parser

    /**
     * Parse boolean value (case-insensitive "true" or "false").
     *
     * @param paramName Parameter name for error messages
     * @return Boolean parser
     */
    public static ArgumentParser<Boolean> booleanValue(String paramName) {
        return value -> {
            String trimmed = value.trim();
            if ("true".equalsIgnoreCase(trimmed)) {
                return Boolean.TRUE;
            } else if ("false".equalsIgnoreCase(trimmed)) {
                return Boolean.FALSE;
            } else {
                throw new IllegalArgumentException(paramName + " must be 'true' or 'false', got: " + trimmed);
            }
        };
    }
}
