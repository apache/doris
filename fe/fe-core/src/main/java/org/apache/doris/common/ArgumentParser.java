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
 * Interface for argument parsers that validate and parse string values to typed
 * objects.
 * This interface extends the validation concept to include type conversion and
 * storage.
 *
 * @param <T> The target type that this parser converts string values to
 */
@FunctionalInterface
public interface ArgumentParser<T> {

    /**
     * Parse and validate a string value, converting it to the target type.
     *
     * @param value The string value to parse
     * @return The parsed and validated value of type T
     * @throws IllegalArgumentException if the value is invalid or cannot be parsed
     */
    T parse(String value) throws IllegalArgumentException;
}
