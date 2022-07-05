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

package org.apache.doris.nereids.util;

/**
 * Utils for Nereids.
 */
public class Utils {
    /**
     * Quoted string if it contains special character or all characters are digit.
     *
     * @param part string to be quoted
     * @return quoted string
     */
    public static String quoteIfNeeded(String part) {
        if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
            return part;
        } else {
            return part.replace("`", "``");
        }
    }
}
