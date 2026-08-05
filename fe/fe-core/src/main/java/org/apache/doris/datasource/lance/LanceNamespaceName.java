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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.DdlException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Reversible mapping between a hierarchical Lance namespace and one Doris database name. */
final class LanceNamespaceName {
    private static final char ESCAPE = '\\';

    private LanceNamespaceName() {
    }

    static String encode(List<String> relativeNamespace, String delimiter, String rootDatabase) {
        if (relativeNamespace.isEmpty()) {
            return rootDatabase;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < relativeNamespace.size(); i++) {
            if (i > 0) {
                result.append(delimiter);
            }
            result.append(escape(relativeNamespace.get(i), delimiter));
        }
        // The local root database represents the empty Lance namespace. Escape
        // the rare, colliding one-level namespace so the mapping stays reversible.
        return rootDatabase.contentEquals(result) ? ESCAPE + result.toString() : result.toString();
    }

    static List<String> decode(String database, String delimiter, String rootDatabase) throws DdlException {
        if (rootDatabase.equals(database)) {
            return Collections.emptyList();
        }
        if (database.length() > 1 && database.charAt(0) == ESCAPE
                && rootDatabase.equals(database.substring(1))) {
            return splitEscaped(database.substring(1), delimiter);
        }
        return splitEscaped(database, delimiter);
    }

    static List<String> parseParent(String value, String delimiter) throws DdlException {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }
        return splitEscaped(value, delimiter);
    }

    private static List<String> splitEscaped(String value, String delimiter) throws DdlException {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < value.length();) {
            if (value.charAt(i) == ESCAPE) {
                int escapedIndex = i + 1;
                if (escapedIndex < value.length() && value.charAt(escapedIndex) == ESCAPE) {
                    current.append(ESCAPE);
                    i += 2;
                } else if (value.startsWith(delimiter, escapedIndex)) {
                    current.append(delimiter);
                    i = escapedIndex + delimiter.length();
                } else {
                    throw new DdlException("Invalid escape in Lance namespace '" + value + "'");
                }
            } else if (value.startsWith(delimiter, i)) {
                addComponent(result, current, value);
                i += delimiter.length();
            } else {
                current.append(value.charAt(i++));
            }
        }
        addComponent(result, current, value);
        return result;
    }

    private static String escape(String component, String delimiter) {
        return component.replace("\\", "\\\\").replace(delimiter, "\\" + delimiter);
    }

    private static void addComponent(List<String> result, StringBuilder current, String encoded)
            throws DdlException {
        if (current.length() == 0) {
            throw new DdlException("Empty Lance namespace component in '" + encoded + "'");
        }
        result.add(current.toString());
        current.setLength(0);
    }
}
