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

/**
 * Converts between a hierarchical Lance namespace and the flat database name exposed by Doris.
 *
 * <p>The input namespace is relative to the optional parent namespace configured on the catalog.
 * An empty relative namespace maps to {@code rootDatabase}. Other namespace components are joined
 * with {@code delimiter}; delimiter occurrences and backslashes inside a component are escaped so
 * the conversion remains reversible. A one-level namespace equal to {@code rootDatabase} receives
 * an additional leading escape to distinguish it from the empty root namespace.
 *
 * <p>This class only converts names. It does not access the remote Lance namespace service.
 */
final class LanceNamespaceName {
    private static final char ESCAPE = '\\';

    private LanceNamespaceName() {
    }

    /**
     * Converts relative Lance namespace components to one Doris database name.
     *
     * <p>For example, with {@code "."} as the delimiter,
     * {@code [company.analytics, raw]} becomes {@code company\.analytics.raw}.
     * An empty namespace becomes {@code rootDatabase}.
     */
    static String namespaceToDorisDatabaseName(
            List<String> relativeNamespace, String delimiter, String rootDatabase) {
        if (relativeNamespace.isEmpty()) {
            return rootDatabase;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < relativeNamespace.size(); i++) {
            if (i > 0) {
                result.append(delimiter);
            }
            result.append(escapeNamespaceComponent(relativeNamespace.get(i), delimiter));
        }
        // The local root database represents the empty Lance namespace. Escape
        // the rare, colliding one-level namespace so the mapping stays reversible.
        return rootDatabase.contentEquals(result) ? ESCAPE + result.toString() : result.toString();
    }

    /** Escapes backslashes and delimiters occurring inside one namespace component. */
    private static String escapeNamespaceComponent(String component, String delimiter) {
        return component.replace("\\", "\\\\").replace(delimiter, "\\" + delimiter);
    }

    /**
     * Restores the relative Lance namespace represented by one Doris database name.
     *
     * <p>For example, with {@code "."} as the delimiter,
     * {@code company\.analytics.raw} becomes {@code [company.analytics, raw]}.
     * A database name equal to {@code rootDatabase} becomes an empty namespace.
     */
    static List<String> dorisDatabaseNameToNamespace(
            String databaseName, String delimiter, String rootDatabase) throws DdlException {
        if (rootDatabase.equals(databaseName)) {
            return Collections.emptyList();
        }
        if (databaseName.length() > 1 && databaseName.charAt(0) == ESCAPE
                && rootDatabase.equals(databaseName.substring(1))) {
            return parseEscapedNamespace(databaseName.substring(1), delimiter);
        }
        return parseEscapedNamespace(databaseName, delimiter);
    }

    /**
     * Parses the escaped parent namespace configured in the catalog properties.
     *
     * <p>For example, with {@code "."} as the delimiter,
     * {@code company.analytics} becomes {@code [company, analytics]}.
     */
    static List<String> parseParentNamespace(String value, String delimiter) throws DdlException {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }
        return parseEscapedNamespace(value, delimiter);
    }

    /**
     * Splits an escaped namespace and rejects invalid escapes or empty components.
     *
     * <p>For example, with {@code "."} as the delimiter, {@code company\.analytics.raw}
     * is parsed as {@code [company.analytics, raw]}.
     */
    private static List<String> parseEscapedNamespace(String value, String delimiter)
            throws DdlException {
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
                addNamespaceComponent(result, current, value);
                i += delimiter.length();
            } else {
                current.append(value.charAt(i++));
            }
        }
        addNamespaceComponent(result, current, value);
        return result;
    }

    /** Adds one parsed component while enforcing that Lance namespace components are non-empty. */
    private static void addNamespaceComponent(
            List<String> result, StringBuilder current, String encoded) throws DdlException {
        if (current.length() == 0) {
            throw new DdlException("Empty Lance namespace component in '" + encoded + "'");
        }
        result.add(current.toString());
        current.setLength(0);
    }
}
