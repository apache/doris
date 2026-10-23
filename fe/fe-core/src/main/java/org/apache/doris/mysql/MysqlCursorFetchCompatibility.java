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

package org.apache.doris.mysql;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Resolves the incompatible cursor result-set behavior used by Connector/J releases. */
public final class MysqlCursorFetchCompatibility {
    private static final Set<String> MYSQL_CONNECTOR_J_CLIENT_NAMES = ImmutableSet.of(
            "MySQL Connector/J", "MySQL Connector Java");
    private static final Pattern CONSUMES_METADATA_TERMINATOR =
            Pattern.compile("^(?:(?:5|6|8)\\.|9\\.[0-4](?:\\.|$))");
    private static final Pattern VERSION = Pattern.compile("^\\d+(?:\\.\\d+)+(?:[-+].*)?$");

    public enum Behavior {
        CONSUMES_METADATA_TERMINATOR,
        STANDARD,
        UNKNOWN
    }

    private MysqlCursorFetchCompatibility() {
    }

    public static Behavior resolve(Map<String, String> connectAttributes) {
        String clientName = connectAttributes.get("_client_name");
        if (clientName == null) {
            return Behavior.UNKNOWN;
        }
        if (!MYSQL_CONNECTOR_J_CLIENT_NAMES.contains(clientName)) {
            return Behavior.STANDARD;
        }

        String clientVersion = connectAttributes.get("_client_version");
        if (clientVersion == null || !VERSION.matcher(clientVersion).matches()) {
            return Behavior.UNKNOWN;
        }
        if (CONSUMES_METADATA_TERMINATOR.matcher(clientVersion).find()) {
            return Behavior.CONSUMES_METADATA_TERMINATOR;
        }
        return Behavior.STANDARD;
    }
}
