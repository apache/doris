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

package org.apache.doris.nereids.analyzer.identifier;

import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * Identifier in Nereids with its database name.
 */
public abstract class IdentifierWithDatabase {
    protected String identifier;
    protected String databaseName;

    private String quoteIdentifier(String name) {
        return name.replace("`", "``");
    }

    public String getIdentifier() {
        return identifier;
    }

    public Optional<String> getDatabaseName() {
        return Optional.of(databaseName);
    }

    protected String quotedString() {
        String replacedId = quoteIdentifier(identifier);
        if (StringUtils.isNotEmpty(databaseName)) {
            String replacedDb = quoteIdentifier(databaseName);
            return String.format("`%s`.`%s`", replacedDb, replacedId);
        } else {
            return String.format("`%s`", replacedId);
        }
    }

    protected String unquotedString() {
        if (StringUtils.isNotEmpty(databaseName)) {
            return String.format("%s.%s", databaseName, identifier);
        } else {
            return String.format("%s", identifier);
        }
    }

    @Override
    public String toString() {
        return quotedString();
    }
}
