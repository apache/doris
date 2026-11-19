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

package org.apache.doris.connector.spi;

/**
 * A provider confirmed that the named table or view does not exist. Authentication, transport, missing files and
 * missing dependent resources must not use this exception. The engine still validates its observed cache
 * identity before retiring lookup state; absence of a table does not imply absence of a same-named view.
 */
public final class ConnectorTableNotFoundException extends DorisConnectorException {
    private final String databaseName;
    private final String tableName;

    public ConnectorTableNotFoundException(String databaseName, String tableName, Throwable cause) {
        super("Table not found: " + databaseName + "." + tableName, cause);
        this.databaseName = java.util.Objects.requireNonNull(databaseName);
        this.tableName = java.util.Objects.requireNonNull(tableName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }
}
