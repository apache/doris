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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

/**
 * Opaque table handle carrying the remote database/table coordinates.
 */
public class JdbcTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String remoteDbName;
    private final String remoteTableName;

    public JdbcTableHandle(String remoteDbName, String remoteTableName) {
        this.remoteDbName = remoteDbName;
        this.remoteTableName = remoteTableName;
    }

    public String getRemoteDbName() {
        return remoteDbName;
    }

    public String getRemoteTableName() {
        return remoteTableName;
    }

    @Override
    public String toString() {
        return "JdbcTableHandle{" + remoteDbName + "." + remoteTableName + "}";
    }
}
