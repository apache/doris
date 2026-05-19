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

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

import java.util.Objects;

/**
 * Opaque column handle for JDBC tables. Carries the remote column name
 * so that the scan plan provider can build properly quoted SELECT clauses.
 */
public class JdbcColumnHandle implements ConnectorColumnHandle {

    private static final long serialVersionUID = 1L;

    private final String localName;
    private final String remoteName;

    public JdbcColumnHandle(String localName, String remoteName) {
        this.localName = Objects.requireNonNull(localName, "localName");
        this.remoteName = Objects.requireNonNull(remoteName, "remoteName");
    }

    public String getLocalName() {
        return localName;
    }

    public String getRemoteName() {
        return remoteName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(localName, remoteName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcColumnHandle)) {
            return false;
        }
        JdbcColumnHandle that = (JdbcColumnHandle) o;
        return localName.equals(that.localName) && remoteName.equals(that.remoteName);
    }

    @Override
    public String toString() {
        return "JdbcColumnHandle{" + localName + " -> " + remoteName + "}";
    }
}
