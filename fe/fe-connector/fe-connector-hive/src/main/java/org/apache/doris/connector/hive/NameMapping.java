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

package org.apache.doris.connector.hive;

import java.util.Objects;

/**
 * Mapping between the local (Doris) and remote (metastore) database/table names of a Hive
 * write/read transaction. This is a plugin-local, fe-core-free copy of {@code
 * org.apache.doris.datasource.NameMapping} (JDK-only, no lombok/guava), used as the identity key of
 * the connector transaction's per-table / per-partition action maps: the remote names drive the
 * actual metastore calls, and the local names surface in diagnostics.
 */
public final class NameMapping {
    private final long ctlId;
    private final String localDbName;
    private final String localTblName;
    private final String remoteDbName;
    private final String remoteTblName;

    public NameMapping(long ctlId, String localDbName, String localTblName,
            String remoteDbName, String remoteTblName) {
        this.ctlId = ctlId;
        this.localDbName = localDbName;
        this.localTblName = localTblName;
        this.remoteDbName = remoteDbName;
        this.remoteTblName = remoteTblName;
    }

    public long getCtlId() {
        return ctlId;
    }

    public String getLocalDbName() {
        return localDbName;
    }

    public String getLocalTblName() {
        return localTblName;
    }

    public String getRemoteDbName() {
        return remoteDbName;
    }

    public String getRemoteTblName() {
        return remoteTblName;
    }

    public String getFullLocalName() {
        return String.format("%s.%s", localDbName, localTblName);
    }

    public String getFullRemoteName() {
        return String.format("%s.%s", remoteDbName, remoteTblName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NameMapping)) {
            return false;
        }
        NameMapping that = (NameMapping) o;
        return ctlId == that.ctlId
                && localDbName.equals(that.localDbName)
                && localTblName.equals(that.localTblName)
                && remoteDbName.equals(that.remoteDbName)
                && remoteTblName.equals(that.remoteTblName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctlId, localDbName, localTblName, remoteDbName, remoteTblName);
    }

    @Override
    public String toString() {
        return "NameMapping{"
                + "ctlId=" + ctlId
                + ", localDbName='" + localDbName + '\''
                + ", localTblName='" + localTblName + '\''
                + ", remoteDbName='" + remoteDbName + '\''
                + ", remoteTblName='" + remoteTblName + '\''
                + '}';
    }
}
