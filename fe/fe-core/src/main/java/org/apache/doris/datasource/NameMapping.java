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

package org.apache.doris.datasource;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;

import java.util.Objects;

/**
 * NameMapping class represents a mapping between local and remote database and table names.
 */
@Getter
public class NameMapping {
    private final long ctlId;
    private final String localDbName;
    private final String localTblName;
    private final String remoteDbName;
    private final String remoteTblName;

    public NameMapping(long ctlId, String localDbName, String localTblName, String remoteDbName, String remoteTblName) {
        this.ctlId = ctlId;
        this.localDbName = localDbName;
        this.localTblName = localTblName;
        this.remoteDbName = remoteDbName;
        this.remoteTblName = remoteTblName;
    }

    @VisibleForTesting
    public static NameMapping createForTest(String dbName, String tblName) {
        return new NameMapping(0, dbName, tblName, dbName, tblName);
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

    public String getFullLocalName() {
        return String.format("%s.%s", localDbName, localTblName);
    }

    public String getFullRemoteName() {
        return String.format("%s.%s", remoteDbName, remoteTblName);
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
