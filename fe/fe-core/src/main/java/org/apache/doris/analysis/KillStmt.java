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

package org.apache.doris.analysis;

/**
 * Representation of a Kill statement.
 * Acceptable syntax:
 * KILL [QUERY | CONNECTION] connection_id
 */
public class KillStmt extends StatementBase {
    private final boolean isConnectionKill;
    private final long connectionId;

    public KillStmt(boolean isConnectionKill, long connectionId) {
        this.isConnectionKill = isConnectionKill;
        this.connectionId = connectionId;
    }

    public boolean isConnectionKill() {
        return isConnectionKill;
    }

    public long getConnectionId() {
        return connectionId;
    }

    @Override
    public void analyze(Analyzer analyzer) {
        // No operation.
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("KILL ");
        if (!isConnectionKill) {
            sb.append("QUERY ");
        }
        sb.append(connectionId);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
    
    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

