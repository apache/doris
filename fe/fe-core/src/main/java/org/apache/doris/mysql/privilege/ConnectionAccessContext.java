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

package org.apache.doris.mysql.privilege;

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;

import java.util.Optional;

/**
 * The circumstances of a check, read from the connection the statement is running on.
 *
 * <p>Nothing is read until asked for. A plugin that decides from grants alone asks for none of it, and there
 * are enough checks per statement - one per object a statement touches, and every object there is when a
 * statement lists what a user may see - that formatting a query id nobody reads would be a real cost.
 */
class ConnectionAccessContext implements AccessContext {

    private final ConnectContext connection;

    private ConnectionAccessContext(ConnectContext connection) {
        this.connection = connection;
    }

    /**
     * The circumstances of the statement running on this thread, or {@link AccessContext#NONE} when the check
     * comes from somewhere other than a client statement - a background job, or replaying an edit log.
     */
    static AccessContext current() {
        ConnectContext connection = ConnectContext.get();
        return connection == null ? AccessContext.NONE : new ConnectionAccessContext(connection);
    }

    @Override
    public Optional<String> getClientIp() {
        return Optional.ofNullable(Strings.emptyToNull(connection.getRemoteIP()));
    }

    @Override
    public Optional<String> getQueryId() {
        TUniqueId queryId = connection.queryId();
        return queryId == null ? Optional.empty() : Optional.of(DebugUtil.printId(queryId));
    }
}
