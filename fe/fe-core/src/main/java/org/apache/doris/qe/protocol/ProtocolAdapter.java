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

package org.apache.doris.qe.protocol;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.thrift.TResultSinkType;

/**
 * The wire-protocol half of a connection.
 *
 * <p>A {@link ConnectContext} is the session: user, catalog and database, session variables,
 * transaction, prepared statements, the statement being executed. Everything only one wire
 * protocol knows about lives behind this interface instead: the MySQL channel and the negotiated
 * capabilities, or the Arrow Flight SQL result cache and endpoints. A context is bound to exactly
 * one adapter when it is created and keeps it for its whole life.
 *
 * <p>Implementations: {@code MysqlProtocolAdapter} (a MySQL client, a proxy context replaying a
 * forwarded statement on the master, and an internal context whose channel discards everything)
 * and {@code FlightProtocolAdapter} (an Arrow Flight SQL session).
 */
public interface ProtocolAdapter {

    ConnectType type();

    /**
     * The client address, as shown in the Host column of SHOW PROCESSLIST and as client_ip in the
     * audit log.
     */
    String remoteHostPortString(ConnectContext ctx);

    /** The result sink a backend must use for a query on this connection. */
    TResultSinkType resultSinkType();

    /**
     * The pool this connection is registered in. Each protocol still keeps its own pool; this
     * goes away when they are merged.
     */
    ConnectPoolMgr connectPool(ConnectScheduler scheduler);

    /**
     * Called from {@link ConnectContext#clear()} once the response of a statement has been sent,
     * to drop the protocol state that only belonged to that statement.
     */
    void afterStatement(ConnectContext ctx);

    /** Tears down the transport side of the connection. Must be idempotent. */
    void closeConnection(ConnectContext ctx);
}
