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

package org.apache.doris.arrowflight.sessions;

import org.apache.doris.arrowflight.auth2.FlightAuthResult;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.TokenMasker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.arrow.flight.CallStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.security.SecureRandom;

/**
 * The Arrow Flight SQL sessions as the connection pool holds them. A bearer token is the peer
 * identity a session is registered in the pool under, nothing else: issuing a token is opening a
 * session, validating a token is looking its session up, and a session that has left the pool -
 * by whichever teardown path ({@link ConnectPoolMgr#unregisterConnection}) - has taken its token
 * with it. So the pool's quotas are the only admission control (a session that does not fit is
 * refused, never one that does evicted), and the pool's timeout checker (wait_timeout) the only
 * expiry.
 */
public class FlightSessionsInConnectPool implements FlightSessionsManager {
    private static final Logger LOG = LogManager.getLogger(FlightSessionsInConnectPool.class);

    private final SecureRandom generator = new SecureRandom();
    private final ConnectScheduler connectScheduler;

    public FlightSessionsInConnectPool(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    // From https://stackoverflow.com/questions/41107/how-to-generate-a-random-alpha-numeric-string
    // ... This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding
    // them in base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32
    // number can encode 5 bits, so 128 is rounded up to the next multiple of 5 ... Why 32? Because 32 = 2^5;
    // each character will represent exactly 5 bits, and 130 bits can be evenly divided into characters.
    @VisibleForTesting
    String newToken() {
        return new BigInteger(130, generator).toString(32);
    }

    @Override
    public String openSession(FlightAuthResult authResult) {
        final String token = newToken();
        ConnectContext connectContext = FlightSessionsManager.buildConnectContext(token,
                authResult.getUserIdentity(), authResult.getRemoteIp(), connectScheduler);
        connectScheduler.submit(connectContext);
        // The one pool every protocol registers in: qe_max_connection, the user's
        // max_user_connections and the Arrow Flight SQL sub-quota, refused in the words a MySQL
        // client is refused in. Nothing is left of a refused session: the client gets no token, so it
        // does not keep a credential that can never open a session.
        ConnectPoolMgr pool = connectScheduler.getConnectPoolMgr();
        int res = pool.registerConnection(connectContext);
        if (res >= 0) {
            String errMsg = pool.limitReachedMessage(connectContext, res);
            // The refused session never entered the pool, so nothing else releases what its
            // adapter allocated (the channel's allocator).
            connectContext.releaseProtocolSession();
            LOG.warn("refuse arrow flight sql session, user: {}, remote: {}: {}", connectContext.getQualifiedUser(),
                    connectContext.getRemoteHostPortString(), errMsg);
            throw CallStatus.RESOURCE_EXHAUSTED.withDescription(errMsg).toRuntimeException();
        }
        // Never log the token itself: fe.log is routinely shipped off the FE host, and the token is
        // accepted as a full credential for as long as the session lives. The id is enough to trace
        // the session's lifecycle.
        LOG.info("open arrow flight sql session, connection id: {}, user: {}, remote: {}, token id: {}",
                connectContext.getConnectionId(), connectContext.getQualifiedUser(),
                connectContext.getRemoteHostPortString(), TokenMasker.tokenId(token));
        return token;
    }

    @Override
    public ConnectContext getConnectContext(String peerIdentity) {
        Preconditions.checkNotNull(peerIdentity, "invalid bearer token");
        ConnectContext connectContext = connectScheduler.getContextWithPeerIdentity(peerIdentity);
        if (connectContext == null) {
            throw CallStatus.UNAUTHENTICATED.withDescription("invalid bearer token, token id: "
                    + TokenMasker.tokenId(peerIdentity) + ": no Arrow Flight SQL session is open under it on this"
                    + " frontend. The session may have been closed (CloseSession), killed (KILL CONNECTION) or"
                    + " ended by wait_timeout, may have been opened on another frontend, or this frontend may"
                    + " have restarted since; reconnect to open a new session. Search fe.log for the token id to"
                    + " see how the session ended").toRuntimeException();
        }
        return connectContext;
    }

    @Override
    public void closeConnectContext(String peerIdentity) {
        ConnectContext connectContext = connectScheduler.getContextWithPeerIdentity(peerIdentity);
        if (connectContext == null) {
            // Ended meanwhile (a KILL, the timeout checker): there is nothing left to close.
            LOG.info("close arrow flight sql session, token id: {}: no session is open under the token",
                    TokenMasker.tokenId(peerIdentity));
            return;
        }
        LOG.info("close arrow flight sql session by client, connection id: {}, user: {}, remote: {}, token id: {}",
                connectContext.getConnectionId(), connectContext.getQualifiedUser(),
                connectContext.getRemoteHostPortString(), TokenMasker.tokenId(peerIdentity));
        // The way a MySQL connection ends on its client's COM_QUIT: the channel's cached results, the
        // deferred executors and the transaction are released and the pool's slots given back
        // (unregisterConnection), and the session's temporary tables are dropped. A Flight session's
        // commands need not run on the thread that closes it, so a statement that is still running
        // is cancelled as well: no client will take its result.
        connectContext.cleanup();
        connectContext.cancelQuery(new Status(TStatusCode.CANCELLED,
                "session closed by client (CloseSession) from " + connectContext.getRemoteHostPortString()));
    }
}
