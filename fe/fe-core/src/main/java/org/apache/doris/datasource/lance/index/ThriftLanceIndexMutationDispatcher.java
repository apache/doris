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

package org.apache.doris.datasource.lance.index;

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.GenericPool;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * The production {@link LanceIndexMutationDispatcher}: the blocking thrift client from the
 * shared backend pool, following the established borrow/call/return-or-invalidate discipline.
 * Deliberately not the gRPC BackendServiceProxy - the mutation contract (commit 2) is a thrift
 * method, and a second rpc face would be a new protocol, not a different transport.
 *
 * <p>The remaining statement budget is applied as the borrowed connection's socket timeout, so
 * the send, the read, and everything between are bounded by what the statement has left. The
 * timeout is restored to the pool default before the connection is returned: a connection
 * borrowed with a short budget must not leak that timeout onto the next borrower of the shared
 * pooled object. A call that did not complete cleanly invalidates the connection instead of
 * returning it, and a pool failure while returning a completed connection only downgrades that
 * connection to invalidated - it never discards the trusted answer already in hand.
 */
public final class ThriftLanceIndexMutationDispatcher implements LanceIndexMutationDispatcher {

    public static final ThriftLanceIndexMutationDispatcher INSTANCE = new ThriftLanceIndexMutationDispatcher();

    private final GenericPool<BackendService.Client> backendPool;

    private ThriftLanceIndexMutationDispatcher() {
        this(ClientPool.backendPool);
    }

    @VisibleForTesting
    ThriftLanceIndexMutationDispatcher(GenericPool<BackendService.Client> backendPool) {
        this.backendPool = backendPool;
    }

    @Override
    public TLanceIndexMutationResult dispatch(TLanceIndexMutationRequest request, Backend backend,
            long timeoutMillis) throws Exception {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        // The borrow is the judgment seam between provable pre-send failures and
        // possible-send failures: everything borrowObject can throw belongs to connection
        // establishment (the pool factory's transport.open(): connect refused, connect
        // timeout, TLS handshake) or pool admission, and the mutation invocation is written
        // only by lanceIndexMutate below - so a failure here proves zero payload bytes were
        // sent and propagates as the typed pre-dispatch marker for the classifier's
        // confirmed-failure mapping, no exception-type guessing involved. The connect itself
        // still uses the pool's own connect timeout (backend_rpc_timeout_ms, 60s by default
        // at class load) rather than the remaining budget - GenericPool offers no overload
        // that applies the caller's timeout to the connect phase; the statement-side budget
        // wait still bounds what the client observes.
        BackendService.Client client;
        try {
            client = backendPool.borrowObject(address,
                    (int) Math.max(1, Math.min(timeoutMillis, Integer.MAX_VALUE)));
        } catch (Exception e) {
            throw new LanceIndexMutationDispatcher.PreDispatchTransportException(e);
        }
        // The single send. Everything this call does - including the blocking wait for the
        // answer - is bounded by the socket timeout set at borrow time. Any failure from
        // here on may have already written the request, so it propagates raw and stays
        // indeterminate at the classifier.
        boolean callCompleted = false;
        try {
            TLanceIndexMutationResult result = client.lanceIndexMutate(request);
            callCompleted = true;
            return result;
        } finally {
            release(address, client, callCompleted);
        }
    }

    /**
     * Returns or invalidates the connection without ever letting a pool failure replace the
     * outcome this dispatch already produced: a completed call holds a trusted result in
     * hand, so a failure while returning it (pool closed, passivation) only downgrades the
     * connection to invalidated - the result itself still reaches the classifier.
     * {@code invalidateObject} already swallows its own failures, so the degrade path cannot
     * throw either.
     */
    private void release(TNetworkAddress address, BackendService.Client client,
            boolean callCompleted) {
        if (!callCompleted) {
            backendPool.invalidateObject(address, client);
            return;
        }
        try {
            restorePoolDefaultTimeout(client);
            backendPool.returnObject(address, client);
        } catch (RuntimeException e) {
            backendPool.invalidateObject(address, client);
        }
    }

    private static void restorePoolDefaultTimeout(BackendService.Client client) {
        TTransport transport = client.getOutputProtocol().getTransport();
        if (transport instanceof TSocket) {
            ((TSocket) transport).setTimeout(Config.backend_rpc_timeout_ms);
        }
    }
}
