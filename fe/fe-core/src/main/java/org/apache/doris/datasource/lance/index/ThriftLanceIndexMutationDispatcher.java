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
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;
import org.apache.doris.thrift.TNetworkAddress;

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
 * returning it.
 */
public final class ThriftLanceIndexMutationDispatcher implements LanceIndexMutationDispatcher {

    public static final ThriftLanceIndexMutationDispatcher INSTANCE = new ThriftLanceIndexMutationDispatcher();

    private ThriftLanceIndexMutationDispatcher() {
    }

    @Override
    public TLanceIndexMutationResult dispatch(TLanceIndexMutationRequest request, Backend backend,
            long timeoutMillis) throws Exception {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        BackendService.Client client = null;
        boolean callCompleted = false;
        try {
            client = ClientPool.backendPool.borrowObject(address,
                    (int) Math.max(1, Math.min(timeoutMillis, Integer.MAX_VALUE)));
            // The single send. Everything this call does - including the blocking wait for the
            // answer - is bounded by the socket timeout set at borrow time.
            TLanceIndexMutationResult result = client.lanceIndexMutate(request);
            callCompleted = true;
            return result;
        } finally {
            if (client != null) {
                if (callCompleted) {
                    restorePoolDefaultTimeout(client);
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
    }

    private static void restorePoolDefaultTimeout(BackendService.Client client) {
        TTransport transport = client.getOutputProtocol().getTransport();
        if (transport instanceof TSocket) {
            ((TSocket) transport).setTimeout(Config.backend_rpc_timeout_ms);
        }
    }
}
