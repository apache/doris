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

package org.apache.doris.qe;

/**
 * Owns a newly created private context, including its statement resources and thread binding.
 * Construct the scope before binding the context so that the caller's binding can be restored.
 * A helper borrowing an existing context must leave cleanup to its owner instead of wrapping it.
 *
 * <p>Finish execution, copy any results and record audit information before leaving the scope.
 * Close requests AFTER_RESPONSE to avoid cancelling a completed statement; a private MySQL
 * context normally has no pending protocol response and is cleaned up immediately. The context
 * is terminal afterwards and cannot be reused for another statement.
 *
 * <p>The scope does not wait for arbitrary background work. A worker that needs this context must
 * own the scope for its full execution. Work that outlives the scope must own independent resources,
 * as deferred connector split cleanup does, without relying on the context remaining open.
 */
public class AutoCloseConnectContext implements AutoCloseable {

    public final ConnectContext connectContext;

    private final ConnectContext previousContext;

    public AutoCloseConnectContext(ConnectContext connectContext) {
        this.previousContext = ConnectContext.get();
        this.connectContext = connectContext;
        connectContext.setThreadLocalInfo();
    }

    public void call() {
        // try (AutoCloseConnectContext autoCloseCtx = new AutoCloseConnectContext(context)) {
        // will report autoCloseCtx is not used, so call an empty method.
    }

    @Override
    public void close() {
        try {
            // The synchronous scope has exited. Finish without cancelling its completed statement.
            connectContext.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }
}
