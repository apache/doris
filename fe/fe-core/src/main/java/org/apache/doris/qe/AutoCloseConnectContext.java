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
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }
}
