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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ForwardingConnectorContext;
import org.apache.doris.kerberos.HadoopAuthenticator;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/** Keep Kernel workers/reflection and Kerberos on this plugin's Hadoop class graph. */
final class TcclPinningConnectorContext extends ForwardingConnectorContext {
    private final Supplier<HadoopAuthenticator> authenticator;

    TcclPinningConnectorContext(ConnectorContext delegate, Supplier<HadoopAuthenticator> authenticator) {
        super(delegate);
        this.authenticator = authenticator;
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> action) throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(TcclPinningConnectorContext.class.getClassLoader());
            HadoopAuthenticator auth = authenticator.get();
            // The parent engine's UGI is a different class in plugin mode. A Kerberos operation
            // must authenticate through the plugin's own UGI rather than log in both copies.
            return auth == null ? delegate().executeAuthenticated(action) : auth.doAs(action::call);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
