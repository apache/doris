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

package org.apache.doris.connector.paimon;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.security.PrivilegedAction;

/** Applies the HMS identity at Paimon's metastore client acquisition and RPC boundary. */
final class PaimonHmsClientPool implements ClientPool<IMetaStoreClient, TException> {

    private final ClientPool<IMetaStoreClient, TException> delegate;
    private final HadoopAuthenticator authenticator;

    private PaimonHmsClientPool(ClientPool<IMetaStoreClient, TException> delegate,
            HadoopAuthenticator authenticator) {
        this.delegate = delegate;
        this.authenticator = authenticator;
    }

    static ClientPool<IMetaStoreClient, TException> wrap(
            ClientPool<IMetaStoreClient, TException> delegate, HadoopAuthenticator authenticator) {
        return new PaimonHmsClientPool(delegate, authenticator);
    }

    static Catalog install(Catalog catalog, HadoopAuthenticator authenticator) {
        if (authenticator == null) {
            return catalog;
        }
        Catalog root = DelegateCatalog.rootCatalog(catalog);
        if (!(root instanceof HiveCatalog)) {
            throw new IllegalStateException("Expected a Paimon HiveCatalog for HMS authentication");
        }
        try {
            Field clients = HiveCatalog.class.getDeclaredField("clients");
            clients.setAccessible(true);
            @SuppressWarnings("unchecked")
            ClientPool<IMetaStoreClient, TException> delegate =
                    (ClientPool<IMetaStoreClient, TException>) clients.get(root);
            // Paimon exposes no client-pool injection seam; replacing only this field prevents the HMS user
            // from leaking into FileIO while covering both cached-pool client creation and every metastore RPC.
            clients.set(root, wrap(delegate, authenticator));
            return catalog;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to install Paimon HMS authentication boundary", e);
        }
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
        try {
            return authenticator.getUGI().doAs(
                    (PrivilegedAction<R>) () -> runUnchecked(action));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to execute a Paimon HMS operation", e);
        } catch (ClientPoolException e) {
            throw (TException) e.getCause();
        } catch (ClientPoolInterruptedException e) {
            Thread.currentThread().interrupt();
            throw (InterruptedException) e.getCause();
        }
    }

    private <R> R runUnchecked(Action<R, IMetaStoreClient, TException> action) {
        try {
            return delegate.run(action);
        } catch (TException e) {
            throw new ClientPoolException(e);
        } catch (InterruptedException e) {
            throw new ClientPoolInterruptedException(e);
        }
    }

    @Override
    public void execute(ExecuteAction<IMetaStoreClient, TException> action)
            throws TException, InterruptedException {
        run(client -> {
            action.run(client);
            return null;
        });
    }

    private static final class ClientPoolException extends RuntimeException {
        private ClientPoolException(TException cause) {
            super(cause);
        }
    }

    private static final class ClientPoolInterruptedException extends RuntimeException {
        private ClientPoolInterruptedException(InterruptedException cause) {
            super(cause);
        }
    }
}
