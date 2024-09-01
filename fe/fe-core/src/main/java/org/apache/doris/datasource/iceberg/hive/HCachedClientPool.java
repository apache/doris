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

package org.apache.doris.datasource.iceberg.hive;

import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.util.PropertyUtil;
import shade.doris.hive.org.apache.thrift.TException;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HCachedClientPool implements ClientPool<IMetaStoreClient, TException> {

    private static volatile Cache<String, HClientPool> clientPoolCache;
    private static final Object clientPoolCacheLock = new Object();
    private final String catalogName;
    private final Configuration conf;
    private final int clientPoolSize;
    private final long evictionInterval;
    private final HadoopAuthenticator authenticator;

    public HCachedClientPool(String catalogName, Configuration conf, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.conf = conf;
        this.clientPoolSize =
            PropertyUtil.propertyAsInt(
                properties,
                CatalogProperties.CLIENT_POOL_SIZE,
                CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        this.evictionInterval =
            PropertyUtil.propertyAsLong(
                properties,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);

        if (clientPoolCache == null) {
            synchronized (clientPoolCacheLock) {
                if (clientPoolCache == null) {
                    clientPoolCache =
                        Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener((key, value, cause) -> ((HClientPool) value).close())
                            .build();
                }
            }
        }
        AuthenticationConfig authConfig = AuthenticationConfig.getKerberosConfig(conf);
        authenticator = HadoopAuthenticator.getHadoopAuthenticator(authConfig);
    }

    protected HClientPool clientPool() {
        return clientPoolCache.get(this.catalogName, (k) -> new HClientPool(this.clientPoolSize, this.conf));
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
        try {
            return authenticator.doAs(() -> clientPool().run(action));
        } catch (IOException e) {
            throw new TException(e);
        } catch (UndeclaredThrowableException e) {
            if (e.getCause() instanceof TException) {
                throw (TException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
            throws TException, InterruptedException {
        try {
            return authenticator.doAs(() -> clientPool().run(action, retry));
        } catch (IOException e) {
            throw new TException(e);
        } catch (UndeclaredThrowableException e) {
            if (e.getCause() instanceof TException) {
                throw (TException) e.getCause();
            }
            throw e;
        }
    }
}
