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

package org.apache.doris.datasource.hive.security;

import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class CachingKerberosAuthenticator implements HadoopAuthenticator {
    private final AuthenticationConfig config;
    private final LoadingCache<HiveMetaStoreCache.AuthenticatorCacheKey, UserGroupInformation> authenticatorCache;

    public CachingKerberosAuthenticator(AuthenticationConfig config,
                                        LoadingCache<HiveMetaStoreCache.AuthenticatorCacheKey,
                                            UserGroupInformation> authCacheLoader) {
        this.config = config;
        this.authenticatorCache = authCacheLoader;
    }

    @Override
    public UserGroupInformation getUGI() throws IOException {
        return authenticatorCache.get(HiveMetaStoreCache.AuthenticatorCacheKey.createHadoopAuthKey(config));
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {
        return getUGI().doAs(action);
    }
}
