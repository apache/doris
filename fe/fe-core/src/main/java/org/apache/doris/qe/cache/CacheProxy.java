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

package org.apache.doris.qe.cache;

import org.apache.doris.common.Status;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.MessageDigest;

/**
 * It encapsulates the request and response parameters and methods,
 * Based on this abstract class, the cache can be placed in FE/BE  and other places such as redis
 */
public abstract class CacheProxy {
    private static final Logger LOG = LogManager.getLogger(CacheBeProxy.class);
    public static int FETCH_TIMEOUT = 10000;
    public static int UPDATE_TIMEOUT = 10000;
    public static int CLEAR_TIMEOUT = 30000;

    public enum CacheProxyType {
        FE,
        BE,
        OUTER
    }

    protected CacheProxy() {
    }

    public static CacheProxy getCacheProxy(CacheProxyType type) {
        switch (type) {
            case BE:
                return new CacheBeProxy();
            case FE:
            case OUTER:
                return null;
        }
        return null;
    }

    public abstract void updateCache(InternalService.PUpdateCacheRequest request, int timeoutMs, Status status);

    public abstract InternalService.PFetchCacheResult fetchCache(InternalService.PFetchCacheRequest request,
                                                                 int timeoutMs, Status status);

    public abstract void clearCache(InternalService.PClearCacheRequest clearRequest);


    public static Types.PUniqueId getMd5(String str) {
        MessageDigest msgDigest;
        try {
            //128 bit
            msgDigest = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            return null;
        }
        final byte[] digest = msgDigest.digest(str.getBytes());
        Types.PUniqueId key = Types.PUniqueId.newBuilder()
                .setLo(getLongFromByte(digest, 0))
                .setHi(getLongFromByte(digest, 8))
                .build();
        return key;
    }

    public static final long getLongFromByte(final byte[] array, final int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = ((value << 8) | (array[offset + i] & 0xFF));
        }
        return value;
    }
}
