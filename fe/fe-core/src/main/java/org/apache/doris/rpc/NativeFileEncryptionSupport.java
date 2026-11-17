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

package org.apache.doris.rpc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.tls.server.FeServerStarterFactory;

import java.util.Collection;
import java.util.List;

/** Runtime proof for the native reader and every private channel carrying file keys. */
public final class NativeFileEncryptionSupport {
    private NativeFileEncryptionSupport() {
    }

    private static boolean protectedTransports() {
        return BackendServiceClient.supportsPrivateFileKeys()
                && ClientPool.backendHeartbeatPool.supportsPrivateFileKeys()
                && FeServerStarterFactory.supportsPrivateFileKeys();
    }

    public static int version() {
        try {
            // Admission precedes backend assignment: require every currently selectable live backend.
            List<Backend> backends = Env.getCurrentSystemInfo().getBackendsByCurrentCluster().values().stream()
                    .filter(Backend::isAlive).toList();
            return minimumVersion(protectedTransports(), backends);
        } catch (AnalysisException unavailableCluster) {
            return 0;
        }
    }

    public static int version(Backend backend) {
        return protectedTransports()
                ? Math.min(FileScanEncryption.VERSION, backend.getNativeFileEncryptionVersion()) : 0;
    }

    public static int version(TNetworkAddress address) {
        for (Backend backend : Env.getCurrentSystemInfo().getAllClusterBackendsNoException().values()) {
            if (backend.getBrpcAddress().equals(address)) {
                return version(backend);
            }
        }
        return 0;
    }

    static int minimumVersion(boolean protectedTransports, Collection<Backend> backends) {
        if (!protectedTransports || backends.isEmpty()) {
            return 0;
        }
        int minimum = FileScanEncryption.VERSION;
        for (Backend backend : backends) {
            minimum = Math.min(minimum, backend.getNativeFileEncryptionVersion());
        }
        return minimum;
    }

    public static void require(Backend backend) {
        if (version(backend) < FileScanEncryption.VERSION) {
            throw new IllegalStateException("File encryption [READ_UNAVAILABLE]");
        }
    }
}
