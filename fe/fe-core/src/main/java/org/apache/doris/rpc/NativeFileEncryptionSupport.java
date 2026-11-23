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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.tls.server.FeServerStarterFactory;

import java.util.Collection;

/** Runtime proof for the native reader and every private channel carrying file keys. */
public final class NativeFileEncryptionSupport {
    private NativeFileEncryptionSupport() {
    }

    private static boolean protectedTransports() {
        return BackendServiceClient.supportsPrivateFileKeys()
                && ClientPool.backendHeartbeatPool.supportsPrivateFileKeys()
                && FeServerStarterFactory.supportsPrivateFileKeys();
    }

    public static boolean supported() {
        try {
            // Live backends outside the query's compute group or scheduling policy cannot execute this scan.
            return allBackendsSupported(protectedTransports(), FederationBackendPolicy.getQueryAvailableBackends());
        } catch (UserException unavailableCluster) {
            return false;
        }
    }

    public static boolean supported(Backend backend) {
        return protectedTransports() && backend.isNativeFileEncryptionSupported();
    }

    public static boolean supported(TNetworkAddress address) {
        for (Backend backend : Env.getCurrentSystemInfo().getAllClusterBackendsNoException().values()) {
            if (backend.getBrpcAddress().equals(address)) {
                return supported(backend);
            }
        }
        return false;
    }

    static boolean allBackendsSupported(boolean protectedTransports, Collection<Backend> backends) {
        return protectedTransports && !backends.isEmpty()
                && backends.stream().allMatch(Backend::isNativeFileEncryptionSupported);
    }

    public static void require(long backendId, TNetworkAddress address) {
        // A captured Backend can outlive its registration or move ports after the RPC target was bound.
        Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null || !backend.getBrpcAddress().equals(address) || !supported(backend)) {
            throw new IllegalStateException("File encryption [READ_UNAVAILABLE]");
        }
    }
}
