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

package org.apache.doris.cloud.rpc;

import org.apache.doris.system.SystemInfoService;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolver.Args;
import io.grpc.NameResolverProvider;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MetaServiceListResolverProvider extends NameResolverProvider {
    public static final Logger LOG = LogManager.getLogger(MetaServiceClient.class);
    public static final String MS_LIST_SCHEME = "ms-list";
    public static final String MS_LIST_SCHEME_PREFIX = MS_LIST_SCHEME + "://";

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, Args args) {
        return new MetaServiceListResolver(targetUri);
    }

    @Override
    public String getDefaultScheme() {
        return MS_LIST_SCHEME;
    }

    static class MetaServiceListResolver extends NameResolver {

        private Listener2 listener;

        private final URI uri;

        public MetaServiceListResolver(URI targetUri) {
            this.uri = targetUri;
        }

        private String getNamingResolverUrl() {
            return uri.getAuthority();
        }

        @Override
        public String getServiceAuthority() {
            // Be consistent with behavior in grpc-go, authority is saved in Host field of
            // URI.
            if (uri.getHost() != null) {
                return uri.getHost();
            }
            return "no host";
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void start(Listener2 listener) {
            this.listener = listener;
            this.resolve();
        }

        private void resolve() {
            try {
                List<SocketAddress> endpoints = new ArrayList<>();
                for (String endpoint : getNamingResolverUrl().split(",")) {
                    if (endpoint.isEmpty()) {
                        continue;
                    }
                    SystemInfoService.HostInfo info = SystemInfoService.getHostAndPort(endpoint);
                    endpoints.add(new InetSocketAddress(info.getHost(), info.getPort()));
                }

                List<EquivalentAddressGroup> equivalentAddressGroup = endpoints.stream()
                        // every socket address is a single EquivalentAddressGroup, so they can be
                        // accessed randomly
                        .map(Arrays::asList)
                        .map(this::addrToEquivalentAddressGroup)
                        .collect(Collectors.toList());

                ResolutionResult resolutionResult = ResolutionResult.newBuilder()
                        .setAddresses(equivalentAddressGroup)
                        .build();

                this.listener.onResult(resolutionResult);

            } catch (Exception e) {
                // when error occurs, notify listener
                this.listener.onError(Status.UNAVAILABLE
                        .withDescription(
                                "Unable to resolve host for meta service endpoint list: " + getNamingResolverUrl())
                        .withCause(e));
            }
        }

        private EquivalentAddressGroup addrToEquivalentAddressGroup(List<SocketAddress> addrList) {
            return new EquivalentAddressGroup(addrList);
        }
    }

}
