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

package org.apache.doris.datasource.arrowflight;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FlightSqlClientLoadBalancer implements Closeable {
    private final ImmutableList<FlightSqlClientWithOptions> clients;
    private final int clientCount;

    public FlightSqlClientLoadBalancer(List<String> hosts, String username, String password,
                                       long timeoutMs, Map<String, String> sessionProperties) {
        ImmutableList.Builder<FlightSqlClientWithOptions> builder = ImmutableList.builder();

        String sessionPropertiesSql = "";
        if (sessionProperties != null) {
            sessionPropertiesSql = sessionProperties.entrySet().stream()
                .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
                .map(e -> e.getKey() + "='" + e.getValue() + "'")
                .collect(Collectors.joining(","));
        }

        for (String host : hosts) {
            List<CallOption> callOptionList = new ArrayList<>();
            callOptionList.add(CallOptions.timeout(timeoutMs, TimeUnit.MILLISECONDS));

            String[] arrowFlightHost = host.trim().split(":");
            if (arrowFlightHost.length != 2) {
                throw new IllegalArgumentException("Invalid arrow flight host: " + host);
            }

            FlightClient flightClient = FlightClient
                    .builder(new RootAllocator(Long.MAX_VALUE),
                    Location.forGrpcInsecure(arrowFlightHost[0].trim(), Integer.parseInt(arrowFlightHost[1].trim()))
                ).build();


            Optional<CredentialCallOption> token = flightClient.authenticateBasicToken(username, password);

            if (token.isPresent()) {
                callOptionList.add(token.get());
            } else {
                flightClient.authenticateBasic(username, password);
            }
            FlightSqlClient flightSqlClient = new FlightSqlClient(flightClient);
            CallOption[] callOptions = callOptionList.toArray(new CallOption[0]);

            if (!sessionPropertiesSql.isEmpty()) {
                flightSqlClient.execute("SET " + sessionPropertiesSql, callOptions);
            }

            builder.add(new FlightSqlClientWithOptions(flightSqlClient, callOptions));
        }

        this.clients = builder.build();
        this.clientCount = clients.size();
    }

    public FlightSqlClientWithOptions randomClient() {
        return clients.get(ThreadLocalRandom.current().nextInt(clientCount));
    }

    @Override
    public void close() throws IOException {
        for (FlightSqlClientWithOptions client : clients) {
            try {
                client.flightSqlClient.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    public static class FlightSqlClientWithOptions {
        private final FlightSqlClient flightSqlClient;
        private final CallOption[] callOptions;

        public FlightSqlClientWithOptions(FlightSqlClient flightSqlClient, CallOption[] callOptions) {
            this.flightSqlClient = flightSqlClient;
            this.callOptions = callOptions;
        }

        public CallOption[] getCallOptions() {
            return callOptions;
        }

        public FlightSqlClient getFlightSqlClient() {
            return flightSqlClient;
        }
    }
}
