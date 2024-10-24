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

package org.apache.doris.utframe;

import org.apache.doris.cloud.proto.MetaServiceGrpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

/*
 * Mocked MetaServer
 * A mocked MetaServer has brpc services.
 *      BRpcService to handle the query request from Frontend.
 *
 * Users can create a MetaServer by customizing rpc services.
 *
 * Better to create a mocked MetaServer from MockedMetaServerFactory.
 * In MockedMetaServerFactory, there default rpc service for above brpc service.
 */
public class MockedMetaServer {
    private Server metaServer;

    private String host;
    private int brpcPort;

    public MockedMetaServer(String host, int brpcPort,
                            MetaServiceGrpc.MetaServiceImplBase pMetaService)
            throws IOException {

        this.host = host;
        this.brpcPort = brpcPort;
        createBrpcService(brpcPort, pMetaService);
    }


    public String getHost() {
        return host;
    }

    public int getBrpcPort() {
        return brpcPort;
    }


    public void start() throws IOException {
        metaServer.start();
        System.out.println("MetaServer brpc service is started with port: " + brpcPort);
    }

    private void createBrpcService(int brpcPort, MetaServiceGrpc.MetaServiceImplBase pMetaServiceImpl) {
        metaServer = ServerBuilder.forPort(brpcPort)
                .addService(pMetaServiceImpl).build();
    }
}
