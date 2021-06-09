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

import org.apache.doris.common.ThriftServer;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.MockedBackendFactory.BeThriftService;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.apache.thrift.TProcessor;

import java.io.IOException;

/*
 * Mocked Backend
 * A mocked Backend has 3 rpc services. 
 *      HeartbeatService.Iface to handle heart beat from Frontend.
 *      BeThriftService to handle agent tasks and other requests from Frontend.
 *      BRpcService to handle the query request from Frontend.
 *      
 * Users can create a BE by customizing three rpc services.
 * 
 * Better to create a mocked Backend from MockedBackendFactory.
 * In MockedBackendFactory, there default rpc service for above 3 rpc services.
 */
public class MockedBackend {

    private ThriftServer heartbeatServer;
    private ThriftServer beThriftServer;
    private Server backendServer;
    
    private String host;
    private int heartbeatPort;
    private int thriftPort;
    private int brpcPort;
    private int httpPort;
    // the fe address: fe host and fe rpc port.
    // This must be set explicitly after creating mocked Backend
    private TNetworkAddress feAddress;

    public MockedBackend(String host, int heartbeatPort, int thriftPort, int brpcPort, int httpPort,
            HeartbeatService.Iface hbService,
            BeThriftService backendService, PBackendServiceGrpc.PBackendServiceImplBase pBackendService)
            throws IOException {

        this.host = host;
        this.heartbeatPort = heartbeatPort;
        this.thriftPort = thriftPort;
        this.brpcPort = brpcPort;
        this.httpPort = httpPort;

        createHeartbeatService(heartbeatPort, hbService);
        createBeThriftService(thriftPort, backendService);
        createBrpcService(brpcPort, pBackendService);

        backendService.setBackend(this);
        backendService.init();
    }

    public void setFeAddress(TNetworkAddress feAddress) {
        this.feAddress = feAddress;
    }

    public TNetworkAddress getFeAddress() {
        return feAddress;
    }

    public String getHost() {
        return host;
    }

    public int getHeartbeatPort() {
        return heartbeatPort;
    }

    public int getBeThriftPort() {
        return thriftPort;
    }

    public int getBrpcPort() {
        return brpcPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void start() throws IOException {
        heartbeatServer.start();
        System.out.println("Be heartbeat service is started with port: " + heartbeatPort);
        beThriftServer.start();
        System.out.println("Be thrift service is started with port: " + thriftPort);
        backendServer.start();
        System.out.println("Be brpc service is started with port: " + brpcPort);
    }

    private void createHeartbeatService(int heartbeatPort, HeartbeatService.Iface serviceImpl) throws IOException {
        TProcessor tprocessor = new HeartbeatService.Processor<HeartbeatService.Iface>(serviceImpl);
        heartbeatServer = new ThriftServer(heartbeatPort, tprocessor);
    }

    private void createBeThriftService(int beThriftPort, BackendService.Iface serviceImpl) throws IOException {
        TProcessor tprocessor = new BackendService.Processor<BackendService.Iface>(serviceImpl);
        beThriftServer = new ThriftServer(beThriftPort, tprocessor);
    }

    private void createBrpcService(int brpcPort, PBackendServiceGrpc.PBackendServiceImplBase pBackendServiceImpl) {
        backendServer = ServerBuilder.forPort(brpcPort)
                .addService(pBackendServiceImpl).build();
    }
}
