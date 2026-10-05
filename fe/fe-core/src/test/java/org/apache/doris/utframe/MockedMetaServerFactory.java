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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.GetStageResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.MetaServiceResponseStatus;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.MetaServiceGrpc;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/*
 * This class is used to create mock metaServer.
 * Usage can be found in Demon.java's beforeClass()
 *
 *
 */
public class MockedMetaServerFactory {
    public static final String METASERVER_DEFAULT_IP = "127.0.0.100";
    public static final int METASERVER_DEFAULT_BRPC_PORT = 5001;
    private static final Logger LOG = LogManager.getLogger(MockedMetaServerFactory.class);

    // create a mocked meta server with customize parameters
    public static MockedMetaServer createMetaServer(String host, int brpcPort,
                                                    MetaServiceGrpc.MetaServiceImplBase pMetaService)
            throws IOException {
        MockedMetaServer metaServer = new MockedMetaServer(host, brpcPort, pMetaService);
        return metaServer;
    }

    // The default Brpc service.
    public static class DefaultPMetaServiceImpl extends MetaServiceGrpc.MetaServiceImplBase {
        @Override
        public void getVersion(Cloud.GetVersionRequest request,
                               StreamObserver<Cloud.GetVersionResponse> responseObserver) {
            responseObserver.onNext(Cloud.GetVersionResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setVersion(1).build());
            responseObserver.onCompleted();
        }

        @Override
        public void getStage(Cloud.GetStageRequest request,
                               StreamObserver<Cloud.GetStageResponse> responseObserver) {
            if (request.hasCloudUniqueId()) {
                // reuse uniqueId for mock ut response
                switch (request.getCloudUniqueId()) {
                    case "Internal-MetaServiceCode.OK":
                        ObjectStoreInfoPB obj = ObjectStoreInfoPB.newBuilder()
                                .setEndpoint("cos.ap-beijing.myqcloud.internal.com")
                                .setAk("akak").setSk("sksk").setRegion("ap-beijing")
                                .setBucket("bucketbucket").setExternalEndpoint("cos.ap-beijing.myqcloud.com")
                                .setPrefix("ut-test").setProvider(Provider.OSS).build();
                        StagePB stage = StagePB.newBuilder().setObjInfo(obj).build();
                        GetStageResponse resp = GetStageResponse.newBuilder()
                                .setStatus(MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"))
                                .addStage(stage).build();
                        responseObserver.onNext(resp);
                        responseObserver.onCompleted();
                        LOG.info("mock get Stage request: {}, response: {}", request, resp);
                        return;
                    default:
                        return;
                }
            }
        }
    }
}
