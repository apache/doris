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

package org.apache.doris.udf;

import org.apache.doris.proto.FunctionService;
import org.apache.doris.proto.PFunctionServiceGrpc;
import org.apache.doris.proto.Types;

import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.grpc.stub.StreamObserver;

public class FunctionServiceImpl extends PFunctionServiceGrpc.PFunctionServiceImplBase {
    private static final Logger logger = Logger.getLogger(FunctionServiceImpl.class.getName());

    public static <T> void ok(StreamObserver<T> observer, T data) {
        observer.onNext(data);
        observer.onCompleted();
    }

    @Override
    public void fnCall(FunctionService.PFunctionCallRequest request,
                       StreamObserver<FunctionService.PFunctionCallResponse> responseObserver) {
        // symbol is functionName
        String functionName = request.getFunctionName();
        logger.info("fnCall request=" + request);
        FunctionService.PFunctionCallResponse res;
        if ("add_int".equals(functionName)) {
            res = FunctionService.PFunctionCallResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0).build())
                    .addResult(Types.PValues.newBuilder().setHasNull(false)
                            .addAllInt32Value(IntStream.range(0, Math.min(request.getArgs(0)
                                            .getInt32ValueCount(), request.getArgs(1).getInt32ValueCount()))
                                    .mapToObj(i -> request.getArgs(0).getInt32Value(i) + request.getArgs(1)
                                            .getInt32Value(i)).collect(Collectors.toList()))
                            .setType(Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.INT32).build())
                            .build()).build();
        } else {
            res = FunctionService.PFunctionCallResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(1).build()).build();
        }
        logger.info("fnCall res=" + res);
        ok(responseObserver, res);
    }

    @Override
    public void checkFn(FunctionService.PCheckFunctionRequest request,
                        StreamObserver<FunctionService.PCheckFunctionResponse> responseObserver) {
        // symbol is functionName
        logger.info("checkFn request=" + request);
        int status = 0;
        if ("add_int".equals(request.getFunction().getFunctionName())) {
            // check inputs count
            if (request.getFunction().getInputsCount() != 2) {
                status = -1;
            }
        }
        FunctionService.PCheckFunctionResponse res =
                FunctionService.PCheckFunctionResponse.newBuilder()
                        .setStatus(Types.PStatus.newBuilder().setStatusCode(status).build()).build();
        logger.info("checkFn res=" + res);
        ok(responseObserver, res);
    }

    @Override
    public void handShake(Types.PHandShakeRequest request, StreamObserver<Types.PHandShakeResponse> responseObserver) {
        logger.info("handShake request=" + request);
        ok(responseObserver,
                Types.PHandShakeResponse.newBuilder().setStatus(Types.PStatus.newBuilder().setStatusCode(0).build())
                        .setHello(request.getHello()).build());
    }

}
