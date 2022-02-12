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

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.doris.proto.FunctionService;
import org.apache.doris.proto.PFunctionServiceGrpc;
import org.apache.doris.proto.Types;

import java.util.List;

/**
 * FunctionGrpc
 *
 * @author lirongqian
 * @since 2022/02/08
 */
@GrpcService
public class FunctionGrpc extends PFunctionServiceGrpc.PFunctionServiceImplBase {
    
    @Override
    public void fnCall(FunctionService.PFunctionCallRequest request, StreamObserver<FunctionService.PFunctionCallResponse> responseObserver) {
        String functionName = request.getFunctionName();
        FunctionService.PFunctionCallResponse res;
        if ("grpc_add".equals(functionName)) {
            List<Types.PValues> argsList = request.getArgsList();
            int sum = 0;
            for (Types.PValues pValues : argsList) {
                sum += pValues.getInt32Value(0) % 10;
            }
            res = FunctionService.PFunctionCallResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder()
                            .setStatusCode(0)
                            .build())
                    .setResult(Types.PValues.newBuilder()
                            .setHasNull(false)
                            .addInt32Value(sum)
                            .setType(Types.PGenericType.newBuilder()
                                    .setId(Types.PGenericType.TypeId.INT32).build()).build()).build();
        } else {
            res = FunctionService.PFunctionCallResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder()
                            .setStatusCode(0)
                            .build()).build();
        }
        ok(responseObserver, res);
    }
    
    @Override
    public void checkFn(FunctionService.PCheckFunctionRequest request, StreamObserver<FunctionService.PCheckFunctionResponse> responseObserver) {
        FunctionService.PCheckFunctionResponse res = FunctionService.PCheckFunctionResponse.newBuilder()
                .setStatus(Types.PStatus.newBuilder().setStatusCode(0).build()).build();
        ok(responseObserver, res);
    }
    
    public static <T> void ok(StreamObserver<T> observer, T data) {
        observer.onNext(data);
        observer.onCompleted();
    }
    
}