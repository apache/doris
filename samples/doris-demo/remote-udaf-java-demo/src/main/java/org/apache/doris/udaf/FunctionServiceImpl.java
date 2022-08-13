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

package org.apache.doris.udaf;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.doris.proto.FunctionService;
import org.apache.doris.proto.PFunctionServiceGrpc;
import org.apache.doris.proto.Types;

import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.List;
import java.util.*;




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
        FunctionService.PFunctionCallResponse.Builder res = FunctionService.PFunctionCallResponse.newBuilder();
        if ("rpc_sum_update".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.INT64);

            long total = 0;
            int size = request.getArgs(0).getInt64ValueCount();
            for(int i=0;i<size;i++){
                total += request.getArgs(0).getInt64Value(i);
            }
            if(request.getContext().getFunctionContext().getArgsDataCount()>0){
                total += request.getContext().getFunctionContext().getArgsData(0).getInt64Value(0);
            }
            pValues.setHasNull(false);
            pValues.addInt64Value(total);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        } 
        if ("rpc_sum_merge".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.INT64);
            int size = request.getArgsCount();
            long total = 0;
            for(int i=0;i<size;i++){
                total += request.getArgs(i).getInt64Value(0);
            }
            pValues.setHasNull(false);
            pValues.addInt64Value(total);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }
        if ("rpc_sum_finalize".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.INT64);
            int size = request.getArgsCount();
            long total = request.getContext().getFunctionContext().getArgsData(0).getInt64Value(0);
            pValues.setHasNull(false);
            pValues.addInt64Value(total);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }
        if ("rpc_avg_update".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.DOUBLE);
            int size = request.getArgs(0).getInt32ValueCount();
            double  total = 0;
            for(int i=0;i<size;i++){
                total += request.getArgs(0).getInt32Value(i);
            }
            if(request.getContext().getFunctionContext().getArgsDataCount()>0){
                total += request.getContext().getFunctionContext().getArgsData(0).getDoubleValue(0);
                size += request.getContext().getFunctionContext().getArgsData(0).getInt32Value(0);
            }
            pValues.setHasNull(false);
            pValues.addDoubleValue(total);
            pValues.addInt32Value(size);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }

        if ("rpc_avg_merge".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.DOUBLE);
            int size = 0;
            double  total = 0;
            int args_len = request.getArgsCount();
            for(int i=0;i<args_len;i++){
                total += request.getArgs(i).getDoubleValue(0);
                size += request.getArgs(i).getInt32Value(0);
            }
            pValues.setHasNull(false);
            pValues.addDoubleValue(total);
            pValues.addInt32Value(size);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }

        if ("rpc_avg_finalize".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.DOUBLE);
            double total = request.getContext().getFunctionContext().getArgsData(0).getDoubleValue(0);
            int size = request.getContext().getFunctionContext().getArgsData(0).getInt32Value(0);
            
            pValues.setHasNull(false);
            pValues.addDoubleValue(total/size);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }
        if ("rpc_count_visit_info_update".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.STRING);

            long total = 0;
            int size = request.getArgs(0).getStringValueCount();

            JSONObject GlobalIpMap = new JSONObject();
            if(request.getContext().getFunctionContext().getArgsDataCount()>0
              && request.getContext().getFunctionContext().getArgsData(0).getStringValueCount()>0 ){
                String  s= request.getContext().getFunctionContext().getArgsData(0).getStringValue(0);
                GlobalIpMap = JSONObject.parseObject(s);
            }
            for(int i=0;i<size;i++){
                String str = request.getArgs(0).getStringValue(i);
                JSONObject object = JSONObject.parseObject(str);
                String ip = object.getString("ip");
                if(GlobalIpMap.containsKey(ip)){
                    int num = GlobalIpMap.getIntValue(ip);
                    num ++;
                    GlobalIpMap.put(ip,num);
                }else{
                    GlobalIpMap.put(ip,1);
                }
            }
            String objStr = JSON.toJSONString(GlobalIpMap);
            pValues.setHasNull(false);
            pValues.addStringValue(objStr);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }
        if ("rpc_count_visit_info_merge".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.STRING);

            String  s1= request.getArgs(0).getStringValue(0);
            JSONObject GlobalIpMap1 = JSONObject.parseObject(s1);
            String  s2= request.getArgs(1).getStringValue(0);
            JSONObject GlobalIpMap2 = JSONObject.parseObject(s2);
            Iterator iter = GlobalIpMap2.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String ip = (entry.getKey().toString());
                int num2 = Integer.parseInt(entry.getValue().toString());
                if(GlobalIpMap1.containsKey(ip)){
                    int num1 = GlobalIpMap1.getIntValue(ip);
                    int total = num1+num2;
                    GlobalIpMap1.put(ip,total);
                }else{
                    GlobalIpMap1.put(ip,num2);
                }
            }
            String objStr = JSON.toJSONString(GlobalIpMap1);
            pValues.setHasNull(false);
            pValues.addStringValue(objStr);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }
        if ("rpc_count_visit_info_finalize".equals(functionName)) {
            Types.PStatus.Builder statusCode = Types.PStatus.newBuilder().setStatusCode(0);
            Types.PValues.Builder pValues = Types.PValues.newBuilder();
            Types.PGenericType.Builder returnType = Types.PGenericType.newBuilder().setId(Types.PGenericType.TypeId.STRING);
            String  s= request.getContext().getFunctionContext().getArgsData(0).getStringValue(0);
            JSONObject finalMap = JSONObject.parseObject(s);
            Map<String,Integer> mapRepeat = new HashMap<String,Integer>();
            Iterator iter = finalMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String key = entry.getKey().toString();
                int value = Integer.parseInt(entry.getValue().toString());
                mapRepeat.put(key, value);
            }
            List<Map.Entry<String, Integer>> list1 = new ArrayList<Map.Entry<String, Integer>>();
            list1.addAll(mapRepeat.entrySet());
            Collections.sort(list1, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            int topN=3;
            String sortedOutputStr="";
            for (Map.Entry<String, Integer> entry : list1) {
                topN --;
                if(topN<0){
                    break;
                }
                sortedOutputStr += entry.getKey()+":"+entry.getValue()+" ";
            }
            pValues.setHasNull(false);
            pValues.addStringValue(sortedOutputStr);
            pValues.setType(returnType.build());
            res.setStatus(statusCode.build());
            res.addResult(pValues.build());
        }
        logger.info("fnCall res=" + res);
        ok(responseObserver, res.build());
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
