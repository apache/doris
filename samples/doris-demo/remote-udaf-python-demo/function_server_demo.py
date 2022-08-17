#!/usr/bin/env python
# encoding: utf-8

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
from concurrent import futures

import grpc

import function_service_pb2
import function_service_pb2_grpc
import types_pb2
import  sys
import time;
import json



class FunctionServerDemo(function_service_pb2_grpc.PFunctionServiceServicer):
    def fn_call(self, request, context):
        response = function_service_pb2.PFunctionCallResponse()
        status = types_pb2.PStatus()
        status.status_code = 0
        response.status.CopyFrom(status)
        if request.function_name == "rpc_sum_update":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.INT64
            result.type.CopyFrom(result_type)
            total=0
            size= len(request.args[0].int64_value)
            for i in range(size):
                total += request.args[0].int64_value[i]
    
            if request.HasField("context"):
                total += request.context.function_context.args_data[0].int64_value[0]
            result.int64_value.append(total)
            response.result.append(result)

        if request.function_name == "rpc_sum_merge":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.INT64
            result.type.CopyFrom(result_type)
            args_len = len(request.args)
            total = 0
            for i in range(args_len):
                total += request.args[i].int64_value[0]
            result.int64_value.append(total)
            response.result.append(result)
        
        if request.function_name == "rpc_sum_finalize":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.INT64
            result.type.CopyFrom(result_type)
            total = request.context.function_context.args_data[0].int64_value[0]
            result.int64_value.append(total)
            response.result.append(result)

        if request.function_name == "rpc_avg_update":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.DOUBLE
            result.type.CopyFrom(result_type)
            total = 0
            size = len(request.args[0].int32_value)
            for i in range(size):
                total += request.args[0].int32_value[i]

            if request.HasField("context"):
                total += request.context.function_context.args_data[0].double_value[0]
                size += request.context.function_context.args_data[0].int32_value[0]

            result.double_value.append(total)
            result.int32_value.append(size)
            response.result.append(result)

        if request.function_name == "rpc_avg_merge":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.DOUBLE
            result.type.CopyFrom(result_type)
            total = 0
            size = 0
            args_len = len(request.args)
            for i in range(args_len):
                total += request.args[i].double_value[0]
                size += request.args[i].int32_value[0]
            result.double_value.append(total)
            result.int32_value.append(size)
            response.result.append(result)

        if request.function_name == "rpc_avg_finalize":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.DOUBLE
            result.type.CopyFrom(result_type)
            total =  request.context.function_context.args_data[0].double_value[0]
            size =  request.context.function_context.args_data[0].int32_value[0]
            avg = total / size
            result.double_value.append(avg)
            response.result.append(result)
        if request.function_name == "rpc_count_visit_info_update":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.STRING
            result.type.CopyFrom(result_type)
            size =  len(request.args[0].string_value)
            currentMap=dict()
            if request.HasField("context"):
                context = request.context.function_context.args_data[0].string_value[0]
                currentMap = json.loads(context)
            for i in range(size):
                s = request.args[0].string_value[i]
                mapInfo = json.loads(s)
                ip=mapInfo['ip']
                if currentMap.has_key(ip):
                    last_val=currentMap[ip]
                    last_val+=1
                    currentMap[ip] = last_val
                else:
                    currentMap[ip] = 1
            json_dict = json.dumps(currentMap)
            result.string_value.append(json_dict)
            response.result.append(result)

        if request.function_name == "rpc_count_visit_info_merge":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.STRING
            result.type.CopyFrom(result_type)

            context1 = request.args[0].string_value[0]
            currentMap1 = json.loads(context1)
            context2 = request.args[1].string_value[0]
            currentMap2 = json.loads(context2)
            for ip,num in  currentMap2.items():
                if currentMap1.has_key(ip):
                    currentMap1[ip] = currentMap1[ip] + num
                else:
                    currentMap1[ip] = num
            json_dict = json.dumps(currentMap1)
            result.string_value.append(json_dict)
            response.result.append(result)

        if request.function_name == "rpc_count_visit_info_finalize":
            result = types_pb2.PValues()
            result.has_null = False
            result_type = types_pb2.PGenericType()
            result_type.id = types_pb2.PGenericType.STRING
            result.type.CopyFrom(result_type)

            context = request.context.function_context.args_data[0].string_value[0]
            currentMap = json.loads(context)
            sortedMap=sorted(currentMap.items(), key = lambda kv:(kv[1], kv[0]),reverse=True) 
            resultMap=dict()
            topN=3
            if len(sortedMap) < topN:
                topN = len(sortedMap)
            finalResult=""
            print(sortedMap)
            for i in  range(topN):
                ip=sortedMap[i][0]
                finalResult +=ip +":"+str(sortedMap[i][1]) +" "
            result.string_value.append(finalResult)
            response.result.append(result)
        return response

    def check_fn(self, request, context):
        response = function_service_pb2.PCheckFunctionResponse()
        status = types_pb2.PStatus()
        status.status_code = 0
        response.status.CopyFrom(status)
        return response

    def hand_shake(self, request, context):
        response = types_pb2.PHandShakeResponse()
        if request.HasField("hello"):
            response.hello = request.hello
        status = types_pb2.Pstatus()
        status.status_code = 0
        response.status.CopyFrom(status)
        return response


def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    function_service_pb2_grpc.add_PFunctionServiceServicer_to_server(FunctionServerDemo(), server)
    server.add_insecure_port("0.0.0.0:%s" % port)
    server.start()
    while True:
        time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig()
    port = 9000
    if len(sys.argv) > 1:
        port = sys.argv[1]
    serve(port)
