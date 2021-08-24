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

package org.apache.doris.stack.rest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class ResponseEntityBuilder {
    public static ResponseEntity badRequest(Object data) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.BAD_REQUEST).msg("异常请求：" + data);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity okWithCommonError(String msg) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.COMMON_ERROR).msg("错误：" + msg);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity okWithCommonError(String msg, RestApiStatusCode code) {
        ResponseBody body = new ResponseBody().code(code).msg("错误：" + msg);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity ok(Object data) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.OK).msg("成功").data(data);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity ok() {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.OK).msg("成功");
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity unauthorized(Object data) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.UNAUTHORIZED).msg("错误：" + data);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity noPermission(Object data) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.NOPERMISSION).msg("错误：" + data);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity internalError(Object data) {
        ResponseBody body = new ResponseBody()
                .code(RestApiStatusCode.INTERNAL_SERVER_ERROR)
                .msg("Internal Error:" + data);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity notFound(Object data) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.NOT_FOUND).msg("Not Found:" + data);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }
}
