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

package org.apache.doris.http.entity;

import org.apache.doris.http.rest.RestApiStatusCode;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * A utility class for creating a ResponseEntity easier.
 */
public class ResponseEntityBuilder {

    public static ResponseEntity badRequest(Object data) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(data);
    }

    public static ResponseEntity okWithCommonError(String msg) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.COMMON_ERROR).commonError(msg);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity ok(Object data) {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.OK).msg("success").data(data);
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity ok() {
        ResponseBody body = new ResponseBody().code(RestApiStatusCode.OK).msg("success");
        return ResponseEntity.status(HttpStatus.OK).body(body);
    }

    public static ResponseEntity unauthorized(Object data) {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(data);
    }

    public static ResponseEntity internalError(Object data) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(data);
    }

    public static ResponseEntity notFound(Object data) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(data);
    }
}
