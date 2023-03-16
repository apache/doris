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

package org.apache.doris.httpv2.entity;

import org.apache.doris.httpv2.rest.RestApiStatusCode;

/**
 * The response body of restful api.
 * <p>
 * The getter setter methods of all member variables need to be retained
 * to ensure that Spring can perform json format conversion.
 *
 * @param <T> type of data
 */
public class ResponseBody<T> {
    // Used to describe the error message. If there are no errors, it displays "OK"
    private String msg;
    // The user displays an error code.
    // If there is no error, 0 is displayed.
    // If there is an error, it is usually Doris's internal error code, not the HTTP standard error code.
    // The HTTP standard error code should be reflected in the return value of the HTTP protocol.
    private int code = RestApiStatusCode.OK.code;
    // to save the response body
    private T data;
    // to save the number of records in response body.
    // currently not used and always be 0.
    private int count;

    public ResponseBody() {
    }

    public ResponseBody msg(String msg) {
        this.msg = msg;
        return this;
    }

    public ResponseBody code(RestApiStatusCode code) {
        this.code = code.code;
        return this;
    }

    public ResponseBody data(T data) {
        this.data = data;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public ResponseBody commonError(String msg) {
        this.code = RestApiStatusCode.COMMON_ERROR.code;
        this.msg = msg;
        return this;
    }
}
