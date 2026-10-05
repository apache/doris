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

package org.apache.doris.cdcclient.model.rest;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RestResponse<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final int SUCCESS = 0;
    public static final int FAIL = 1;

    private static final String DEFAULT_SUCCESS_MESSAGE = "Success";
    private static final String DEFAULT_SYSTEM_ERROR_MESSAGE = "Internal Error";

    private String msg;
    private int code;
    private T data;

    public static <T> RestResponse<T> success(T data) {
        RestResponse<T> response = new RestResponse<>();
        response.setCode(SUCCESS);
        response.setMsg(DEFAULT_SUCCESS_MESSAGE);
        response.setData(data);
        return response;
    }

    public static <T> RestResponse<T> internalError(T data) {
        RestResponse<T> response = new RestResponse<>();
        response.setCode(FAIL);
        response.setMsg(DEFAULT_SYSTEM_ERROR_MESSAGE);
        response.setData(data);
        return response;
    }
}
