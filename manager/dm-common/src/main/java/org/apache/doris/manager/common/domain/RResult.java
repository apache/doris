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

package org.apache.doris.manager.common.domain;

import java.util.HashMap;
import java.util.Objects;

public class RResult extends HashMap<String, Object> {
    public static final String CODE_TAG = "code";
    public static final String MSG_TAG = "msg";
    public static final String DATA_TAG = "data";
    private static final long serialVersionUID = 1L;
    private static final int CODE_SUCCESS = 0;
    private static final int CODE_FAILED = 500;

    public RResult() {
    }

    public RResult(int code, String msg) {
        super.put(CODE_TAG, code);
        super.put(MSG_TAG, msg);
    }

    public RResult(int code, String msg, Object data) {
        super.put(CODE_TAG, code);
        super.put(MSG_TAG, msg);
        if (!Objects.isNull(data)) {
            super.put(DATA_TAG, data);
        }
    }

    public static RResult success() {
        return RResult.success("success");
    }

    public static RResult success(Object data) {
        return RResult.success("success", data);
    }

    public static RResult success(String msg) {
        return RResult.success(msg, null);
    }

    public static RResult success(String msg, Object data) {
        return new RResult(CODE_SUCCESS, msg, data);
    }

    public static RResult error() {
        return RResult.error("fail");
    }

    public static RResult error(String msg) {
        return RResult.error(msg, null);
    }

    public static RResult error(String msg, Object data) {
        return new RResult(CODE_FAILED, msg, data);
    }

    public static RResult error(int code, String msg) {
        return new RResult(code, msg, null);
    }

    public Object getData() {
        return this.get("data");
    }

    public int getCode() {
        return (int) this.get("code");
    }

    public boolean isSuccess() {
        return getCode() == CODE_SUCCESS;
    }
}
