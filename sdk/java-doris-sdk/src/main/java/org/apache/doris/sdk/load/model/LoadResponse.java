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

package org.apache.doris.sdk.load.model;

/**
 * Result of a stream load operation.
 */
public class LoadResponse {

    public enum Status { SUCCESS, FAILURE }

    private final Status status;
    private final RespContent respContent;
    private final String errorMessage;

    private LoadResponse(Status status, RespContent respContent, String errorMessage) {
        this.status = status;
        this.respContent = respContent;
        this.errorMessage = errorMessage;
    }

    public static LoadResponse success(RespContent resp) {
        return new LoadResponse(Status.SUCCESS, resp, null);
    }

    public static LoadResponse failure(RespContent resp, String errorMessage) {
        return new LoadResponse(Status.FAILURE, resp, errorMessage);
    }

    public Status getStatus() { return status; }
    public RespContent getRespContent() { return respContent; }
    public String getErrorMessage() { return errorMessage; }

    @Override
    public String toString() {
        return "LoadResponse{status=" + status
                + (errorMessage != null ? ", error='" + errorMessage + "'" : "")
                + (respContent != null ? ", resp=" + respContent : "")
                + "}";
    }
}
