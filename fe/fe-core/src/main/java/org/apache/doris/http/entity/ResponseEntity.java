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

import java.util.Map;

/**
 * http response entity
 * @author zhangjiafeng
 */
public final class ResponseEntity<T> {

    public ResponseEntity() {

    }

    public ResponseEntity(HttpStatus status) {
        this.code = status.value();
        this.msg = status.getReasonPhrase();
    }

    public ResponseEntity(HttpStatus status, T data) {
        this.code = status.value();
        this.msg = status.getReasonPhrase();
        this.data = data;
    }

    public ResponseEntity(HttpStatus status, Map<String, String> headers, T body) {
        this.code = status.value();
        this.msg = status.getReasonPhrase();
        this.data = body;
    }

    // Used to describe the error message. If there are no errors, it displays "OK"
    private String msg;
    // The user displays an error code.
    // If there is no error, 200 is displayed.
    // If there is an error, it is usually Doris's internal error code, not the HTTP standard error code.
    // The HTTP standard error code should be reflected in the return value of the HTTP protocol.
    private int code;
    // to save the response body
    private T data;
    // to save the number of records in response body.
    // currently not used and always be 0.
    private int count;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setMsgWithCode(String msg, RestApiStatusCode code) {
        this.msg = msg;
        this.code = code.code;
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

    /**
     * Create ResponseBuilder base on status
     *
     * @param status
     * @return
     */
    public static ResponseBuilder status(HttpStatus status) {
        return new ResponseBuilder(status);
    }

    /**
     * Create Http OK ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder ok() {
        return status(HttpStatus.OK);
    }

    /**
     * Create Http OK ResponseEntity
     *
     * @param data
     * @return
     */
    public static <T> ResponseEntity<T> ok(T data) {
        ResponseBuilder builder = ok();
        return builder.build(data);
    }

    /**
     * Create Http Created ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder created() {
        return status(HttpStatus.CREATED);
    }

    /**
     * Create Http Created ResponseEntity
     *
     * @param body
     * @return
     */
    public static <T> ResponseEntity<T> created(T body) {
        ResponseBuilder builder = created();
        return builder.build(body);
    }

    /**
     * Create Http No Content ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder noContent() {
        return status(HttpStatus.NO_CONTENT);
    }

    /**
     * Create Http No Content ResponseEntity
     *
     * @param body
     * @return
     */
    public static <T> ResponseEntity<T> noContent(T body) {
        ResponseBuilder builder = noContent();
        return builder.build(body);
    }

    /**
     * Create Http Not Found ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder notFound() {
        return status(HttpStatus.NOT_FOUND);
    }

    /**
     * Create Http Not Found ResponseEntity
     *
     * @param body
     * @return
     */
    public static <T> ResponseEntity<T> notFound(T body) {
        ResponseBuilder builder = notFound();
        return builder.build(body);
    }

    /**
     * Create Http Internal Server Error ResponseEntity
     *
     * @return
     */
    public static ResponseBuilder internalServerError() {
        return status(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Create Http Internal Server Error ResponseEntity
     *
     * @param data
     * @return
     */
    public static <T> ResponseEntity<T> internalServerError(T data) {
        ResponseBuilder builder = internalServerError();
        return builder.build(data);
    }

    /**
     * Set headers
     *
     * @param headers
     * @return
     */
    public ResponseEntity<T> headers(Map<String, String> headers) {
        return this;
    }

    /**
     * Http Response Builder
     *
     */
    public static class ResponseBuilder {

        private HttpStatus status;

        private Map<String, String> headers;

        public ResponseBuilder(HttpStatus status) {
            this.status = status;
        }

        public ResponseBuilder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public <T> ResponseEntity<T> build() {
            return build(null);
        }

        public <T> ResponseEntity<T> build(T data) {
            return new ResponseEntity<>(this.status, this.headers, data);
        }
    }

}
