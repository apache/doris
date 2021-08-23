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

import org.apache.doris.stack.exception.AuthorizationException;
import org.apache.doris.stack.exception.BadRequestException;
import org.apache.doris.stack.exception.HdfsUnknownHostException;
import org.apache.doris.stack.exception.HdfsUrlException;
import org.apache.doris.stack.exception.NoPermissionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @Description：Unified exception handling
 */
@ControllerAdvice
@Slf4j
public class RestApiExceptionHandler {

    @ExceptionHandler(AuthorizationException.class)
    @ResponseBody
    public Object unauthorizedHandler(AuthorizationException e) {
        log.error("authorized exception:{}", e.getMessage());
        e.printStackTrace();
        return ResponseEntityBuilder.unauthorized(e.getMessage());
    }

    @ExceptionHandler(NoPermissionException.class)
    @ResponseBody
    public Object noPermissionHandler(NoPermissionException e) {
        log.error("no permission exception:{}", e.getMessage());
        e.printStackTrace();
        return ResponseEntityBuilder.noPermission(e.getMessage());
    }

    @ExceptionHandler(BadRequestException.class)
    @ResponseBody
    public Object badRequestExceptionHandler(BadRequestException e) {
        log.error("bad request exception:{}", e.getMessage());
        e.printStackTrace();
        return ResponseEntityBuilder.badRequest(e.getMessage());
    }

    @ExceptionHandler(HdfsUnknownHostException.class)
    @ResponseBody
    public Object hdfsHostUnknownExceptionHandler(HdfsUnknownHostException e) {
        log.error("palo data import hdfs host unknown exception:{}", e.getMessage());
        e.printStackTrace();
        return ResponseEntityBuilder.okWithCommonError(e.getMessage(), RestApiStatusCode.HDFS_HOST_ERROR);
    }

    @ExceptionHandler(HdfsUrlException.class)
    @ResponseBody
    public Object hdfsUrlErrorExceptionHandler(HdfsUrlException e) {
        log.error("palo data import hdfs url error exception:{}", e.getMessage());
        e.printStackTrace();
        return ResponseEntityBuilder.okWithCommonError(e.getMessage(), RestApiStatusCode.HDFS_URL_ERROR);
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public Object unexpectedExceptionHandler(Exception e) {
        log.error("common exception:{}", e.getMessage());
        e.printStackTrace();
        return ResponseEntityBuilder.okWithCommonError(e.getMessage());
    }

    @ExceptionHandler(java.util.NoSuchElementException.class)
    @ResponseBody
    public Object noSuchElementExceptionHandler(Exception e) {
        log.error("common exception:{}", e.getMessage());
        e.printStackTrace();
        return ResponseEntityBuilder.okWithCommonError("访问数据不存在，请刷新重试或者联系技术人员.",
                RestApiStatusCode.NOT_FOUND);
    }

}
