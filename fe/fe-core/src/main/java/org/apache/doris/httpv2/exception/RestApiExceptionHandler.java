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

package org.apache.doris.httpv2.exception;

import org.apache.doris.common.UserException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * A handler to handle all exceptions of restful api
 */
@ControllerAdvice
public class RestApiExceptionHandler {

    private static final Logger LOG = LogManager.getLogger(RestApiExceptionHandler.class);

    @ExceptionHandler(UnauthorizedException.class)
    @ResponseBody
    public Object unauthorizedHandler(UnauthorizedException e) {
        LOG.warn("unauthorized exception", e);
        return ResponseEntityBuilder.unauthorized(e.getMessage());
    }

    @ExceptionHandler(UserException.class)
    @ResponseBody
    public Object userExceptionHandler(UserException e) {
        LOG.warn("user exception", e);
        return ResponseEntityBuilder.ok(e.getMessage());
    }

    @ExceptionHandler(BadRequestException.class)
    @ResponseBody
    public Object badRequestExceptionHandler(BadRequestException e) {
        LOG.warn("bad request exception", e);
        return ResponseEntityBuilder.badRequest(e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public Object unexpectedExceptionHandler(Exception e) {
        LOG.warn("unexpected exception", e);
        return ResponseEntityBuilder.internalError(e.getMessage());
    }
}
