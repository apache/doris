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

package org.apache.doris.http.exception;

import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.ResponseEntityBuilder;

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
        LOG.debug("unauthorized exception", e);
        return ResponseEntityBuilder.unauthorized(e.getMessage());
    }

    @ExceptionHandler(DdlException.class)
    @ResponseBody
    public Object ddlExceptionHandler(DdlException e) {
        LOG.debug("ddl exception", e);
        return ResponseEntityBuilder.ok(e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public Object unexpectedExceptionHandler(Exception e) {
        LOG.debug("unexpected exception", e);
        return ResponseEntityBuilder.internalError(e.getMessage());
    }
}
