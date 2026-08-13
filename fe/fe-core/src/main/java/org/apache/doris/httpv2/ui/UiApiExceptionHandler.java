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

package org.apache.doris.httpv2.ui;

import org.apache.doris.httpv2.ui.websql.WebSqlError;
import org.apache.doris.httpv2.ui.websql.WebSqlException;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(basePackages = "org.apache.doris.httpv2.ui")
public class UiApiExceptionHandler {
    private static final Logger LOG = LogManager.getLogger(UiApiExceptionHandler.class);

    @ExceptionHandler(UiApiException.class)
    public ResponseEntity<ApiError> handleUiApiException(UiApiException exception) {
        ApiError error = new ApiError(exception.getCode(), exception.getMessage(), null);
        return ResponseEntity.status(exception.getStatus()).body(error);
    }

    @ExceptionHandler(WebSqlException.class)
    public ResponseEntity<ApiError> handleWebSqlException(WebSqlException exception) {
        WebSqlError error = exception.getError();
        ApiError response = new ApiError(error.getCode(), error.getMessage(), exception.getDetails());
        return ResponseEntity.status(error.getStatus()).body(response);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class})
    public ResponseEntity<ApiError> handleInvalidRequest(Exception exception) {
        ApiError response = new ApiError(
                "UI_INVALID_REQUEST", "The request body or parameters are invalid.", null);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpectedException(Exception exception) {
        LOG.warn("Unexpected UI API exception", exception);
        ApiError error = new ApiError("UI_INTERNAL_ERROR", "An internal error occurred.", null);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class ApiError {
        private final String code;
        private final String message;
        private final Object details;

        ApiError(String code, String message, Object details) {
            this.code = code;
            this.message = message;
            this.details = details;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        public Object getDetails() {
            return details;
        }
    }
}
