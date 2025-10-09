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

package org.apache.doris.nereids.exceptions;

import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.Optional;

/** Nereids's AnalysisException. */
public class AnalysisException extends RuntimeException {
    private final ErrorCode errorCode;
    private final String message;
    private final Optional<Integer> line;
    private final Optional<Integer> startPosition;
    private final Optional<LogicalPlan> plan;

    /** Constructor of AnalysisException. */
    public AnalysisException(ErrorCode errorCode, String message, Throwable cause, Optional<Integer> line,
            Optional<Integer> startPosition, Optional<LogicalPlan> plan) {
        super(message, cause);
        this.errorCode = errorCode;
        this.message = message;
        this.line = line;
        this.startPosition = startPosition;
        this.plan = plan;
    }

    /** Constructor of AnalysisException. */
    public AnalysisException(ErrorCode errorCode, String message, Optional<Integer> line,
            Optional<Integer> startPosition, Optional<LogicalPlan> plan) {
        super(message);
        this.errorCode = errorCode;
        this.message = message;
        this.line = line;
        this.startPosition = startPosition;
        this.plan = plan;
    }

    public AnalysisException(ErrorCode errorCode, String message, Throwable cause) {
        this(errorCode, message, cause, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public AnalysisException(ErrorCode errorCode, String message) {
        this(errorCode, message, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public AnalysisException(String message, Throwable cause) {
        this(ErrorCode.NONE, message, cause);
    }

    public AnalysisException(String message) {
        this(ErrorCode.NONE, message);
    }

    @Override
    public String getMessage() {
        String planAnnotation = plan.map(p -> ";\n" + p.treeString()).orElse("");
        return getSimpleMessage() + planAnnotation;
    }

    private String getSimpleMessage() {
        if (line.isPresent() || startPosition.isPresent()) {
            String lineAnnotation = line.map(l -> "line " + l).orElse("");
            String positionAnnotation = startPosition.map(s -> " pos " + s).orElse("");
            return message + ";" + lineAnnotation + positionAnnotation;
        } else {
            return message;
        }
    }

    /** get error code.
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /** error code enum.
     */
    public enum ErrorCode {
        NONE,
        EXPRESSION_EXCEEDS_LIMIT,
    }
}
