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

package org.apache.doris.nereids.hint;

import java.util.Objects;

/**
 * select hint.
 * e.g. set_var(query_timeout='1800', exec_mem_limit='2147483648')
 */
public class Hint {
    // e.g. set_var
    private String hintName;

    private HintStatus status;

    private String errorMessage = "";

    /**
     * hint status which need to show in explain when it is not used or have syntax error
     */
    public enum HintStatus {
        UNUSED,
        SYNTAX_ERROR,
        SUCCESS
    }

    public Hint(String hintName) {
        this.hintName = Objects.requireNonNull(hintName, "hintName can not be null");
        this.status = HintStatus.UNUSED;
    }

    public void setHintName(String hintName) {
        this.hintName = hintName;
    }

    public void setStatus(HintStatus status) {
        this.status = status;
    }

    public HintStatus getStatus() {
        return status;
    }

    public boolean isSuccess() {
        return getStatus() == HintStatus.SUCCESS;
    }

    public boolean isSyntaxError() {
        return getStatus() == HintStatus.SYNTAX_ERROR;
    }

    public boolean isUnUsed() {
        return getStatus() == HintStatus.UNUSED;
    }

    public String getHintName() {
        return hintName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getExplainString() {
        StringBuilder out = new StringBuilder();
        out.append("\nHint:\n");
        return out.toString();
    }
}
