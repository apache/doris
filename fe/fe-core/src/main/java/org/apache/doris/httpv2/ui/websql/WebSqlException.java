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

package org.apache.doris.httpv2.ui.websql;

public class WebSqlException extends RuntimeException {
    private final WebSqlError error;
    private final Object details;

    public WebSqlException(WebSqlError error) {
        this(error, null, null);
    }

    public WebSqlException(WebSqlError error, Throwable cause) {
        this(error, null, cause);
    }

    public WebSqlException(WebSqlError error, Object details, Throwable cause) {
        super(error.getMessage(), cause);
        this.error = error;
        this.details = details;
    }

    public WebSqlError getError() {
        return error;
    }

    public Object getDetails() {
        return details;
    }
}
