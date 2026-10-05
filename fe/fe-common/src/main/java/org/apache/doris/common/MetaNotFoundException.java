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

package org.apache.doris.common;

/**
 * Exception for meta info is null, like db table partition tablet replica job
 */
public class MetaNotFoundException extends UserException {
    public MetaNotFoundException(String msg) {
        super(InternalErrorCode.META_NOT_FOUND_ERR, msg);
    }

    public MetaNotFoundException(String msg, ErrorCode mysqlErrorCode) {
        super(InternalErrorCode.META_NOT_FOUND_ERR, msg);
        setMysqlErrorCode(mysqlErrorCode);
    }

    public MetaNotFoundException(InternalErrorCode errcode, String msg) {
        super(errcode, msg);
    }

    public MetaNotFoundException(Throwable e) {
        super(e);
    }

    public MetaNotFoundException(String msg, Throwable e) {
        super(msg, e);
    }
}
