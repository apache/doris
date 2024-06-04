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

package org.apache.doris.alter;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;

/*
 * This exception will be thrown when the alter job(v2) being cancelled due to
 * internal error
 */
public class AlterCancelException extends DdlException {

    private static final long serialVersionUID = 7648387358354279124L;

    public AlterCancelException(String msg) {
        super(msg);
    }

    public AlterCancelException(String msg, ErrorCode mysqlErrorCode) {
        super(msg);
        setMysqlErrorCode(mysqlErrorCode);
    }
}
