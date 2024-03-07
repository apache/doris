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

import org.apache.doris.qe.ConnectContext;

// Used to report error happened when execute SQL of user
public class ErrorReport {

    private static String reportCommon(String pattern, ErrorCode errorCode, Object... objs) {
        String errMsg;
        if (pattern == null) {
            errMsg = errorCode.formatErrorMsg(objs);
        } else {
            errMsg = String.format(pattern, objs);
        }
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            ctx.getState().setError(errorCode, errMsg);
        }
        // TODO(zc): think about LOG to file
        return errMsg;
    }

    public static void reportAnalysisException(String pattern, Object... objs)
            throws AnalysisException {
        throw new AnalysisException(reportCommon(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs),
                ErrorCode.ERR_UNKNOWN_ERROR);
    }

    public static void reportAnalysisException(ErrorCode errorCode, Object... objs)
            throws AnalysisException {
        reportAnalysisException(null, errorCode, objs);
    }

    public static void reportAnalysisException(String pattern, ErrorCode errorCode, Object... objs)
            throws AnalysisException {
        throw new AnalysisException(reportCommon(pattern, errorCode, objs), errorCode);
    }

    public static void reportDdlException(String pattern, Object... objs)
            throws DdlException {
        reportDdlException(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs);
    }

    public static void reportDdlException(ErrorCode errorCode, Object... objs)
            throws DdlException {
        reportDdlException(null, errorCode, objs);
    }

    public static void reportDdlException(String pattern, ErrorCode errorCode, Object... objs)
            throws DdlException {
        throw new DdlException(reportCommon(pattern, errorCode, objs), errorCode);
    }

    public static void report(ErrorCode errorCode, Object... objs) {
        report(null, errorCode, objs);
    }

    public static void report(String pattern, ErrorCode errorCode, Object... objs) {
        reportCommon(pattern, errorCode, objs);
    }
}
